package fort

import (
	"sync"

	"k8s.io/client-go/tools/cache"
)

// A shared informer defined by query that is also cloneable.
type CloneableSharedInformerQuery interface {
	cache.SharedInformer
	// Create a clone of the InformerQuery. Use the given sources for
	// the new query. When cloning we will likely be replacing
	// "live" informers with ManualInformers and "Query" informers
	// with new cloned version of the informers.
	Clone(newSources []cache.SharedInformer) CloneableSharedInformerQuery
}

// Generates a new SharedInformer by running the given query over
// the given set of source informers. This is a SQL query
// encoded in gocode, and so follows that standard SQL pattern.
//
// The query logically constructs an array with each tuple of
// objects from the source informers and passes this to the
// selector function.
//
// The selector function returns an object that is emitted
// by the informer. The selector is typed to make typing
// clearer in the code.
//
// If defined, the where function will be called on the tuple first,
// if it returns false the tuple will be dropped, otherwise it will
// be passed on to the GroupBy or Selector.
//
// If the groupBy function is defined, the query will first run the tuples
// from the sources through the function and then pass the aggregated
// results to the select function. See the comment on GroupByFunc
// for more details.
func QueryInformer(query QuerySpec) CloneableSharedInformerQuery {
	return query.Build()
}

type QuerySpec interface {
	Build() CloneableSharedInformerQuery
}

type Select[Out, In any] struct {
	Select SingleSelectFunc[Out, In]
	From   cache.SharedInformer
	Where  SingleFilterFunc[In]
}

type SingleSelectFunc[Out, Left any] func(value Left) (Out, error)
type SingleFilterFunc[In any] func(in In) bool

type Join[Out, Left, Right any] struct {
	Select JoinSelectFunc[Out, Left, Right]
	From   cache.SharedInformer
	Join   cache.SharedInformer
	On     JoinOnFunc[Left, Right]
	Where  JoinFilterFunc[Left, Right]
}

type JoinSelectFunc[Out, Left, Right any] func(left Left, right Right) (Out, error)
type JoinFilterFunc[Left, Right any] func(left Left, right Right) bool

// If a join is too expensive to do as a full join, the caller
// can define a JoinOnFunc. Only one of the left and right arguments
// will be non-nil, the function returns the key for that element.
type JoinOnFunc[Left, Right any] func(left Left, right Right) any

type GroupBy[Out, In any] struct {
	Select  GroupSelectFunc[Out]
	From    cache.SharedInformer
	Where   SingleFilterFunc[In]
	GroupBy SingleGroupByFunc[In]
}

type GroupSelectFunc[Out any] func(fields []GroupField) (Out, error)
type SingleGroupByFunc[In any] func(in In) (any, []GroupField)

type GroupByJoin[Out, Left, Right any] struct {
	Select  GroupSelectFunc[Out]
	From    cache.SharedInformer
	Join    cache.SharedInformer
	On      JoinOnFunc[Left, Right]
	Where   JoinFilterFunc[Left, Right]
	GroupBy JoinGroupByFunc[Left, Right]
}

type JoinGroupByFunc[Left, Right any] func(left Left, right Right) (any, []GroupField)

type GroupField interface{}

type groupField struct {
	key      any
	count    bool
	sum      *int64
	distinct any
	anyValue any
}

// A group key defined by the given value.
func GroupKey(key any) GroupField {
	return &groupField{key: key}
}

// Converts to the count of the number of tuples in this group.
func Count() GroupField {
	return &groupField{count: true}
}

// Converts to the sum of the values for all the tuples in this group.
func Sum(val int64) GroupField {
	return &groupField{sum: &val}
}

// Converts to a list of distinct values for all the tuples in this group.
func Distinct(val any) GroupField {
	return &groupField{distinct: val}
}

// Returns one of the values passed in for all the tuples in this group.
func AnyValue(val any) GroupField {
	return &groupField{anyValue: val}
}

type FlatMap[Out, In any] struct {
	Map  FlatMapFunc[Out, In]
	Over cache.SharedInformer
}

type FlatMapFunc[Out, In any] func(obj In) ([]Out, error)

// After cloning we would like to replace the live informers (looking at the real system state)
// with informers we can update by hand as we try to do simulations. To do so we can use
// ManualSharedInformers. They implement the ResourcEventHandler API and so can be updated
// using OnAdd, OnUpdate and OnDelete.
type ManualSharedInformer interface {
	CloneableSharedInformerQuery
	cache.ResourceEventHandler

	SetIsStopped()
	SetHasSynced()
	GetKeyFunc() cache.KeyFunc
}

func NewManualSharedInformer() ManualSharedInformer {
	return NewManualSharedInformerWithKeyFunc(cache.MetaNamespaceKeyFunc)
}

func NewManualSharedInformerWithKeyFunc(keyFunc cache.KeyFunc) ManualSharedInformer {
	return &manualInformer{
		handlers: map[int]cache.ResourceEventHandler{},
		keyFunc:  keyFunc,
	}
}

// Lock multiple informers together to ensure we can snapshot them
// consistently.
func LockInformerSet(informers []CloneableSharedInformerQuery) InformerLockSet {
	ls := &informerLockSet{}
	for _, inf := range informers {
		if m, ok := inf.(interface{ Lock() *sync.Mutex }); ok {
			lock := m.Lock()
			lock.Lock()
			ls.locks = append(ls.locks, lock)
		}
	}
	return ls
}

type InformerLockSet interface {
	Unlock()
}

type informerLockSet struct {
	locks []*sync.Mutex
}

func (ls *informerLockSet) Unlock() {
	for i := len(ls.locks) - 1; i >= 0; i-- {
		ls.locks[i].Unlock()
	}
}
