package fort

import (
	"sync"

	"k8s.io/client-go/tools/cache"
)

// CloneableSharedInformerQuery is a shared informer defined by a query that can be cloned.
// Cloning is essential for simulations where live informers are replaced with manual ones.
type CloneableSharedInformerQuery interface {
	cache.SharedInformer
	// Clone creates a new instance of the query using the provided new sources.
	Clone(newSources []cache.SharedInformer) CloneableSharedInformerQuery
}

// QueryInformer generates a new SharedInformer by running the given query spec.
// It follows a declarative pattern similar to SQL but implemented as Go code.
func QueryInformer(query QuerySpec) CloneableSharedInformerQuery {
	return query.Build()
}

// QuerySpec defines the interface for building a query informer.
type QuerySpec interface {
	Build() CloneableSharedInformerQuery
}

// Select defines a simple transformation and filtering query.
type Select[Out, In any] struct {
	Select SingleSelectFunc[Out, In]
	From   cache.SharedInformer
	Where  SingleFilterFunc[In]
}

type SingleSelectFunc[Out, Left any] func(value Left) (Out, error)
type SingleFilterFunc[In any] func(in In) bool

// Join defines a many-to-many join between two informers.
type Join[Out, Left, Right any] struct {
	Select JoinSelectFunc[Out, Left, Right]
	From   cache.SharedInformer
	Join   cache.SharedInformer
	On     JoinOnFunc[Left, Right]
	Where  JoinFilterFunc[Left, Right]
}

type JoinSelectFunc[Out, Left, Right any] func(left Left, right Right) (Out, error)
type JoinFilterFunc[Left, Right any] func(left Left, right Right) bool

// JoinOnFunc defines the key used for joining two objects.
// To ensure map compatibility, it should return a comparable type (e.g., [N]string).
type JoinOnFunc[Left, Right any] func(left Left, right Right) any

// GroupBy defines an aggregation query over a single informer.
type GroupBy[Out, In any] struct {
	Select  GroupSelectFunc[Out]
	From    cache.SharedInformer
	Where   SingleFilterFunc[In]
	GroupBy SingleGroupByFunc[In]
}

type GroupSelectFunc[Out any] func(fields []GroupField) (Out, error)

// SingleGroupByFunc returns a comparable key for the group and a slice of aggregate fields.
type SingleGroupByFunc[In any] func(in In) (any, []GroupField)

// GroupByJoin defines an aggregation query over the results of a join.
type GroupByJoin[Out, Left, Right any] struct {
	Select  GroupSelectFunc[Out]
	From    cache.SharedInformer
	Join    cache.SharedInformer
	On      JoinOnFunc[Left, Right]
	Where   JoinFilterFunc[Left, Right]
	GroupBy JoinGroupByFunc[Left, Right]
}

type JoinGroupByFunc[Left, Right any] func(left Left, right Right) (any, []GroupField)

// GroupField represents an individual aggregate field in a GroupBy query.
type GroupField interface{}

type groupField struct {
	key      any
	count    bool
	sum      *int64
	distinct any
	anyValue any
}

// GroupKey wraps a value to be used as a grouping key field.
func GroupKey(key any) GroupField {
	return &groupField{key: key}
}

// Count returns an aggregate field representing the number of items in the group.
func Count() GroupField {
	return &groupField{count: true}
}

// Sum returns an aggregate field representing the sum of a numeric value across the group.
func Sum(val int64) GroupField {
	return &groupField{sum: &val}
}

// Distinct returns an aggregate field representing the set of unique values across the group.
func Distinct(val any) GroupField {
	return &groupField{distinct: val}
}

// AnyValue returns an aggregate field representing an arbitrary value from the group.
func AnyValue(val any) GroupField {
	return &groupField{anyValue: val}
}

// FlatMap defines a one-to-many transformation query.
type FlatMap[Out, In any] struct {
	Map  FlatMapFunc[Out, In]
	Over cache.SharedInformer
}

type FlatMapFunc[Out, In any] func(obj In) ([]Out, error)

// ManualSharedInformer allows manual triggering of events, useful for testing and simulations.
type ManualSharedInformer interface {
	CloneableSharedInformerQuery
	cache.ResourceEventHandler

	SetIsStopped()
	SetHasSynced()
	GetKeyFunc() cache.KeyFunc
}

// NewManualSharedInformer creates a ManualSharedInformer using the default MetaNamespaceKeyFunc.
func NewManualSharedInformer() ManualSharedInformer {
	return NewManualSharedInformerWithKeyFunc(cache.MetaNamespaceKeyFunc)
}

// NewManualSharedInformerWithKeyFunc creates a ManualSharedInformer with a custom KeyFunc.
func NewManualSharedInformerWithKeyFunc(keyFunc cache.KeyFunc) ManualSharedInformer {
	return &manualInformer{
		handlers: map[int]cache.ResourceEventHandler{},
		keyFunc:  keyFunc,
	}
}

// LockInformerSet locks multiple informers together in a deterministic order.
// Use this to ensure consistent snapshots across related informers.
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

// InformerLockSet provides a single Unlock method to release all acquired locks.
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
