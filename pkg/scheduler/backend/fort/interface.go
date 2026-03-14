package fort

import (
	"fmt"
	"sync"

	"k8s.io/client-go/tools/cache"
)

// DefaultKeyFunc is a robust key function that handles both K8s objects and primitive types.
func DefaultKeyFunc(obj any) (string, error) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err == nil && key != "" {
		return key, nil
	}
	// Fallback for non-K8s objects. Never return error to ensure indexer.Add works.
	return fmt.Sprintf("%v", obj), nil
}

// LockGroup manages a shared RWMutex for a connected set of query informers.
type LockGroup interface {
	RLock()
	RUnlock()
	Lock()
	Unlock()
}

type lockGroup struct {
	sync.RWMutex
}

func NewLockGroup() LockGroup {
	return &lockGroup{}
}

// CloneableSharedInformerQuery is a shared informer defined by a query that can be cloned.
type CloneableSharedInformerQuery interface {
	cache.SharedInformer
	// Clone creates a new instance of the query using the provided new sources.
	Clone(newSources []cache.SharedInformer) CloneableSharedInformerQuery
	// GetLockGroup returns the shared lock used by this informer.
	GetLockGroup() LockGroup
	// SetName sets the name for debug logging.
	SetName(name string)
}

// QueryInformer generates a new SharedInformer by running the given query spec.
func QueryInformer(query QuerySpec) CloneableSharedInformerQuery {
	return query.Build()
}

// QuerySpec defines the interface for building a query informer.
type QuerySpec interface {
	Build() CloneableSharedInformerQuery
}

// Select defines a simple transformation and filtering query.
type Select[Out, In any] struct {
	Lock   LockGroup
	Select SingleSelectFunc[Out, In]
	From   cache.SharedInformer
	Where  SingleFilterFunc[In]
}

type SingleSelectFunc[Out, Left any] func(value Left) (Out, error)
type SingleFilterFunc[In any] func(in In) bool

// JoinValue represents a pair of joined objects.
type JoinValue[Left, Right any] struct {
	Left  Left
	Right Right
}

// Join defines a many-to-many join between two informers.
type Join[Out, Left, Right any] struct {
	Lock   LockGroup
	Select JoinSelectFunc[Out, Left, Right]
	From   cache.SharedInformer
	Join   cache.SharedInformer
	On     JoinOnFunc[Left, Right]
	Where  JoinFilterFunc[Left, Right]
}

type JoinSelectFunc[Out, Left, Right any] func(left Left, right Right) (Out, error)
type JoinFilterFunc[Left, Right any] func(left Left, right Right) bool

// JoinOnFunc defines the key used for joining two objects.
type JoinOnFunc[Left, Right any] func(left Left, right Right) any

// GroupBy defines an aggregation query over a single informer.
type GroupBy[Out, In any] struct {
	Lock    LockGroup
	Select  GroupSelectFunc[Out]
	From    cache.SharedInformer
	Where   SingleFilterFunc[In]
	GroupBy SingleGroupByFunc[In]
}

type GroupSelectFunc[Out any] func(fields []GroupField) (Out, error)
type SingleGroupByFunc[In any] func(in In) (any, []GroupField)

// GroupByJoin defines an aggregation query over the results of a join.
type GroupByJoin[Out, Left, Right any] struct {
	Lock    LockGroup
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

func GroupKey(key any) GroupField {
	return &groupField{key: key}
}

func Count() GroupField {
	return &groupField{count: true}
}

func Sum(val int64) GroupField {
	return &groupField{sum: &val}
}

func Distinct(val any) GroupField {
	return &groupField{distinct: val}
}

func AnyValue(val any) GroupField {
	return &groupField{anyValue: val}
}

// FlatMap defines a one-to-many transformation query.
type FlatMap[Out, In any] struct {
	Lock LockGroup
	Map  FlatMapFunc[Out, In]
	Over cache.SharedInformer
}

type FlatMapFunc[Out, In any] func(obj In) ([]Out, error)

// LockedResourceEventHandler allows processing events without re-acquiring the LockGroup.
type LockedResourceEventHandler interface {
	OnAddLocked(obj any, isInInitialList bool)
	OnUpdateLocked(oldObj, newObj any)
	OnDeleteLocked(oldObj any)
}

// ManualSharedInformer allows manual triggering of events.
type ManualSharedInformer interface {
	CloneableSharedInformerQuery
	cache.ResourceEventHandler
	LockedResourceEventHandler

	SetIsStopped()
	SetHasSynced()
	GetKeyFunc() cache.KeyFunc
	TriggerWatchError(err error)
}

// NewManualSharedInformer creates a ManualSharedInformer with a default lock and keyfunc.
func NewManualSharedInformer() ManualSharedInformer {
	return NewManualSharedInformerWithOptions(NewLockGroup(), DefaultKeyFunc)
}

// NewManualSharedInformerWithOptions creates a ManualSharedInformer with specific options.
func NewManualSharedInformerWithOptions(lock LockGroup, keyFunc cache.KeyFunc) ManualSharedInformer {
	return &manualInformer{
		handlers: map[int]cache.ResourceEventHandler{},
		keyFunc:  keyFunc,
		indexer:  cache.NewIndexer(keyFunc, cache.Indexers{}),
		lock:     lock,
	}
}

// LockInformerSet acquires a Read Lock on the domain, enabling a consistent snapshot.
func LockInformerSet(informers []CloneableSharedInformerQuery) InformerLockSet {
	if len(informers) == 0 {
		return &informerLockSet{}
	}
	lock := informers[0].GetLockGroup()
	lock.RLock()
	return &informerLockSet{lock: lock}
}

type InformerLockSet interface {
	Unlock()
}

type informerLockSet struct {
	lock LockGroup
}

func (ls *informerLockSet) Unlock() {
	if ls.lock != nil {
		ls.lock.RUnlock()
	}
}
