package fort

import (
	"fmt"
	"sync"

	"k8s.io/client-go/tools/cache"
)

// DefaultKeyFunc is a robust key function that handles both K8s objects and primitive types.
// It ensures that even non-meta objects can be indexed without returning errors.
func DefaultKeyFunc(obj any) (string, error) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err == nil && key != "" {
		return key, nil
	}
	// Fallback for non-K8s objects. Never return error to ensure indexer.Add works.
	return fmt.Sprintf("%v", obj), nil
}

// UnwrapDeleted returns the underlying object if it's wrapped in a DeletedFinalStateUnknown container.
func UnwrapDeleted(obj any) any {
	if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		return d.Obj
	}
	return obj
}

// LockGroup manages a shared RWMutex for a connected set of query informers (a Domain).
// In Fort, an entire query DAG (from sources to final aggregates) should share a single
// LockGroup to ensure transactional consistency across the domain.
type LockGroup interface {
	RLock()
	RUnlock()
	Lock()
	Unlock()
}

type lockGroup struct {
	sync.RWMutex
}

// NewLockGroup creates a new shared mutex domain.
func NewLockGroup() LockGroup {
	return &lockGroup{}
}

// CloneableSharedInformerQuery is a shared informer defined by a query that can be cloned.
// Clones are "born hydrated" via Copy-on-Write (COW) data structures, enabling O(1) snapshots.
type CloneableSharedInformerQuery interface {
	cache.SharedInformer
	// Clone creates a new instance of the query using the provided new sources.
	// REQUIRES: Caller must hold the shared LockGroup (RLock or Lock) of the parent.
	Clone(newSources []cache.SharedInformer) CloneableSharedInformerQuery
	// GetLockGroup returns the shared lock used by this informer domain.
	GetLockGroup() LockGroup
	// SetName sets the name for debug logging.
	SetName(name string)
	// GetSources returns the upstream informers providing data to this query.
	GetSources() []cache.SharedInformer
	// IsStoppedChan returns a channel that is closed when the informer is stopped.
	IsStoppedChan() <-chan struct{}
	// GetKeyFunc returns the key function used by this informer.
	GetKeyFunc() cache.KeyFunc
}

// QueryInformer generates a new SharedInformer by running the given query spec.
func QueryInformer(query QuerySpec) CloneableSharedInformerQuery {
	return query.Build()
}

// QuerySpec defines the interface for building a query informer from a declarative specification.
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
	// On defines the join key. If nil, a full Cartesian join is performed.
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

// Aggregate builders.

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
// Internal query stages use this to propagate events across the shared-lock domain.
type LockedResourceEventHandler interface {
	OnAddLocked(obj any, isInInitialList bool)
	OnUpdateLocked(oldObj, newObj any)
	OnDeleteLocked(oldObj any)
}

// ManualSharedInformer allows manual triggering of events.
// It is primarily used for testing and for providing hydrated snapshot sources.
type ManualSharedInformer interface {
	CloneableSharedInformerQuery
	cache.ResourceEventHandler
	LockedResourceEventHandler

	SetIsStopped()
	SetHasSynced()
	GetKeyFunc() cache.KeyFunc
	TriggerWatchError(err error)

	// AddEventHandlerNoReplay registers a handler without replaying current state.
	// Used during pipeline cloning to prevent redundant O(N) hydration.
	AddEventHandlerNoReplay(h cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error)
}

// NewManualSharedInformer creates a ManualSharedInformer with a default lock and keyfunc.
func NewManualSharedInformer() ManualSharedInformer {
	return NewManualSharedInformerWithOptions(NewLockGroup(), DefaultKeyFunc)
}

// NewManualSharedInformerWithOptions creates a ManualSharedInformer with specific options.
func NewManualSharedInformerWithOptions(lock LockGroup, keyFunc cache.KeyFunc) ManualSharedInformer {
	return &manualInformer{
		handlers:      map[int]cache.ResourceEventHandler{},
		keyFunc:       keyFunc,
		indexer:       NewBTreeIndexer(keyFunc),
		lock:          lock,
		synced:        make(chan struct{}),
		isStoppedChan: make(chan struct{}),
	}
}

// LockDomain acquires a Read Lock on the domain (LockGroup), enabling a consistent snapshot.
func LockDomain(informers ...CloneableSharedInformerQuery) DomainLock {
	if len(informers) == 0 {
		return &domainLock{}
	}
	lock := informers[0].GetLockGroup()
	lock.RLock()
	return &domainLock{lock: lock}
}

// DomainLock provides a handle to release a domain-level read lock.
type DomainLock interface {
	Unlock()
}

type domainLock struct {
	lock LockGroup
}

func (ls *domainLock) Unlock() {
	if ls.lock != nil {
		ls.lock.RUnlock()
	}
}

// ClonePipeline recursively clones a query DAG starting from root, replacing leaf sources.
// The memo map should initially contain leaf replacements (Source -> ClonedSource)
// and will be populated with cloned intermediate stages to handle shared query branches (Diamond DAGs).
func ClonePipeline(root cache.SharedInformer, memo map[cache.SharedInformer]cache.SharedInformer) cache.SharedInformer {
	return clonePipelineRecursive(root, memo, 0)
}

const maxCloneDepth = 100

func clonePipelineRecursive(root cache.SharedInformer, memo map[cache.SharedInformer]cache.SharedInformer, depth int) cache.SharedInformer {
	if depth > maxCloneDepth {
		panic(fmt.Sprintf("Recursive clone depth exceeded %d (possible cycle in query DAG)", maxCloneDepth))
	}

	if repl, ok := memo[root]; ok {
		return repl
	}

	q, ok := root.(CloneableSharedInformerQuery)
	if !ok {
		return root
	}

	sources := q.GetSources()
	if len(sources) == 0 {
		// This is a leaf that was not in the initial memo map.
		return root
	}

	newSources := make([]cache.SharedInformer, len(sources))
	for i, s := range sources {
		newSources[i] = clonePipelineRecursive(s, memo, depth+1)
	}

	res := q.Clone(newSources)
	memo[root] = res
	return res
}
