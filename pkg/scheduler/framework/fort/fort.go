/*
Fort is a database-lite (a datafort). It is an in-memory engine for mapreduce and join operations.
All of the maps are fast cloneable so the entire "db" can be cloned quickly.
*/
package fort

// A state spec defines a set of sources and derived maps.
type StateSpec interface{}

// Create a new empty spec
func NewSpec() StateSpec {
	return newSpec()
}

// Add a source to the spec. A source is a map
// that is updated by external logic. Everything in fort
// is derived from a source (or a map derived from a source, etc)
func NewSource[K comparable](s StateSpec, name string) {
	s.(*stateSpec).Register(
		func(s State, isClone bool) {
			makeOrCloneMap[K](s.(*state), name, isClone)
		},
	)
}

// A key value source can be updated using the given methods.
// All derived maps are updated when these operators are called.
// A source is just a map from key to value.
type KeyValueSource[K comparable] interface {
	Update(key K, value any)
	Delete(key K)
}

// Define a new map created by joining two source maps on the given spec.
// This will create a new map with one entry for each pair of entries in the source maps.
// The keys are JoinKey objects and the values are JoinValue objects.
func FullJoin[LK, RK comparable](s StateSpec, name, left, right string) {
	Join(s, name, left, right, getAllItems[LK, RK], getAllItems[RK, LK])
}

// Define a new map created by doing a "key join" between two maps on the given spec.
// This creates a single entry for the objects with the same key in
// the two source maps. If both maps do not have a key, then no
// value is generated for that key. The key is the same as the input
// key, and the value is a JoinValue.
func KeyJoin[K comparable](s StateSpec, name, left, right string) {
	Join(s, name, left, right, lookupByKey[K], lookupByKey[K])
}

// Internal join operator used to implement FullJoin and KeyJoin.
func Join[LK, RK comparable](s StateSpec, name, left, right string, leftLookup LookupFunc[RK, LK], rightLookup LookupFunc[LK, RK]) {
	s.(*stateSpec).Register(newJoinFactory(name, left, right, leftLookup, rightLookup))
}

// Run map reduce on a given input map and generate a new map based on the operation.
func MapReduce[I, O comparable](s StateSpec, name string, mapper Mapper[I, O], reducer Reducer, source string) {
	s.(*stateSpec).Register(newMapReduceFactory(name, mapper, reducer, source))
}

// State object. This is a set of named maps and their operators.
// It can be fast cloned using the operation here.
// You can get sources and generated maps
type State interface {
	Clone() State
	Print()
}

// Create a new state object from a spec.
func New(spec StateSpec) State {
	return newState(spec)
}

// Get a reference to a source object from the state given its name.
func Source[K comparable](s State, name string) KeyValueSource[K] {
	v, _ := s.(*state).root.Get(name)
	return v.(KeyValueSource[K])
}

// Get a reference to a derived map object given its name.
// Note that derived maps are read only. They are updated
// by the internal operators when source maps are updated.
func GetMap[K comparable](s State, name string) KeyValueMap[K] {
	v, _ := s.(*state).root.Get(name)
	return v.(KeyValueMap[K])
}

// A KeyValueMap is a simple read-only map interface.
type KeyValueMap[K comparable] interface {
	Get(key K) (any, bool)
	Has(key K) bool
	All() KeyValueIterator[K]

	Print()

	keyValueSource
}

// Join structures

type LookupFunc[S, T comparable] func(sourceItem *KeyValue[S], targetItems *CloneMap[T]) KeyValueIterator[T]

// This is the value returned by a join operation.
type JoinValue[LK, RK comparable] struct {
	Left  *KeyValue[LK]
	Right *KeyValue[RK]
}

type JoinKey [2]any

// Common reducers

var (
	Count     = CountReducer
	Identical = IdenticalReducer
	SumList   = SumListReducer
)

// MapReduce structures

type Mapper[I, O comparable] func(kv *KeyValue[I]) KeyValueSet[O]
type Reducer func(owner any) ReducerEntry
type KeyFunc[I, O comparable] func(key I, value any) O

type Cloneable interface {
	Clone(root any) Cloneable
}

type ReducerEntry interface {
	Add(value any)
	Remove(value any) bool
	Value() any

	Cloneable
}

type StrTuple [2]string

// Key value sets

type KeyValue[K comparable] struct {
	Key   K
	Value any
}

type KeyValueSet[K comparable] []KeyValue[K]

type KeyValueIterator[K comparable] func(yield func(key K, value any) bool)

func (s KeyValueSet[K]) All() KeyValueIterator[K] {
	return func(yield func(key K, value any) bool) {
		for _, kv := range s {
			if !yield(kv.Key, kv.Value) {
				return
			}
		}
	}
}
