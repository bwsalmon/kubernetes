/*
Fort is almost a database (its a datafort!). It is an in-memory engine for mapreduce and
join operations. All data in fort is represented as memory key-value maps.
All the results of mapreduces and joins are automatically updated as source data is updated.
All of the maps are fast cloneable so the entire "db" can be cloned quickly.
This makes it simple to generate complex data structures that are automatically
updated as data changes, and are easily fast cloneable.
*/
package fort

import "k8s.io/client-go/tools/cache"

// A state spec defines a set of sources and derived maps.
type StateSpec interface {
	New(name string, spec *SourceSpec) error
}

// Create a new empty spec
func NewSpec() StateSpec {
	return newSpec()
}

// Add a source to the spec. A source is a map
// that is updated by external logic. Everything in fort
// is derived from a source (or a map derived from a source, etc)
func NewExternalSource[K comparable]() *SourceSpec {
	return &SourceSpec{
		Create: func(s State, name string, isClone bool) (any, error) {
			return makeOrCloneMap[K](s.(*state), name, isClone), nil
		},
		Dependencies: []string{},
	}
}

// An external source can be updated using the given methods.
// All derived maps are updated when these operators are called.
// A source is just a map from key to value.
type ExternalSource[K comparable] interface {
	Update(key K, value any)
	Delete(key K)
}

func WrapInformer(informer cache.SharedInformer) *SourceSpec {
	return &SourceSpec{
		Create: func(s State, name string, isClone bool) (any, error) {
			return wrapInformer(informer)
		},
		Dependencies: []string{},
	}
}

// Define a new map created by joining two source maps on the given spec.
// This will create a new map with one entry for each pair of entries in the source maps.
// The keys of the resulting map will be a JoinKey, the values will be a JoinValue.
func FullJoin[LK, RK comparable](left, right string) *SourceSpec {
	return fullJoinFactory[LK, RK](left, right)
}

type LookupFunc[LK, RK comparable] func(kv *KeyValue[LK]) RK

// Define a new map created by joining two source maps on the given spec.
// This performs a "one-to-many" join between the two source maps.
// It will create a new map with one entry for each item in left with the corresponding
// entry given by the key returned from the lookup function provided.
// The keys of the resulting map will be the keys from the left map, the values will be a JoinValue.
func LookupJoin[LK, RK comparable](left, right string, lookupFunc LookupFunc[LK, RK]) *SourceSpec {
	return lookupJoinFactory(left, right, lookupFunc)
}

// Run map reduce on a given input map and generate a new map based on the operation.
//
// Logically map reduce will:
//   - call the mapper function on each key value pair in the original map. The mapper
//     function returns one or more key value pairs generated from the source key value pair.
//   - aggregate the results of all the mapper calls by key.
//   - call the reducer function on the set of values with a given key.
//   - save the reducer output in the result map with the given key.
func MapReduce[I, O comparable](mapper Mapper[I, O], reducer Reducer, source string) *SourceSpec {
	return newMapReduceFactory(mapper, reducer, source)
}

func Materialize[K comparable](source string) *SourceSpec {
	return newMaterializer[K](source)
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
func GetExternalSource[K comparable](s State, name string) ExternalSource[K] {
	v, _ := s.(*state).root.Get(name)
	return v.(ExternalSource[K])
}

// Get a reference to a derived map object given its name.
// Note that derived maps are read only. They are updated
// by the internal operators when source maps are updated.
func GetMap[K comparable](s State, name string) KeyValueMap[K] {
	v, _ := s.(*state).root.Get(name)
	return v.(KeyValueMap[K])
}

type KeyValueTarget interface {
	onUpdate(key any, value any, source KeyValueSource)
	onDelete(key any, value any, source KeyValueSource)
}

type KeyValueSource interface {
	addTarget(target KeyValueTarget)
}

// Get a reference to a derived map object given its name.
// Note that derived maps are read only. They are updated
// by the internal operators when source maps are updated.
func GetSource(s State, name string) KeyValueSource {
	v, _ := s.(*state).root.Get(name)
	return v.(KeyValueSource)
}

// A KeyValueMap is a simple read-only map interface.
type KeyValueMap[K comparable] interface {
	Get(key K) (any, bool)
	Has(key K) bool
	All() KeyValueIterator[K]

	Print()
	KeyValueSource
}

// This is the value returned by a join operation.
type JoinValue struct {
	Left  any
	Right any
}

type JoinKey[LK, RK comparable] struct {
	Left  LK
	Right RK
}

// Common reducers

var (
	Count    = CountReducer
	AnyValue = AnyValueReducer
	Sum      = SumReducer
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
type StrTriple [3]string

// Key value sets

// Key value.
// Note that the keyvalue pair is
// templated by key type but not result type.
// This is because arbitrary length key generation is
// expensive; typing the keys allows us to avoid this
// issue.
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
