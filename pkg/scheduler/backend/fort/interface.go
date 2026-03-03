/*
Fort is almost a database (it's a datafort!).

It is an in-memory engine for map-reduce and join operations. All data in Fort is represented as memory key-value maps.
All the results of map-reduce and join are automatically updated as source data is updated.
All of the maps are fast cloneable so the entire "db" can be cloned quickly.
This makes it simple to generate complex data structures that are automatically
updated as data changes, and are easily fast cloneable.
*/
package fort

import (
	"golang.org/x/exp/constraints"
	"k8s.io/apimachinery/pkg/util/sets"
)

/*
A DataFort is a user defined object containing Fort objects. All related objects must
be contained in the same Fort object.

	 For example, a trivial DataFort might look like:

		type HelloWorldFort struct {
			Input fort.WriteMap[string, string]
			Output fort.ReadMap[string, string]
		}

		func (f *HelloWorldFort) InitOrClone(cloneFrom *HelloWorldFort) {
		    // A place for user input.
			f.Input = fort.NewWriteMap(cloneFrom.Input)
		    // Record the user input into a readable map.
			f.Output = fort.NewReadMap(f.Input, cloneFrom.Output)
		}

		func testStuff() {
		   // Create a new HelloWorldFort.
		   myFort := fort.New[HelloWorldFort]()

		   // Add data.
		   myFort.Input.Update("hello", "world")

		   // Should get "world".
		   myFort.Output.Get("hello")

		   // Clone the fort.
		   myClone := fort.Clone(myFort)

		   // Should get "world".
		   myClone.Output.Get("hello")
		}

Immutable data:

Fort assumes that keys and values passed in by the user are immutable. Once a value is given to Fort (and
when it is read out), the user must not edit it. If the user wishes to store complex objects they are
responsible for creating new copies when editing them, and then passing them in using Updates to WriteMaps.

Cloning:

Clones act like deep clones of the data, but they are actually implemented using copy-on-write mechanisms
internally, making clones very fast.

Concurrency:

Note that while Fort ensures that Clones of the same fort can be safely used concurrently, it does not
ensure that a given clone can be used concurrently in a consistent fashion. If the user
needs concurrent access to individual clones, they must add locking in the fort itself for updates / reads.
*/
type DataFort[T any] interface {
	// Initialize a currently empty DataFort. Don't call directly,
	// use fort.New and fort.Clone instead.
	//
	// If this is a new DataFort then an empty struct
	// will be passed into cloneFrom.
	//
	// If this is a clone, the struct from which to clone
	// will be passed into cloneFrom.
	//
	// In general the implementer should only need to pass the appropriate field
	// from cloneFrom into the operator call (NewReadMap, etc).
	// They can then treat initialization and cloning as identical.
	InitOrClone(cloneFrom *T)
}

// All data in Fort eventually derives from WriteMaps. A
// WriteMap starts empty, but the user can update and delete
// entries using the interface.
//
// Note that WriteMaps are not readable directly.
//
// WriteMaps can them be used as Sources for other
// transformations.
type WriteMap[K comparable, V any] interface {
	// Update the given key to point to the given value.
	// Note that values passed into Fort are then owned by
	// Fort, the caller must not edit them.
	//
	// Calling this function will update all of the dependent objects
	// so when it returns the other data structures will be consistent.
	Update(key K, value V) error

	// Delete the given key from the map. Note that we require the
	// caller to pass in the deleted item so we don't have to actually
	// keep a physical map internally.
	//
	// Calling this function will update all of the dependent objects
	// so when it returns the other data structures will be consistent.
	Delete(key K, value V) error

	Source[K, V]
}

// Create a new WriteMap. If cloneFrom is nil, create a new empty WriteMap.
// If cloneFrom is not nil, create a new WriteMap that is a clone
// of the cloneFrom map.
func NewWriteMap[K comparable, V any](cloneFrom WriteMap[K, V]) WriteMap[K, V] {
	return newWriteMap[K, V]()
}

// ReadMaps are the way to get data out of Fort.
// A ReadMap materializes a source so that the user
// can query items from it as needed.
type ReadMap[K comparable, V any] interface {
	// Get the value for the given key. The boolean
	// is true if the key was found, false otherwise.
	Get(key K) (V, bool)

	// Return an iterator for the entire map.
	// Can be used like:
	// for k, v := range myMap.All() { }
	All() KeyValueIterator[K, V]

	// Print the map, used in debugging.
	Print()

	Target[K, V]
}

// Create a new ReadMap that captures data from the source.
// If cloneFrom is nil, this creates a new map, if cloneFrom is not
// nil the ReadMap will be a clone of the source ReadMap.
func NewReadMap[K comparable, V any](source Source[K, V], cloneFrom ReadMap[K, V]) ReadMap[K, V] {
	var newMap *readMap[K, V]
	if cloneFrom == nil {
		newMap = newReadMap[K, V]()
	} else {
		newMap = cloneFrom.(*readMap[K, V]).Clone()
	}
	source.addTarget(newMap)
	return newMap
}

// Define a new derived source generated by running map reduce on the given source.
// This can in turn be used as a source for other transforms.
//
// Logically map reduce will:
//   - call the mapper function on each key value pair in the original source. The mapper
//     function returns one or more key value pairs generated from the source key value pair.
//   - aggregate the results of all the mapper calls by key.
//   - call the reducer function on the set of values with a given key.
//   - save the reducer output with the given key.
//
// If cloneFrom is nil, then this creates a new empty MapReduce. If cloneFrom is not nil,
// it will be cloned from cloneFrom.
//
// Note that the input arguments should define all of the necessary template parameters
// automatically.
func MapReduce[
	InputKeyType comparable,
	InputValueType any,
	OutputKeyType comparable,
	MappedValueType any,
	OutputValueType any,
](
	source Source[InputKeyType, InputValueType],
	mapper Mapper[InputKeyType, InputValueType, OutputKeyType, MappedValueType],
	reducer Reducer[MappedValueType, OutputValueType],
	cloneFrom Source[OutputKeyType, OutputValueType],
) Source[OutputKeyType, OutputValueType] {
	return newMapReducer(mapper, reducer, source, cloneFrom)
}

// Define a new derived source created by joining two sources.
// Logically FullJoin will create a new source with one entry for each pair of entries in the sources.
// The keys of the result will be JoinKeys, the values will be JoinValues.
//
// If cloneFrom is nil, then this creates a new empty join. If cloneFrom is not nil,
// it will be cloned from cloneFrom.
//
// Note that the input arguments should define all of the necessary template parameters
// automatically.
func FullJoin[LeftKeyType comparable, LeftValueType any, RightKeyType comparable, RightValueType any](
	left Source[LeftKeyType, LeftValueType],
	right Source[RightKeyType, RightValueType],
	cloneFrom Source[JoinKey[LeftKeyType, RightKeyType], JoinValue[LeftValueType, RightValueType]],
) Source[JoinKey[LeftKeyType, RightKeyType], JoinValue[LeftValueType, RightValueType]] {
	return newFullJoiner(left, right, cloneFrom)
}

// Interfaces for key value pairs.

type KeyValue[KeyType comparable, ValueType any] struct {
	// Note that we use generics for keys because the number
	// of lookup and manipulation operations we do on keys
	// makes using interfaces expensive. Values are generally
	// just passed around, so using an interface works fine.
	Key KeyType

	// Note that values in Fort must be immutable. Once a value
	// is passed in using Update or from the result of a mapper,
	// it cannot be changed.
	Value ValueType
}

// An iterator over KeyValue sets. Note that these iterators can be used
// in range operations.
type KeyValueIterator[KeyType comparable, ValueType any] func(yield func(key KeyType, value ValueType) bool)

// Convenience wrapper for a list of key value pairs.
type KeyValueSet[KeyType comparable, ValueType any] []KeyValue[KeyType, ValueType]

// MapReduce types.

// Map from an input key value pair to a set of output key value pairs.
// Note that the values used in the output key value pairs must be immutable;
// once returned they are owned by Fort and cannot be changed.

type Mapper[InputKey comparable, InputValue any, OutputKey comparable, OutputValue any] func(kv *KeyValue[InputKey, InputValue]) KeyValueSet[OutputKey, OutputValue]

// Common reducers.
// To define a custom reducer, see the interfaces in reducers.go.

// Count the number of entries with the given key.
func Count[I any]() ReducerEntry[I, int64] {
	return &counter[I]{}
}

// Return some value for a given key. Useful for scenarios where
// only the key matters or the value is always the same for a given key.
// Note that this will return some value that has *at some point* in the
// past been assigned to this key. The value may not currently exist in the
// data set.
func AnyValue[T comparable]() ReducerEntry[T, T] {
	return &anyValue[T]{}
}

// Return the set of unique objects with a given key.
func Distinct[T comparable]() ReducerEntry[T, sets.Set[T]] {
	return &setReducer[T]{
		values: make(map[T]int),
	}
}

// Return the sum of all values with a given key.
// Note that this currently works only on Integer types.
func Sum[T constraints.Integer]() ReducerEntry[T, T] {
	return &sumReducer[T]{}
}

// Types used in join.

// The key type used in the result from Joins.
type JoinKey[LeftKeyType, RightKeyType comparable] struct {
	Left  LeftKeyType
	Right RightKeyType
}

// The value type used in the results from Joins.
type JoinValue[L, R any] struct {
	Left  L
	Right R
}

// Create a new DataFort of type T.
// The generics syntax is funky, but is called like:
//
//	newFort := fort.New[MyFortType]()
func New[T any, PT interface {
	*T
	DataFort[T]
}]() *T {
	newFort := new(T)
	var emptyFort T
	PT(newFort).InitOrClone(&emptyFort)
	return newFort
}

// Clone a DataFort.
// The generics syntax is funky, but is called like:
//
//	clonedFort := fort.Clone(existingFort)
func Clone[T any, PT interface {
	*T
	DataFort[T]
}](toClone *T) *T {
	newFort := new(T)
	PT(newFort).InitOrClone(toClone)
	return newFort
}
