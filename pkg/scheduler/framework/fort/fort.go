package fort

type StateSpec interface{}

func NewSpec() StateSpec {
	return newSpec()
}

func NewSource[K comparable](s StateSpec, name string) {
	s.(*stateSpec).Register(
		func(s State, isClone bool) {
			makeOrCloneMap[K](s.(*state), name, isClone)
		},
	)
}

func FullJoin[LK, RK comparable](s StateSpec, name, left, right string) {
	Join(s, name, left, right, getAllItems[LK, RK], getAllItems[RK, LK])
}

func KeyJoin[K comparable](s StateSpec, name, left, right string) {
	Join(s, name, left, right, lookupByKey[K], lookupByKey[K])
}

func Join[LK, RK comparable](s StateSpec, name, left, right string, leftLookup LookupFunc[RK, LK], rightLookup LookupFunc[LK, RK]) {
	s.(*stateSpec).Register(newJoinFactory(name, left, right, leftLookup, rightLookup))
}

func MapReduce[I, O comparable](s StateSpec, name string, mapper Mapper[I, O], reducer Reducer, source string) {
	s.(*stateSpec).Register(newMapReduceFactory(name, mapper, reducer, source))
}

type State interface {
	Clone() State
	Print()
}

func New(spec StateSpec) State {
	return newState(spec)
}

func Source[K comparable](s State, name string) KeyValueSource[K] {
	v, _ := s.(*state).root.Get(name)
	return v.(KeyValueSource[K])
}

func GetMap[K comparable](s State, name string) KeyValueMap[K] {
	v, _ := s.(*state).root.Get(name)
	return v.(KeyValueMap[K])
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

type KeyValueSource[K comparable] interface {
	Update(key K, value any)
	Delete(key K)
}

type KeyValue[K comparable] struct {
	Key   K
	Value any
}

type KeyValueSet[K comparable] []KeyValue[K]

type KeyValueIterator[K comparable] func(yield func(key K, value any) bool)

type KeyValueMap[K comparable] interface {
	Get(key K) (any, bool)
	Has(key K) bool
	All() KeyValueIterator[K]

	Print()

	keyValueSource
}

func (s KeyValueSet[K]) All() KeyValueIterator[K] {
	return func(yield func(key K, value any) bool) {
		for _, kv := range s {
			if !yield(kv.Key, kv.Value) {
				return
			}
		}
	}
}
