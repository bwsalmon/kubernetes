package fort

type StateSpec interface {
	Source(name string)
	Join(name, left, right string)
	MapReduce(name string, mapper Mapper, reducer Reducer, source string)
}

func NewSpec() StateSpec {
	return newSpec()
}

type State interface {
	Get(name string) KeyValueMap
	Source(name string) KeyValueSource

	Clone() State

	Print()
}

func New(spec StateSpec) State {
	return newState(spec)
}

// Join structures

// This is the value returned by a join operation.
type JoinValue struct {
	Left  KeyValue
	Right KeyValue
}

// Common reducers

var (
	Count     = CountReducer
	Identical = IdenticalReducer
	SumList   = SumListReducer
)

// MapReduce structures

type Mapper func(kv *KeyValue) KeyValueSet
type Reducer func(owner any) ReducerEntry

type Cloneable interface {
	Clone(root any) Cloneable
}

type ReducerEntry interface {
	Add(value any)
	Remove(value any) bool
	Value() any

	Cloneable
}

// Key value sets

type KeyValueSource interface {
	Update(key string, value any)
	Delete(key string)
}

type KeyValue struct {
	Key   string
	Value any
}

type KeyValueSet map[string]any

type KeyValueIterator func(yield func(key string, value any) bool)

type KeyValueMap interface {
	Get(key string) (any, bool)
	Has(key string) bool
	All() KeyValueIterator

	Print()

	keyValueSource
}

func (s KeyValueSet) All() KeyValueIterator {
	return func(yield func(key string, value any) bool) {
		for key, value := range s {
			if !yield(key, value) {
				return
			}
		}
	}
}
