package fort

import "log"

func newMapReduceFactory[I, O comparable](name string, mapper Mapper[I, O], reducer Reducer, source string) StateUpdateFunc {
	return func(s State, isClone bool) {
		st := s.(*state)

		mr := &mapReducer[I, O]{
			owner:          nil,
			mapper:         mapper,
			reducer:        reducer,
			mapperResults:  makeOrCloneMap[I](st, "_mapper_"+name, isClone),
			reducerResults: makeOrCloneMap[O](st, "_reducer_"+name, isClone),
			results:        makeOrCloneMap[O](st, name, isClone),
		}

		sourceObj := GetMap[I](s, source)
		if sourceObj == nil {
			log.Fatalf("Couldn't find source %s", source)
		}
		sourceObj.addTarget(mr)
	}
}

type mapReducer[I, O comparable] struct {
	owner          any
	mapper         Mapper[I, O]
	mapperResults  *CloneMap[I]
	reducer        Reducer
	reducerResults *CloneMap[O]
	results        *CloneMap[O]
}

var _ keyValueTarget = &mapReducer[string, string]{}

func (m *mapReducer[I, O]) onUpdate(key any, value any, source keyValueSource) {
	results := m.mapper(&KeyValue[I]{Key: key.(I), Value: value})
	for _, res := range results {
		m.addToResults(res.Key, res.Value)
	}
	m.mapperResults.Update(key.(I), results)
}

func (m *mapReducer[I, O]) onDelete(key any, value any, source keyValueSource) {
	if existing, found := m.mapperResults.Get(key.(I)); found {
		for _, kv := range existing.(KeyValueSet[O]) {
			m.removeFromResults(kv.Key, kv.Value)
		}
	}
	m.mapperResults.Delete(key.(I))
}

func (m *mapReducer[I, O]) addToResults(key O, value any) {
	var mutable ReducerEntry

	existing, found := m.reducerResults.Get(key)
	if found {
		mutable = existing.(ReducerEntry).Clone(m.owner).(ReducerEntry)
	} else {
		mutable = m.reducer(m.owner)
	}

	mutable.Add(value)
	if existing != mutable {
		m.reducerResults.Update(key, mutable)
	}

	m.results.Update(key, mutable.Value())
}

func (m *mapReducer[I, O]) removeFromResults(key O, value any) {
	if existing, found := m.reducerResults.Get(key); found {
		mutable := existing.(ReducerEntry).Clone(m.owner).(ReducerEntry)
		if mutable.Remove(value) {
			m.reducerResults.Delete(key)
			m.results.Delete(key)
		} else {
			m.reducerResults.Update(key, mutable)
			m.results.Update(key, mutable.Value())
		}
	}
}

func CountReducer(owner any) ReducerEntry {
	return &counter{owner: owner}
}

type counter struct {
	owner any
	count int64
}

func (c *counter) Add(value any) {
	c.count++
}

func (c *counter) Remove(value any) bool {
	c.count--
	return c.count == 0
}

func (c *counter) Value() any {
	return c.count
}

func (c *counter) Clone(owner any) Cloneable {
	if owner != c.owner {
		return &counter{owner: owner, count: c.count}
	}
	return c
}

func IdenticalReducer(owner any) ReducerEntry {
	return &identical{owner: owner}
}

type identical struct {
	owner any
	value any
	count int64
}

func (s *identical) Add(value any) {
	if s.value == nil && value != nil {
		s.value = value
	}
	s.count++
}

func (s *identical) Remove(value any) bool {
	s.count--
	return s.count == 0
}

func (s *identical) Value() any {
	return s.value
}

func (s *identical) Clone(owner any) Cloneable {
	if owner != s.owner {
		return &identical{owner: owner, count: s.count, value: s.value}
	}
	return s
}

func SumListReducer(listLength int) Reducer {
	return func(owner any) ReducerEntry {
		return &sumListReducer{sums: make([]int64, listLength)}
	}
}

type sumListReducer struct {
	owner any
	sums  []int64
}

func (s *sumListReducer) Add(value any) {
	v := value.([]int64)
	for i := range v {
		s.sums[i] += v[i]
	}
}

func (s *sumListReducer) Remove(value any) bool {
	allZero := true
	v := value.([]int64)
	for i := range v {
		s.sums[i] -= v[i]
		if s.sums[i] != 0 {
			allZero = false
		}
	}
	return allZero
}

func (s *sumListReducer) Value() any {
	return s.sums
}

func (s *sumListReducer) Clone(owner any) Cloneable {
	if owner != s.owner {
		newList := make([]int64, len(s.sums))
		copy(newList, s.sums)
		return &sumListReducer{owner: s.owner, sums: newList}
	}
	return s
}
