package fort

import (
	"log"
	"maps"
)

func newMapReduceFactory[I, O comparable](mapper Mapper[I, O], reducer Reducer, source string) SourceSpec {
	return func(s State, name string, isClone bool) any {
		st := s.(*state)

		mr := &mapReducer[I, O]{
			owner:          nil,
			mapper:         mapper,
			reducer:        reducer,
			mapperResults:  makeOrCloneMap[I](st, "@mapper_"+name, isClone),
			reducerResults: makeOrCloneMap[O](st, "@reducer_"+name, isClone),
			results:        makeOrCloneMap[O](st, name, isClone),
		}

		sourceObj := GetSource(s, source)
		if sourceObj == nil {
			log.Fatalf("Couldn't find source %s", source)
		}
		sourceObj.addTarget(mr)

		return mr.results
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

var _ KeyValueTarget = &mapReducer[string, string]{}

func (m *mapReducer[I, O]) onUpdate(key any, value any, source KeyValueSource) {
	existingResults, foundExistingResults := m.mapperResults.Get(key.(I))

	results := m.mapper(&KeyValue[I]{Key: key.(I), Value: value})
	for _, res := range results {
		m.addToResults(res.Key, res.Value)
	}
	m.mapperResults.Update(key.(I), results)

	if foundExistingResults {
		for _, kv := range existingResults.(KeyValueSet[O]) {
			m.removeFromResults(kv.Key, kv.Value)
		}
	}
}

func (m *mapReducer[I, O]) onDelete(key any, value any, source KeyValueSource) {
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

func AnyValueReducer(owner any) ReducerEntry {
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

func MakeMap[K comparable](owner any) ReducerEntry {
	return &makeMapReducer[K]{
		owner:  owner,
		values: make(MakeMapMap[K]),
	}
}

type MakeMapValue struct {
	Count int64
	Value any
}

type MakeMapMap[K comparable] map[K]MakeMapValue

type makeMapReducer[K comparable] struct {
	owner  any
	values MakeMapMap[K]
}

func (m *makeMapReducer[K]) Add(value any) {
	kv := value.(KeyValue[K])
	if curr, found := m.values[kv.Key]; found {
		m.values[kv.Key] = MakeMapValue{
			Count: curr.Count + 1,
			Value: kv.Value,
		}
	} else {
		m.values[kv.Key] = MakeMapValue{
			Count: 1,
			Value: kv.Value,
		}
	}
}

func (m *makeMapReducer[K]) Remove(value any) bool {
	kv := value.(KeyValue[K])
	if curr, found := m.values[kv.Key]; found {
		if curr.Count > 1 {
			m.values[kv.Key] = MakeMapValue{
				Count: curr.Count - 1,
				Value: kv.Value,
			}
		} else {
			delete(m.values, kv.Key)
		}
	}
	return len(m.values) == 0
}

func (m *makeMapReducer[K]) Value() any {
	return m.values
}

func (m *makeMapReducer[K]) Clone(owner any) Cloneable {
	if owner != m.owner {
		return &makeMapReducer[K]{
			owner:  owner,
			values: maps.Clone(m.values),
		}
	}
	return m
}

func SumReducer(owner any) ReducerEntry {
	return &sumReducer{owner: owner}
}

type sumReducer struct {
	owner any
	sum   int
}

func (s *sumReducer) Add(value any) {
	s.sum += value.(int)
}

func (s *sumReducer) Remove(value any) bool {
	s.sum -= value.(int)
	return s.sum == 0
}

func (s *sumReducer) Value() any {
	return s.sum
}

func (s *sumReducer) Clone(owner any) Cloneable {
	if owner != s.owner {
		return &sumReducer{owner: owner, sum: s.sum}
	}
	return s
}
