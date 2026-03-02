package fort

import (
	"log"
)

func newMapReduceFactory[I, O comparable](name string, mapper Mapper[I, O], reducer Reducer, source string) EntitySpec {
	return EntitySpec{
		{
			Name: name,
			Create: func(s DataFort, name string, isClone bool) (any, error) {
				st := s.(*dataFort)

				mr := &mapReducer[I, O]{
					owner:          nil,
					mapper:         mapper,
					reducer:        reducer,
					mapperResults:  makeOrCloneMap[I](st, "@mapper_"+name, isClone),
					reducerResults: makeOrCloneMap[O](st, "@reducer_"+name, isClone),
					results:        makeOrCloneMap[O](st, name, isClone),
				}

				sourceObj := getSource(st, source)
				if sourceObj == nil {
					log.Fatalf("Couldn't find source %s", source)
				}
				sourceObj.addTarget(mr)

				return mr.results, nil
			},
			Dependencies: []string{source},
		},
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

func (m *mapReducer[I, O]) OnUpdate(key any, value any, source KeyValueSource) {
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

func (m *mapReducer[I, O]) OnDelete(key any, value any, source KeyValueSource) {
	if existing, found := m.mapperResults.Get(key.(I)); found {
		for _, kv := range existing.(KeyValueSet[O]) {
			m.removeFromResults(kv.Key, kv.Value)
		}
	}
	m.mapperResults.Delete(key.(I))
}

func (m *mapReducer[I, O]) addToResults(key O, value any) {
	var mutable ReducerEntry

	existing, found, isMutable := m.reducerResults.GetMutability(key)
	if found {
		mutable = existing.(ReducerEntry)
		if !isMutable {
			mutable = mutable.Clone()
		}
	} else {
		mutable = m.reducer()
	}

	changed := mutable.Add(value)
	if existing != mutable {
		m.reducerResults.Update(key, mutable)
	}

	if changed {
		m.results.Update(key, mutable.Value())
	}
}

func (m *mapReducer[I, O]) removeFromResults(key O, value any) {
	if existing, found, isMutable := m.reducerResults.GetMutability(key); found {
		mutable := existing.(ReducerEntry)
		if !isMutable {
			mutable = mutable.Clone()
		}
		changed, empty := mutable.Remove(value)
		if changed {
			if empty {
				m.reducerResults.Delete(key)
				m.results.Delete(key)
			} else {
				m.reducerResults.Update(key, mutable)
				m.results.Update(key, mutable.Value())
			}
		}
	}
}
