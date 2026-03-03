package fort

type mapReducer[IK comparable, IV any, OK comparable, MV any, OV any] struct {
	mapper         Mapper[IK, IV, OK, MV]
	mapperResults  *CloneMap[IK, KeyValueSet[OK, MV]]
	reducer        Reducer[MV, OV]
	reducerResults *CloneMap[OK, ReducerEntry[MV, OV]]
	target         *keyValueConnector[OK, OV]
}

var _ Target[uint32, int32] = &mapReducer[uint32, int32, uint64, int64, int16]{}

func newMapReducer[
	InputKeyType comparable,
	InputValueType any,
	OutputKeyType comparable,
	MappedValueType any,
	OutputValueType any,
](
	mapper Mapper[InputKeyType, InputValueType, OutputKeyType, MappedValueType],
	reducer Reducer[MappedValueType, OutputValueType],
	source Source[InputKeyType, InputValueType],
	cloneFrom Source[OutputKeyType, OutputValueType],
) *mapReducer[InputKeyType, InputValueType, OutputKeyType, MappedValueType, OutputValueType] {
	newMr := &mapReducer[InputKeyType, InputValueType, OutputKeyType, MappedValueType, OutputValueType]{
		mapper:  mapper,
		reducer: reducer,
		target:  newKeyValueConnector[OutputKeyType, OutputValueType](),
	}

	if cloneFrom == nil {
		newMr.mapperResults = makeOrCloneMap[InputKeyType, KeyValueSet[OutputKeyType, MappedValueType]](nil)
		newMr.reducerResults = makeOrCloneMap[OutputKeyType, ReducerEntry[MappedValueType, OutputValueType]](nil)
	} else {
		c := cloneFrom.(*mapReducer[InputKeyType, InputValueType, OutputKeyType, MappedValueType, OutputValueType])
		newMr.mapperResults = c.mapperResults.Clone()
		newMr.reducerResults = c.reducerResults.Clone()
	}

	source.addTarget(newMr)

	return newMr
}

func (m *mapReducer[IK, IV, OK, MV, OV]) OnUpdate(key IK, value IV, source Source[IK, IV]) error {
	existingResults, foundExistingResults := m.mapperResults.Get(key)

	results := m.mapper(&KeyValue[IK, IV]{Key: key, Value: value})
	for _, res := range results {
		m.addToResults(res.Key, res.Value)
	}
	m.mapperResults.Update(key, results)

	if foundExistingResults {
		for _, kv := range existingResults {
			m.removeFromResults(kv.Key, kv.Value)
		}
	}

	return nil
}

func (m *mapReducer[IK, IV, OK, MV, OV]) OnDelete(key IK, value IV, source Source[IK, IV]) error {
	if existing, found := m.mapperResults.Get(key); found {
		for _, kv := range existing {
			m.removeFromResults(kv.Key, kv.Value)
		}
	}
	m.mapperResults.Delete(key)
	return nil
}

func (m *mapReducer[IK, IV, OK, MV, OV]) addToResults(key OK, value MV) {
	var mutable ReducerEntry[MV, OV]

	existing, found, isMutable := m.reducerResults.GetMutability(key)
	if found {
		mutable = existing.(ReducerEntry[MV, OV])
		if !isMutable {
			mutable = mutable.Clone()
		}
	} else {
		mutable = m.reducer()
	}

	if existing != mutable {
		m.reducerResults.Update(key, mutable)
	}

	changed := mutable.Add(value)
	if changed {
		v := mutable.Value()
		m.target.Update(key, v)
	}
}

func (m *mapReducer[IK, IV, OK, MV, OV]) removeFromResults(key OK, value MV) {
	if existing, found, isMutable := m.reducerResults.GetMutability(key); found {
		mutable := existing.(ReducerEntry[MV, OV])
		if !isMutable {
			mutable = mutable.Clone()
		}
		changed, empty := mutable.Remove(value)
		if changed {
			if empty {
				m.reducerResults.Delete(key)
				var emptyVal OV
				m.target.Delete(key, emptyVal)
			} else {
				v := mutable.Value()
				m.reducerResults.Update(key, mutable)
				m.target.Update(key, v)
			}
		}
	}
}

func (m *mapReducer[IK, IV, OK, MV, OV]) addTarget(t Target[OK, OV]) {
	m.target.addTarget(t)
}

func (m *mapReducer[IK, IV, OK, MV, OV]) Print() {
	print("Mapper")
	m.mapperResults.Print()
	print("Reducer")
	m.reducerResults.Print()
}
