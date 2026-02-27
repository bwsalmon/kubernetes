package fort

import (
	"fmt"
	"maps"
	"slices"

	"k8s.io/apimachinery/pkg/util/sets"
)

func fullJoinFactory[LK, RK comparable](name, left, right string) EntitySpec {
	return EntitySpec{
		{
			Name: name,
			Create: func(s DataFort, name string, clonedState bool) (any, error) {
				st := s.(*dataFort)
				j := &fullJoiner[LK, RK]{
					target:      newKeyValueConnector[JoinKey[LK, RK]](),
					leftSource:  getSource(st, left),
					left:        makeOrCloneMap[LK](st, "@join_left_"+name, clonedState),
					rightSource: getSource(st, right),
					right:       makeOrCloneMap[RK](st, "@join_right_"+name, clonedState),
				}

				j.leftSource.addTarget(j)
				j.rightSource.addTarget(j)

				return j.target, nil
			},
			Dependencies: []string{left, right},
		},
	}
}

type fullJoiner[LK, RK comparable] struct {
	target      *keyValueConnector[JoinKey[LK, RK]]
	leftSource  KeyValueSource
	left        *CloneMap[LK]
	rightSource KeyValueSource
	right       *CloneMap[RK]
}

var _ KeyValueTarget = &fullJoiner[string, int]{}

func (j *fullJoiner[LK, RK]) OnUpdate(key any, value any, source KeyValueSource) {
	if source == j.leftSource {
		kv := KeyValue[LK]{Key: key.(LK), Value: value}
		j.joinUpdate(KeyValueSet[LK]{kv}.All(), j.right.All())
		j.left.Update(key.(LK), value)
	} else {
		kv := KeyValue[RK]{Key: key.(RK), Value: value}
		j.joinUpdate(j.left.All(), KeyValueSet[RK]{kv}.All())
		j.right.Update(key.(RK), value)
	}
}

func (j *fullJoiner[LK, RK]) OnDelete(key any, value any, source KeyValueSource) {
	//fmt.Printf("Join onDelete %s, %v\n", key, value)
	if source == j.leftSource {
		j.joinDelete(KeyValueSet[LK]{{Key: key.(LK), Value: value}}.All(), j.right.All())
		j.left.Delete(key.(LK))
	} else {
		j.joinDelete(j.left.All(), KeyValueSet[RK]{{Key: key.(RK), Value: value}}.All())
		j.right.Delete(key.(RK))
	}
}

func (j *fullJoiner[LK, RK]) joinUpdate(left KeyValueIterator[LK], right KeyValueIterator[RK]) {
	for lkey, lvalue := range left {
		for rkey, rvalue := range right {
			j.target.Update(
				JoinKey[LK, RK]{lkey, rkey},
				JoinValue{
					Left:  lvalue,
					Right: rvalue,
				},
			)
		}
	}
}

func (j *fullJoiner[LK, RK]) joinDelete(left KeyValueIterator[LK], right KeyValueIterator[RK]) {
	for lkey, lval := range left {
		for rkey, rval := range right {
			//fmt.Printf("Join called delete %s,%s\n", lkey, rkey)
			j.target.Delete(
				JoinKey[LK, RK]{lkey, rkey},
				JoinValue{
					Left:  lval,
					Right: rval,
				},
			)
		}
	}
}

type LookupFunc[LK, RK comparable] func(kv *KeyValue[LK]) RK

func LookupJoin[LK, RK comparable](name, left, right string, lookupFunc LookupFunc[LK, RK]) EntitySpec {
	return lookupJoinFactory(name, left, right, lookupFunc)
}

func lookupJoinFactory[LK, RK comparable](name, left, right string, lookupFunc LookupFunc[LK, RK]) EntitySpec {
	return EntitySpec{
		{
			Name: name,
			Create: func(s DataFort, name string, clonedState bool) (any, error) {
				st := s.(*dataFort)
				j := &lookupJoiner[LK, RK]{
					target:       makeOrCloneMap[LK](st, name, clonedState),
					leftSource:   getSource(st, left),
					left:         makeOrCloneMap[LK](st, "@join_left_"+name, clonedState),
					rightSource:  getSource(st, right),
					right:        makeOrCloneMap[RK](st, "@join_right_"+name, clonedState),
					reverseIndex: makeOrCloneMap[RK](st, "@join_rindex_"+name, clonedState),
					getTargetKey: lookupFunc,
				}

				j.leftSource.addTarget(j)
				j.rightSource.addTarget(j)

				return j.target, nil
			},
			Dependencies: []string{left, right},
		},
	}
}

type lookupJoiner[LK, RK comparable] struct {
	target       *CloneMap[LK]
	leftSource   KeyValueSource
	left         *CloneMap[LK]
	rightSource  KeyValueSource
	right        *CloneMap[RK]
	reverseIndex *CloneMap[RK]
	getTargetKey func(kv *KeyValue[LK]) RK
}

var _ KeyValueTarget = &lookupJoiner[string, int]{}

func (j *lookupJoiner[LK, RK]) OnUpdate(inKey any, value any, source KeyValueSource) {
	if source == j.leftSource {
		key := inKey.(LK)
		targetKey := j.getTargetKey(&KeyValue[LK]{Key: key, Value: value})

		if existingValue, found := j.left.Get(key); found {
			existingTargetKey := j.getTargetKey(&KeyValue[LK]{Key: key, Value: existingValue})
			if targetKey != existingTargetKey {
				j.removeFromReverseIndex(existingTargetKey, key)
			}
		}

		if targetValue, found := j.right.Get(targetKey); found {
			j.target.Update(key, JoinValue{
				Left:  value,
				Right: targetValue,
			})
		} else {
			j.target.Delete(key)
		}

		j.addToReverseIndex(targetKey, key)
		j.left.Update(key, value)
	} else {
		key := inKey.(RK)
		entry, found := j.reverseIndex.Get(key)
		if found {
			for targetKey := range *(entry.(*sets.Set[LK])) {
				if targetValue, found := j.left.Get(targetKey); found {
					j.target.Update(targetKey, JoinValue{
						Left:  targetValue,
						Right: value,
					})
				}
			}
		}
		j.right.Update(key, value)
	}
}

func (j *lookupJoiner[LK, RK]) OnDelete(inKey any, value any, source KeyValueSource) {
	if source == j.leftSource {
		key := inKey.(LK)
		j.target.Delete(key)
		targetKey := j.getTargetKey(&KeyValue[LK]{Key: key, Value: value})
		j.removeFromReverseIndex(targetKey, key)
		j.left.Delete(key)
	} else {
		key := inKey.(RK)
		if entry, found := j.reverseIndex.Get(key); found {
			for targetKey := range *(entry.(*sets.Set[LK])) {
				j.target.Delete(targetKey)
			}
		}
		j.right.Delete(key)
	}
}

func (m *lookupJoiner[LK, RK]) addToReverseIndex(sourceKey RK, targetKey LK) {
	var sourceSet sets.Set[LK]
	if currSourceSet, found := m.reverseIndex.Get(sourceKey); found {
		sourceSet = currSourceSet.(*sets.Set[LK]).Clone()
		sourceSet.Insert(targetKey)
	} else {
		sourceSet = sets.New(targetKey)
	}
	m.reverseIndex.Update(sourceKey, &sourceSet)
}

func (m *lookupJoiner[LK, RK]) removeFromReverseIndex(sourceKey RK, targetKey LK) {
	if currSourceSet, found := m.reverseIndex.Get(sourceKey); found {
		sourceSet := currSourceSet.(*sets.Set[LK]).Clone()
		sourceSet.Delete(targetKey)
		if len(sourceSet) == 0 {
			m.reverseIndex.Delete(sourceKey)
		} else {
			m.reverseIndex.Update(sourceKey, &sourceSet)
		}
	}
}

func newUnion[KeyType comparable](name string, sources map[string]string) EntitySpec {
	return EntitySpec{
		{
			Name: name,
			Create: func(s DataFort, name string, clonedState bool) (any, error) {
				st := s.(*dataFort)

				union := &unioner[KeyType]{
					sources: map[KeyValueSource]string{},
				}

				for sourceAlias, sourceName := range sources {
					val, found := st.Get(sourceName)
					if !found {
						return nil, fmt.Errorf("Couldn't find source %s", sourceName)
					}
					source := val.(KeyValueSource)
					union.sources[source] = sourceAlias

					source.addTarget(union)
				}

				return union, nil
			},
			Dependencies: slices.Collect(maps.Keys(sources)),
		},
	}
}

type unioner[KeyType comparable] struct {
	keyValueConnector[UnionKey[KeyType]]
	sources map[KeyValueSource]string
}

func (u *unioner[K]) OnUpdate(key any, value any, source KeyValueSource) {
	u.keyValueConnector.Update(UnionKey[K]{
		SourceName: u.sources[source],
		Key:        key.(K),
	}, value)
}

func (u *unioner[K]) OnDelete(key any, value any, source KeyValueSource) {
	u.keyValueConnector.Delete(UnionKey[K]{
		SourceName: u.sources[source],
		Key:        key.(K),
	}, value)
}

func mergeJoinFactory[KeyType comparable](name string, sources map[string]string) EntitySpec {
	unionName := "@mergeJoin_" + name

	allSources := newUnion[KeyType](unionName, sources)

	join := newMapReduceFactory(
		name,
		func(kv *KeyValue[UnionKey[KeyType]]) KeyValueSet[KeyType] {
			return KeyValueSet[KeyType]{
				KeyValue[KeyType]{
					Key: kv.Key.Key,
					Value: KeyValue[string]{
						Key:   kv.Key.SourceName,
						Value: kv.Value,
					},
				},
			}
		},
		Map[string],
		unionName,
	)

	return slices.Concat(allSources, join)
}
