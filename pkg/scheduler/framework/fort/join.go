package fort

import "k8s.io/apimachinery/pkg/util/sets"

// Internal join operator used to implement different join types.
func Join[LK, RK comparable](s StateSpec, name, left, right string, matcherFactory JoinMatcherFactory[LK, RK]) {
	s.(*stateSpec).Register(
		func(s State, clonedState bool) {
			st := s.(*state)
			j := &joiner[LK, RK]{
				target:      makeOrCloneMap[JoinKey](st, name, clonedState),
				leftSource:  GetMap[LK](st, left),
				left:        makeOrCloneMap[LK](st, "_join_left_"+name, clonedState),
				rightSource: GetMap[RK](st, right),
				right:       makeOrCloneMap[RK](st, "_join_right_"+name, clonedState),
				matcher:     matcherFactory(s, "_join_matcher_"+name, clonedState),
			}

			j.leftSource.addTarget(j)
			j.rightSource.addTarget(j)
		},
	)
}

// Join structures
type JoinMatcherFactory[LK, RK comparable] func(s State, name string, isClone bool) JoinMatcher[LK, RK]

type JoinMatcher[LK, RK comparable] interface {
	LeftMatches(kv *KeyValue[RK], leftItems *CloneMap[LK]) KeyValueIterator[LK]
	RightMatches(kv *KeyValue[LK], rightItems *CloneMap[RK]) KeyValueIterator[RK]
	LeftUpdate(kv *KeyValue[LK])
	RightUpdate(kv *KeyValue[RK])
	LeftDelete(key LK)
	RightDelete(key RK)
}

type joiner[LK, RK comparable] struct {
	target      *CloneMap[JoinKey]
	leftSource  keyValueSource
	left        *CloneMap[LK]
	rightSource keyValueSource
	right       *CloneMap[RK]
	matcher     JoinMatcher[LK, RK]
}

var _ keyValueTarget = &joiner[string, int]{}

func (j *joiner[LK, RK]) onUpdate(key any, value any, source keyValueSource) {
	if source == j.leftSource {
		kv := KeyValue[LK]{Key: key.(LK), Value: value}
		rightItems := j.matcher.RightMatches(&kv, j.right)
		j.joinUpdate(KeyValueSet[LK]{kv}.All(), rightItems)
		j.left.Update(key.(LK), value)
		j.matcher.LeftUpdate(&kv)
	} else {
		kv := KeyValue[RK]{Key: key.(RK), Value: value}
		leftItems := j.matcher.LeftMatches(&kv, j.left)
		j.joinUpdate(leftItems, KeyValueSet[RK]{kv}.All())
		j.right.Update(key.(RK), value)
		j.matcher.RightUpdate(&kv)
	}
}

func (j *joiner[LK, RK]) onDelete(key any, value any, source keyValueSource) {
	//fmt.Printf("Join onDelete %s, %v\n", key, value)
	if source == j.leftSource {
		kv := KeyValue[LK]{Key: key.(LK), Value: value}
		rightItems := j.matcher.RightMatches(&kv, j.right)
		j.joinDelete(KeyValueSet[LK]{{Key: key.(LK), Value: value}}.All(), rightItems)
		j.left.Delete(key.(LK))
		j.matcher.LeftDelete(key.(LK))
	} else {
		kv := KeyValue[RK]{Key: key.(RK), Value: value}
		leftItems := j.matcher.LeftMatches(&kv, j.left)
		j.joinDelete(leftItems, KeyValueSet[RK]{{Key: key.(RK), Value: value}}.All())
		j.right.Delete(key.(RK))
		j.matcher.RightDelete(key.(RK))
	}
}

func (j *joiner[LK, RK]) joinUpdate(left KeyValueIterator[LK], right KeyValueIterator[RK]) {
	for lkey, lvalue := range left {
		for rkey, rvalue := range right {
			j.target.Update(
				JoinKey{lkey, rkey},
				JoinValue[LK, RK]{
					Left:  &KeyValue[LK]{Key: lkey, Value: lvalue},
					Right: &KeyValue[RK]{Key: rkey, Value: rvalue},
				},
			)
		}
	}
}

func (j *joiner[LK, RK]) joinDelete(left KeyValueIterator[LK], right KeyValueIterator[RK]) {
	for lkey := range left {
		for rkey := range right {
			//fmt.Printf("Join called delete %s,%s\n", lkey, rkey)
			j.target.Delete(JoinKey{lkey, rkey})
		}
	}
}

type fullJoinMatcher[LK, RK comparable] struct{}

var _ JoinMatcher[string, string] = &fullJoinMatcher[string, string]{}

func (m *fullJoinMatcher[LK, RK]) LeftMatches(kv *KeyValue[RK], items *CloneMap[LK]) KeyValueIterator[LK] {
	return items.All()
}

func (m *fullJoinMatcher[LK, RK]) RightMatches(kv *KeyValue[LK], items *CloneMap[RK]) KeyValueIterator[RK] {
	return items.All()
}

func (m *fullJoinMatcher[LK, RK]) LeftUpdate(kv *KeyValue[LK])  {}
func (m *fullJoinMatcher[LK, RK]) RightUpdate(kv *KeyValue[RK]) {}
func (m *fullJoinMatcher[LK, RK]) LeftDelete(key LK)            {}
func (m *fullJoinMatcher[LK, RK]) RightDelete(key RK)           {}

func fullJoinMatcherFactory[LK, RK comparable](s State, name string, isClone bool) JoinMatcher[LK, RK] {
	return &fullJoinMatcher[LK, RK]{}
}

type lookupJoinMatcher[LK, RK comparable] struct {
	index        *CloneMap[LK]
	reverseIndex *CloneMap[RK]
	getTargetKey func(kv *KeyValue[LK]) RK
}

var _ JoinMatcher[string, string] = &lookupJoinMatcher[string, string]{}

func (m *lookupJoinMatcher[LK, RK]) LeftMatches(kv *KeyValue[RK], items *CloneMap[LK]) KeyValueIterator[LK] {
	kvs := KeyValueSet[LK]{}
	entry, found := m.reverseIndex.Get(kv.Key)
	if found {
		for targetKey := range entry.(sets.Set[LK]) {
			if target, found := items.Get(targetKey); found {
				kvs = append(kvs, KeyValue[LK]{Key: targetKey, Value: target})
			}
		}
	}
	return kvs.All()
}

func (m *lookupJoinMatcher[LK, RK]) RightMatches(kv *KeyValue[LK], items *CloneMap[RK]) KeyValueIterator[RK] {
	var kvs KeyValueSet[RK]
	targetKey := m.getTargetKey(kv)
	target, found := items.Get(targetKey)
	if found {
		kvs = KeyValueSet[RK]{{Key: targetKey, Value: target}}
	}
	return kvs.All()
}

func (m *lookupJoinMatcher[LK, RK]) LeftUpdate(kv *KeyValue[LK]) {
	newKey := m.getTargetKey(kv)
	oldKey, found := m.index.Get(kv.Key)
	if !found || oldKey != newKey {
		m.index.Update(kv.Key, newKey)
		m.addToReverseIndex(newKey, kv.Key)
		m.removeFromReverseIndex(oldKey.(RK), kv.Key)
	}
}

func (m *lookupJoinMatcher[LK, RK]) RightUpdate(kv *KeyValue[RK]) {}

func (m *lookupJoinMatcher[LK, RK]) LeftDelete(key LK) {
	oldTargetKey, found := m.index.Get(key)
	if found {
		m.index.Delete(key)
		m.removeFromReverseIndex(oldTargetKey.(RK), key)
	}
}

func (m *lookupJoinMatcher[LK, RK]) RightDelete(key RK) {}

func (m *lookupJoinMatcher[LK, RK]) addToReverseIndex(sourceKey RK, targetKey LK) {
	var sourceSet sets.Set[LK]
	if currSourceSet, found := m.reverseIndex.Get(sourceKey); found {
		sourceSet = currSourceSet.(sets.Set[LK]).Clone()
		sourceSet.Insert(targetKey)
	} else {
		sourceSet = sets.New(targetKey)
	}
	m.reverseIndex.Update(sourceKey, sourceSet)
}

func (m *lookupJoinMatcher[LK, RK]) removeFromReverseIndex(sourceKey RK, targetKey LK) {
	if currSourceSet, found := m.reverseIndex.Get(sourceKey); found {
		sourceSet := currSourceSet.(sets.Set[LK]).Clone()
		sourceSet.Delete(targetKey)
		if len(sourceSet) == 0 {
			m.reverseIndex.Delete(sourceKey)
		} else {
			m.reverseIndex.Update(sourceKey, sourceSet)
		}
	}
}

func lookupJoinMatcherFactory[LK, RK comparable](getTarget LookupFunc[LK, RK]) JoinMatcherFactory[LK, RK] {
	return func(s State, name string, isClone bool) JoinMatcher[LK, RK] {
		st := s.(*state)
		return &lookupJoinMatcher[LK, RK]{
			index:        makeOrCloneMap[LK](st, "_index_"+name, isClone),
			reverseIndex: makeOrCloneMap[RK](st, "_rindex_"+name, isClone),
			getTargetKey: getTarget,
		}
	}
}
