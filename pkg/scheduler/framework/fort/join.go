package fort

import "fmt"

func newJoinFactory(name, left, right string) StateUpdateFunc {
	return func(s State, clonedState bool) {
		st := s.(*state)
		j := &joiner{
			target:      st.makeOrCloneMap(name, clonedState),
			leftSource:  st.Get(left),
			left:        st.makeOrCloneMap("_join_left_"+name, clonedState),
			rightSource: st.Get(right),
			right:       st.makeOrCloneMap("_join_right_"+name, clonedState),
		}

		j.leftSource.addTarget(j)
		j.rightSource.addTarget(j)
	}
}

type joiner struct {
	target      *CloneMap
	leftSource  keyValueSource
	left        *CloneMap
	rightSource keyValueSource
	right       *CloneMap
}

var _ keyValueTarget = &joiner{}

func (j *joiner) onUpdate(kv *KeyValue, source keyValueSource) {
	if source == j.leftSource {
		j.joinUpdate(KeyValueSet{kv.Key: kv.Value}.All(), j.right.All())
		j.left.Update(kv.Key, kv.Value)
	} else {
		j.joinUpdate(j.left.All(), KeyValueSet{kv.Key: kv.Value}.All())
		j.right.Update(kv.Key, kv.Value)
	}
}

func (j *joiner) onDelete(kv *KeyValue, source keyValueSource) {
	fmt.Printf("Join onDelete %s, %v\n", kv.Key, kv.Value)
	if source == j.leftSource {
		j.joinDelete(KeyValueSet{kv.Key: kv.Value}.All(), j.right.All())
		j.left.Delete(kv.Key)
	} else {
		j.joinDelete(j.left.All(), KeyValueSet{kv.Key: kv.Value}.All())
		j.right.Delete(kv.Key)
	}
}

func (j *joiner) joinUpdate(left, right KeyValueIterator) {
	for lkey, lvalue := range left {
		for rkey, rvalue := range right {
			j.target.Update(
				lkey+"~"+rkey,
				JoinValue{
					Left:  KeyValue{Key: lkey, Value: lvalue},
					Right: KeyValue{Key: rkey, Value: rvalue},
				},
			)
		}
	}
}

func (j *joiner) joinDelete(left, right KeyValueIterator) {
	for lkey := range left {
		for rkey := range right {
			fmt.Printf("Join called delete %s,%s\n", lkey, rkey)
			j.target.Delete(lkey + "~" + rkey)
		}
	}
}
