package fort

import (
	"fmt"
	"log"
)

type StateUpdateFunc func(s State, clonedState bool)

type keyValueTarget interface {
	onUpdate(kv *KeyValue, source keyValueSource)
	onDelete(kv *KeyValue, source keyValueSource)
}

type keyValueSource interface {
	addTarget(target keyValueTarget)
}

func newState(spec StateSpec) State {
	s := &state{
		spec: spec,
		root: newCloneMap(map[string]any{}, nil, nil, 0),
	}
	spec.(*stateSpec).Update(s, false)
	return s
}

type state struct {
	spec StateSpec
	root *CloneMap
}

func (s *state) Source(name string) KeyValueSource {
	return Get[*CloneMap](s.root, name)
}

func (s *state) Get(name string) KeyValueMap {
	return Get[KeyValueMap](s.root, name)
}

func (s *state) Clone() State {
	newState := &state{
		spec: s.spec,
		root: newCloneMap(map[string]any{}, nil, nil, 0),
	}
	newState.root.root = newState.root

	for key, value := range s.root.data {
		cloned := value.(Cloneable).Clone(newState.root)
		newState.root.Update(key, cloned)
	}

	spec := newState.spec.(*stateSpec)
	spec.Update(newState, true)

	return newState
}

func (s *state) makeOrCloneMap(name string, isClone bool) *CloneMap {
	if isClone {
		if v, found := s.root.Get(name); found {
			return v.(*CloneMap)
		}
		log.Fatalf("Couldn't find map %s", name)
	}

	nm := newCloneMap(map[string]any{}, nil, s.root, 0)
	s.root.Update(name, nm)
	return nm
}

func (s *state) Print() {
	fmt.Printf("State{\n")
	for name, cmap := range s.root.All() {
		fmt.Printf("  %s:\n", name)
		cmap.(*CloneMap).Print()
	}
	fmt.Printf("}\n")
}

type stateSpec struct {
	updateFuncs []StateUpdateFunc
}

func newSpec() *stateSpec {
	return &stateSpec{
		updateFuncs: []StateUpdateFunc{},
	}
}

func (s *stateSpec) Register(f StateUpdateFunc) {
	s.updateFuncs = append(s.updateFuncs, f)
}

func (s *stateSpec) Update(st *state, clonedState bool) {
	for _, f := range s.updateFuncs {
		f(st, clonedState)
	}
}

func (s *stateSpec) Source(name string) {
	s.Register(
		func(s State, isClone bool) {
			s.(*state).makeOrCloneMap(name, isClone)
		},
	)
}

func (s *stateSpec) Join(name, left, right string) {
	s.Register(
		newJoinFactory(name, left, right),
	)
}

func (s *stateSpec) MapReduce(name string, mapper Mapper, reducer Reducer, source string) {
	s.Register(
		newMapReduceFactory(name, mapper, reducer, source),
	)
}
