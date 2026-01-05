package fort

import (
	"maps"

	"k8s.io/apimachinery/pkg/util/sets"
)

// Create a new reducer entry owned by the given object.
type Reducer func(owner any) ReducerEntry

// An individual reducer entry.
type ReducerEntry interface {
	// Add a value to the reducer. The result
	// should be true if the Value() result has changed
	// after the add, false if it has not.
	Add(value any) bool

	// Remove a value from the reducer.
	//
	// The "changed" result should be true if the Value()
	// result has changed after the removal, false if it has not.
	//
	// The "empty" result should be true if the given reducer entry
	// is now empty and can be removed from our maps, false otherwise.
	Remove(value any) (changed, empty bool)

	// Return the current value of the reducer.
	// Note that while the internal state of the reducer is mutable
	// (Fort manages the state using CloneIfNotOwneed),
	// the values returned by Value() must be immmutable,
	// as with other values pased into the system.
	Value() any

	// If the reducer value is owned by the given
	// owner then return the current entry. If
	// it is owned by some other value, then
	// return a new copy of the reducer that is now
	// owned by the provided owner.
	CloneIfNotOwned(owner any) ReducerEntry
}

// Common reducers

func countReducerFactory(owner any) ReducerEntry {
	return &counter{owner: owner}
}

type counter struct {
	owner any
	count int64
}

func (c *counter) Add(value any) bool {
	c.count++
	return true
}

func (c *counter) Remove(value any) (changed, empty bool) {
	c.count--
	return true, c.count == 0
}

func (c *counter) Value() any {
	return c.count
}

func (c *counter) CloneIfNotOwned(owner any) ReducerEntry {
	if owner != c.owner {
		return &counter{owner: owner, count: c.count}
	}
	return c
}

func anyValueReducerFactory(owner any) ReducerEntry {
	return &anyValue{owner: owner}
}

type anyValue struct {
	owner any
	value any
	count int64
}

func (s *anyValue) Add(value any) bool {
	changed := false
	if s.value == nil && value != nil {
		s.value = value
		changed = true
	}
	s.count++
	return changed
}

func (s *anyValue) Remove(value any) (changed, empty bool) {
	s.count--
	return s.count == 0, s.count == 0
}

func (s *anyValue) Value() any {
	return s.value
}

func (s *anyValue) CloneIfNotOwned(owner any) ReducerEntry {
	if owner != s.owner {
		return &anyValue{owner: owner, count: s.count, value: s.value}
	}
	return s
}

func setReducerFactory(owner any) ReducerEntry {
	return &setReducer{
		owner:  owner,
		values: make(map[any]int),
	}
}

type setReducer struct {
	owner  any
	values map[any]int
}

func (s *setReducer) Add(value any) bool {
	if count, found := s.values[value]; found {
		s.values[value] = count + 1
		return false
	}
	s.values[value] = 1
	return true
}

func (s *setReducer) Remove(value any) (changed, empty bool) {
	if count, found := s.values[value]; found {
		if count > 1 {
			s.values[value] = count - 1
			return false, false
		}
		delete(s.values, value)
		return true, len(s.values) == 0
	}
	return false, false
}

func (s *setReducer) Value() any {
	ret := sets.New[any]()
	for k := range s.values {
		ret.Insert(k)
	}
	return ret
}

func (s *setReducer) CloneIfNotOwned(owner any) ReducerEntry {
	if owner != s.owner {
		ret := &setReducer{values: map[any]int{}}
		maps.Copy(ret.values, s.values)
		return ret
	}
	return s
}

func sumReducerFactory(owner any) ReducerEntry {
	return &sumReducer{owner: owner}
}

type sumReducer struct {
	owner any
	sum   int
}

func (s *sumReducer) Add(value any) bool {
	s.sum += value.(int)
	return true
}

func (s *sumReducer) Remove(value any) (changed, empty bool) {
	s.sum -= value.(int)
	return true, s.sum == 0
}

func (s *sumReducer) Value() any {
	return s.sum
}

func (s *sumReducer) CloneIfNotOwned(owner any) ReducerEntry {
	if owner != s.owner {
		return &sumReducer{owner: owner, sum: s.sum}
	}
	return s
}
