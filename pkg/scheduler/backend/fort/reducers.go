package fort

import (
	"maps"

	"k8s.io/apimachinery/pkg/util/sets"
)

// Create a new reducer entry owned by the given object.
type Reducer func() ReducerEntry

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
	Clone() ReducerEntry
}

// Common reducers

func countReducerFactory() ReducerEntry {
	return &counter{}
}

type counter struct {
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

func (c *counter) Clone() ReducerEntry {
	return &counter{count: c.count}
}

func anyValueReducerFactory() ReducerEntry {
	return &anyValue{}
}

type anyValue struct {
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

func (s *anyValue) Clone() ReducerEntry {
	return &anyValue{count: s.count, value: s.value}
}

func setReducerFactory() ReducerEntry {
	return &setReducer{
		values: make(map[any]int),
	}
}

type setReducer struct {
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

func (s *setReducer) Clone() ReducerEntry {
	ret := &setReducer{values: map[any]int{}}
	maps.Copy(ret.values, s.values)
	return ret
}

func sumReducerFactory() ReducerEntry {
	return &sumReducer{}
}

type sumReducer struct {
	sum int
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

func (s *sumReducer) Clone() ReducerEntry {
	return &sumReducer{sum: s.sum}
}

func Map[InnerKeyType comparable]() ReducerEntry {
	return &innerMapReducer[InnerKeyType]{
		values: make(map[InnerKeyType]any),
	}
}

type innerMapReducer[KeyType comparable] struct {
	values map[KeyType]any
}

func (s *innerMapReducer[K]) Add(value any) bool {
	kv := value.(KeyValue[K])
	s.values[kv.Key] = kv.Value
	return true
}

func (s *innerMapReducer[K]) Remove(value any) (changed, empty bool) {
	kv := value.(KeyValue[K])
	delete(s.values, kv.Key)
	return true, len(s.values) == 0
}

func (s *innerMapReducer[K]) Value() any {
	ret := map[K]any{}
	maps.Copy(ret, s.values)
	return ret
}

func (s *innerMapReducer[K]) Clone() ReducerEntry {
	ret := &innerMapReducer[K]{values: map[K]any{}}
	maps.Copy(ret.values, s.values)
	return ret
}
