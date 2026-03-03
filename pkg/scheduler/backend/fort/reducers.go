package fort

import (
	"maps"

	"golang.org/x/exp/constraints"
	"k8s.io/apimachinery/pkg/util/sets"
)

// Create a new reducer entry owned by the given object.
type Reducer[I, O any] func() ReducerEntry[I, O]

// An individual reducer entry.
type ReducerEntry[I, O any] interface {
	// Add a value to the reducer. The result
	// should be true if the Value() result has changed
	// after the add, false if it has not.
	Add(value I) bool

	// Remove a value from the reducer.
	//
	// The "changed" result should be true if the Value()
	// result has changed after the removal, false if it has not.
	//
	// The "empty" result should be true if the given reducer entry
	// is now empty and can be removed from our maps, false otherwise.
	Remove(value I) (changed, empty bool)

	// Return the current value of the reducer.
	// Note that while the internal state of the reducer is mutable
	// (Fort manages the state using CloneIfNotOwneed),
	// the values returned by Value() must be immmutable,
	// as with other values pased into the system.
	Value() O

	// If the reducer value is owned by the given
	// owner then return the current entry. If
	// it is owned by some other value, then
	// return a new copy of the reducer that is now
	// owned by the provided owner.
	Clone() ReducerEntry[I, O]
}

// Common reducers

type counter[I any] struct {
	count int64
}

func (c *counter[I]) Add(value I) bool {
	c.count++
	return true
}

func (c *counter[I]) Remove(value I) (changed, empty bool) {
	c.count--
	return true, c.count == 0
}

func (c *counter[I]) Value() int64 {
	return c.count
}

func (c *counter[I]) Clone() ReducerEntry[I, int64] {
	return &counter[I]{count: c.count}
}

type anyValue[T comparable] struct {
	value T
	count int64
}

func (s *anyValue[T]) Add(value T) bool {
	changed := false
	var empty T
	if s.value == empty && value != empty {
		s.value = value
		changed = true
	}
	s.count++
	return changed
}

func (s *anyValue[T]) Remove(value T) (changed, empty bool) {
	s.count--
	return s.count == 0, s.count == 0
}

func (s *anyValue[T]) Value() T {
	return s.value
}

func (s *anyValue[T]) Clone() ReducerEntry[T, T] {
	return &anyValue[T]{count: s.count, value: s.value}
}

type setReducer[T comparable] struct {
	values map[T]int
}

func (s *setReducer[T]) Add(value T) bool {
	if count, found := s.values[value]; found {
		s.values[value] = count + 1
		return false
	}
	s.values[value] = 1
	return true
}

func (s *setReducer[T]) Remove(value T) (changed, empty bool) {
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

func (s *setReducer[T]) Value() sets.Set[T] {
	ret := sets.New[T]()
	for k := range s.values {
		ret.Insert(k)
	}
	return ret
}

func (s *setReducer[T]) Clone() ReducerEntry[T, sets.Set[T]] {
	ret := &setReducer[T]{values: map[T]int{}}
	maps.Copy(ret.values, s.values)
	return ret
}

type sumReducer[T constraints.Integer] struct {
	sum T
}

func (s *sumReducer[T]) Add(value T) bool {
	s.sum += value
	return true
}

func (s *sumReducer[T]) Remove(value T) (changed, empty bool) {
	s.sum -= value
	return true, s.sum == 0
}

func (s *sumReducer[T]) Value() T {
	return s.sum
}

func (s *sumReducer[T]) Clone() ReducerEntry[T, T] {
	return &sumReducer[T]{sum: s.sum}
}

func MapFromKeyValues[K, V comparable]() ReducerEntry[KeyValue[K, V], map[K]V] {
	return &innerMapReducer[K, V]{
		values: map[K]innerMapReducerEnt[V]{},
	}
}

type innerMapReducer[K, V comparable] struct {
	values map[K]innerMapReducerEnt[V]
}

type innerMapReducerEnt[V comparable] struct {
	Value V
	Count int64
}

func (s *innerMapReducer[K, V]) Add(value KeyValue[K, V]) bool {
	if existing, found := s.values[value.Key]; found {
		existing.Count++
		if existing.Value != value.Value {
			existing.Value = value.Value
			return true
		}
		return false
	}

	s.values[value.Key] = innerMapReducerEnt[V]{
		Value: value.Value,
		Count: 1,
	}
	return true
}

func (s *innerMapReducer[K, V]) Remove(value KeyValue[K, V]) (changed, empty bool) {
	if existing, found := s.values[value.Key]; found {
		if existing.Count > 1 {
			existing.Count--
			return false, false
		}
		delete(s.values, value.Key)
		return true, len(s.values) == 0
	}
	return false, false
}

func (s *innerMapReducer[K, V]) Value() map[K]V {
	ret := map[K]V{}
	for key, ent := range s.values {
		ret[key] = ent.Value
	}
	return ret
}

func (s *innerMapReducer[K, V]) Clone() ReducerEntry[KeyValue[K, V], map[K]V] {
	newEnt := &innerMapReducer[K, V]{
		values: map[K]innerMapReducerEnt[V]{},
	}
	for key, ent := range s.values {
		newEnt.values[key] = innerMapReducerEnt[V]{
			Value: ent.Value,
			Count: ent.Count,
		}
	}
	return newEnt
}
