package fort

import (
	"fmt"
	"runtime"
	"sync"
)

type CloneMap struct {
	lock       sync.RWMutex
	references int64
	data       map[string]any
	base       *CloneMap
	root       any
	targets    []keyValueTarget
}

var _ KeyValueMap = &CloneMap{}

func newCloneMap(data map[string]any, base *CloneMap, root any, references int64) *CloneMap {
	newMap := &CloneMap{
		references: references,
		data:       data,
		base:       base,
		root:       root,
	}

	runtime.SetFinalizer(newMap, cloneMapFinalizer)

	return newMap
}

func Get[T any](m *CloneMap, key string) T {
	v, found := m.Get(key)
	if found && v != nil {
		return v.(T)
	}
	var empty T
	return empty
}

func (m *CloneMap) Get(key string) (any, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.getLocked(key)
}

func (m *CloneMap) Has(key string) bool {
	_, found := m.Get(key)
	return found
}

func (m *CloneMap) getLocked(key string) (any, bool) {
	v, found := m.data[key]
	if !found && m.base != nil {
		v, found = m.base.Get(key)
	}

	return v, found
}

func (m *CloneMap) All() KeyValueIterator {
	return func(yield func(key string, value any) bool) {
		m.lock.RLock()
		defer m.lock.RUnlock()

		for key, value := range m.data {
			if !yield(key, value) {
				return
			}
		}
		if m.base != nil {
			for key, value := range m.base.All() {
				if _, found := m.data[key]; !found {
					if !yield(key, value) {
						return
					}
				}
			}
		}
	}
}

func (m *CloneMap) Update(key string, value any) {
	if m.updateNoCallback(key, value) {
		m.callOnUpdate(&KeyValue{Key: key, Value: value})
	}
}

func (m *CloneMap) updateNoCallback(key string, value any) bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	if value != m.data[key] {
		m.data[key] = value
		return true
	}

	return false
}

var tombstone = &struct{}{}

func (m *CloneMap) Delete(key string) {
	kv := KeyValue{}
	if m.deleteNoCallback(key, &kv) {
		fmt.Printf("Deleted %s, %v\n", kv.Key, kv.Value)
		m.callOnDelete(&kv)
	}
}

func (m *CloneMap) deleteNoCallback(key string, callback *KeyValue) bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	val, found := m.getLocked(key)
	if found {
		if m.base == nil {
			delete(m.data, key)
		} else {
			m.data[key] = tombstone
		}

		*callback = KeyValue{Key: key, Value: val}
		return true
	}

	return false
}

func (m *CloneMap) Clone(root any) Cloneable {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.root != root {
		newBase := newCloneMap(m.data, m.base, m.root, 2)

		m.data = map[string]any{}
		m.base = newBase
		m.root = root

		return newCloneMap(map[string]any{}, newBase, root, 0)
	}

	return m
}

// Merging logic. We keep refernce counts for each map in the chain.
// Whan a map is finalized decrease the ref count on its base.
func cloneMapFinalizer(m *CloneMap) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if m.base != nil {
		m.base.removeRef()
	}
}

// Down the ref count on the map
// Use this as an opportunity to check for
// merging opportunites.
func (m *CloneMap) removeRef() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.references--

	// Check if we can merge into our base.
	// Note that this is a property of our
	// base, not ourself, but the removal of a
	// reference is a good time to check the previous
	// tree.
	m.mergeBaseIfPossible()
}

// Assumes that the map lock is held.
func (m *CloneMap) mergeBaseIfPossible() {
	if m.base != nil {
		m.base.lock.Lock()
		defer m.base.lock.Unlock()

		m.base.mergeBaseIfPossible()
		if m.base.references == 1 {
			// Update our base's map with our data.
			// Do this rather than the inverse because we
			// assume the base is larger.
			updateLockedMaps(m.base.data, m.data)

			// Move the base's map into our object
			// and drop our reference to the base to
			// allow it to be garbage collected.
			m.data = m.base.data
			m.base.references = 0
			m.base = nil
		}
	}
}

func updateLockedMaps(dest map[string]any, src map[string]any) {
	for key, value := range src {
		if value == tombstone {
			delete(dest, key)
		} else {
			dest[key] = value
		}
	}
}

func (m *CloneMap) addTarget(target keyValueTarget) {
	m.targets = append(m.targets, target)
}

func (m *CloneMap) callOnUpdate(kv *KeyValue) {
	for _, target := range m.targets {
		target.onUpdate(kv, m)
	}
}

func (m *CloneMap) callOnDelete(kv *KeyValue) {
	for _, target := range m.targets {
		target.onDelete(kv, m)
	}
}

func (m *CloneMap) Print() {
	m.lock.RLock()
	defer m.lock.RUnlock()
	fmt.Print("  CloneMap{\n")
	for key, value := range m.All() {
		fmt.Printf("    %s: %v\n", key, value)
	}
	fmt.Print("  }\n")
}
