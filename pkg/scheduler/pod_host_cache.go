package scheduler

/*
 * A cache to store results from pod scheduling that we didn't use for the actual pod. This allows us to use
 * this list for subsequent pods with the same scheduling parameters rather than having to evaluate a whole bunch of
 * nodes again. Note that we revalidate the host before using it, so if some out of band event made it infeasible we
 * will detect this and fall back on a search.
 *
 * We only support pods that are guaranteed to have a single pod per node, and only support pods with "simple" scheduling
 * constraints so that we can safely guarantee that the feasibility and score transfer between pods.
 *
 * As we are caching results, it is possible that things have changed on nodes underneath us (pods dying). However, we already
 * experience this race because we freeze our cache before each scheduler iteration. We use a very short expiration time to
 * ensure we do not expose ourselves to a much larger window.
 *
 * Note that this cache is not thread safe; it is reliant on the scheduler running in a single go routine.
 */

import (
	"fmt"
	"time"

	"container/list"

	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const (
	// How long cached extries remain usable.
	expirationTime = 10 * time.Second

	// Maximum size of the cache
	maxCacheSize = 1000
)

type PodHostCache struct {
	signatures *listCache
	hostnames  *listCache
}

type entry struct {
	signature     string
	hostname      string
	signatureList listCacheItem
	hostList      listCacheItem
	cache         *PodHostCache
}

func NewPodHostCache() *PodHostCache {
	return &PodHostCache{
		signatures: newListCache(removePodHostCacheEntry),
		hostnames:  newListCache(removePodHostCacheEntry),
	}
}

func (c *PodHostCache) AddSignature(signature *framework.PodSignatureResult, sortedHosts []framework.NodePluginScores, t time.Time) {
	if !signature.Signable {
		return
	}

	c.signatures.RemoveListIfExists(signature.Signature)

	for _, host := range sortedHosts {
		e := c.newEntry(signature.Signature, host.Name)
		c.addEntry(e, t)
	}
}

func (c *PodHostCache) SuggestedHost(signature *framework.PodSignatureResult) (string, error) {
	if !signature.Signable {
		return "", fmt.Errorf("pod not signable")
	}

	l, found := c.signatures.entries[signature.Signature]
	if !found {
		return "", fmt.Errorf("no signature list to use")
	}

	ent := l.list.Front()
	if ent == nil {
		return "", fmt.Errorf("signature list is empty")
	}

	return ent.Value.(*listCacheItem).Value.(*entry).hostname, nil
}

func (c *PodHostCache) HostAvailable(signature *framework.PodSignatureResult) bool {
	if !signature.Signable {
		return false
	}
	_, found := c.signatures.entries[signature.Signature]
	return found
}

func (c *PodHostCache) InvalidateHost(hostname string) {
	c.hostnames.RemoveListIfExists(hostname)
}

// Expire cache entries that are stale. Should be called each scheduling iteration before checking for suggested hosts.

func (c *PodHostCache) Evict() {
	c.signatures.Evict(time.Now())
}

func (c *PodHostCache) newEntry(signature, hostname string) *entry {
	e := &entry{
		signature: signature,
		hostname:  hostname,
		cache:     c,
	}
	return e
}

func (c *PodHostCache) addEntry(ent *entry, t time.Time) {
	c.signatures.Add(ent.signature, ent, t, &ent.signatureList)
	c.hostnames.Add(ent.hostname, ent, t, &ent.hostList)
}

func removePodHostCacheEntry(elem any) {
	ent := elem.(*entry)
	ent.cache.signatures.Remove(&ent.signatureList)
	ent.cache.hostnames.Remove(&ent.hostList)
}

type itemRemovedCallback func(any)

func newListCache(removedCallback itemRemovedCallback) *listCache {
	return &listCache{
		entries:         map[string]*listCacheList{},
		evictionList:    list.New(),
		removedCallback: removedCallback,
	}
}

// A cache of lists, indexed by string.
type listCache struct {
	entries map[string]*listCacheList

	// List of entries in the cache by time added, with the oldest item
	// at the head of the list.
	evictionList *list.List

	// Optional callback invoked when an item is removed from a list in the cache.
	removedCallback itemRemovedCallback
}

// An entry in the listCache. This contains a list, the time when it was added
// and an entry that links this into the evictionList for the cache.
type listCacheList struct {
	key          string
	list         *list.List
	timestamp    time.Time
	evictionList *list.Element
	cache        *listCache
}

// An item inside a list in the list cache.
type listCacheItem struct {
	Value any
	elem  *list.Element
	list  *listCacheList
}

// Create a new list in our cache if it doesn't exist, add this entry to the tail of the list
// if such a list already exists.

func (m *listCache) Add(key string, value any, t time.Time, outItem *listCacheItem) {
	l, found := m.entries[key]
	if !found {
		l = &listCacheList{
			key:       key,
			timestamp: t,
			cache:     m,
			list:      list.New(),
		}
		m.entries[key] = l
		l.evictionList = m.evictionList.PushBack(l)
	}

	*outItem = listCacheItem{
		Value: value,
		list:  l,
		elem:  l.list.PushBack(outItem),
	}
}

// Remove item from the list in which it resides, or a no-op if it isn't in a list.

func (m *listCache) Remove(elem *listCacheItem) {
	if elem.list != nil {
		elem.list.list.Remove(elem.elem)
		if elem.list.list.Len() == 0 {
			m.RemoveListIfExists(elem.list.key)
		}
		elem.list = nil
		if m.removedCallback != nil {
			m.removedCallback(elem.Value)
		}
	}
}

// If a list exists in our cache with the given key, remove it.

func (m *listCache) RemoveListIfExists(key string) {
	if l, found := m.entries[key]; found {
		delete(m.entries, key)
		if m.removedCallback != nil {
			elem := l.list.Front()
			for {
				if elem == nil {
					break
				}

				// Grab the next element in case the callback deletes the entry.
				next := elem.Next()

				m.removedCallback(elem.Value.(*listCacheItem).Value)
				elem = next
			}
		}
		m.evictionList.Remove(l.evictionList)
	}
}

// Evict stale list entries given the current time and cache settings.

func (m *listCache) Evict(n time.Time) {
	for {
		t := m.evictionList.Front()
		if t == nil {
			break
		}
		ent := t.Value.(*listCacheList)
		if n.Sub(ent.timestamp) > expirationTime || len(m.entries) > maxCacheSize {
			ent.cache.RemoveListIfExists(ent.key)
		} else {
			break
		}
	}
}
