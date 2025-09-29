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
 */

import (
	"crypto/md5"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const (
	// Is the cache enabled?
	cacheEnabled = true

	// How long cached extries remain usable.
	expirationTime = 10 * time.Second

	// Maximum size of the cache
	maxCacheSize = 1000
)

type PodHostCache struct {
	signatures *listCache
	hostnames  *listCache
}

type listCache struct {
	entries      map[string]*listCacheEntry
	evictionList *listEntry
}

func newListCache() *listCache {
	return &listCache{
		entries:      map[string]*listCacheEntry{},
		evictionList: newList(),
	}
}

type listCacheEntry struct {
	list         *listEntry
	timestamp    time.Time
	evictionList listEntry
}

// If a list exists in our cache with the given key, remove it.

func (m *listCache) removeListIfExists(key string) error {
	if l, found := m.entries[key]; found {
		delete(m.entries, key)
		l.evictionList.remove()

		// Keep removing the head of the list until it is empty.
		for {
			lent, err := l.list.head()
			if err != nil {
				return err
			}
			if lent == nil {
				return nil
			}
			ent := lent.entry.(*entry)
			err = ent.removeFromLists()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Create a new list in our cache if it doesn't exist, add this entry to the tail of the list
// if such a list already exists.

func (m *listCache) addOrPushToList(lent *listEntry, key string, t time.Time) {
	l, found := m.entries[key]
	if !found {
		l = &listCacheEntry{
			list:      newList(),
			timestamp: t,
		}
		l.evictionList.entry = l
		m.entries[key] = l
		m.evictionList.pushTail(&l.evictionList)
	}

	l.list.pushTail(lent)
}

// Evict stale list entries given the current time and cache settings.

func (m *listCache) evict(n time.Time) error {
	for {
		t, err := m.evictionList.head()
		if t == nil {
			return nil
		}
		if err != nil {
			return err
		}
		if n.Sub(t.entry.(*listCacheEntry).timestamp) > expirationTime || len(m.entries) > maxCacheSize {
			t.remove()
		} else {
			break
		}
	}
	return nil
}

type entry struct {
	signature     string
	hostname      string
	signatureList listEntry
	hostList      listEntry
	cache         *PodHostCache
}

func NewPodHostCache() *PodHostCache {
	return &PodHostCache{
		signatures: newListCache(),
		hostnames:  newListCache(),
	}
}

func (c *PodHostCache) AddPod(pod *v1.Pod, sortedHosts []framework.NodePluginScores) error {
	if !podCachingCandidate(pod) || !cacheEnabled {
		return nil
	}

	sig := podSchedulingSignature(pod)
	sortedHostnames := make([]string, len(sortedHosts))
	for i := range sortedHosts {
		sortedHostnames[i] = sortedHosts[i].Name
	}
	return c.AddSignature(sig, sortedHostnames, time.Now())
}

func (c *PodHostCache) AddSignature(signature string, sortedHostnames []string, t time.Time) error {
	err := c.signatures.removeListIfExists(signature)
	if err != nil {
		return err
	}
	for _, hostname := range sortedHostnames {
		e := c.newEntry(signature, hostname)
		c.addEntry(e, t)
	}
	return nil
}

func (c *PodHostCache) SuggestedHost(pod *v1.Pod) (string, error) {
	if podCachingCandidate(pod) && cacheEnabled {
		sig := podSchedulingSignature(pod)
		return c.SuggestedHostSig(sig)
	}
	return "", fmt.Errorf("pod not candidate for caching or cache disabled")
}

func (c *PodHostCache) SuggestedHostSig(signature string) (string, error) {
	l, found := c.signatures.entries[signature]
	if !found {
		return "", fmt.Errorf("no signature list to use")
	}

	ent, err := l.list.head()
	if err != nil {
		return "", err
	}
	if ent == nil {
		return "", fmt.Errorf("signature list is empty")
	}

	return ent.entry.(*entry).hostname, nil
}

func (c *PodHostCache) HostAvailable(pod *v1.Pod) bool {
	if podCachingCandidate(pod) && cacheEnabled {
		sig := podSchedulingSignature(pod)
		return c.HostAvailableSig(sig)
	}
	return false
}

func (c *PodHostCache) HostAvailableSig(signature string) bool {
	_, found := c.signatures.entries[signature]
	return found
}

func (c *PodHostCache) InvalidateHost(hostname string) error {
	if cacheEnabled {
		return c.hostnames.removeListIfExists(hostname)
	}
	return nil
}

// Expire cache entries that are stale. Should be called each scheduling iteration before checking for suggested hosts.

func (c *PodHostCache) Evict() error {
	// This should only be nil in tests.
	if c.signatures != nil {
		return c.signatures.evict(time.Now())
	}
	return nil
}

func (c *PodHostCache) newEntry(signature, hostname string) *entry {
	e := &entry{
		signature: signature,
		hostname:  hostname,
		cache:     c,
	}
	e.signatureList.entry = e
	e.hostList.entry = e
	return e
}

func (c *PodHostCache) addEntry(ent *entry, t time.Time) {
	c.signatures.addOrPushToList(&ent.signatureList, ent.signature, t)
	c.hostnames.addOrPushToList(&ent.hostList, ent.hostname, t)
}

func (ent *entry) removeFromLists() error {
	empty, err := ent.signatureList.remove()
	if err != nil {
		return err
	}
	if empty {
		ent.cache.signatures.removeListIfExists(ent.signature)
	}
	empty, err = ent.hostList.remove()
	if err != nil {
		return err
	}
	if empty {
		ent.cache.hostnames.removeListIfExists(ent.hostname)
	}
	return nil
}

// An intrusive doubly linked list, implemented as a circular list with the first entry having "nil" as the entry value.

type listEntry struct {
	entry any
	prev  *listEntry
	next  *listEntry
}

func newList() *listEntry {
	n := &listEntry{}
	n.prev = n
	n.next = n
	return n
}

func (l *listEntry) remove() (bool, error) {
	if l.entry == nil {
		return false, fmt.Errorf("entry is a list head and can't be removed")
	}

	l.prev.next = l.next
	l.next.prev = l.prev

	emptyList := (l.prev == l.next)

	l.next = nil
	l.prev = nil

	return emptyList, nil
}

func (l *listEntry) pushTail(ent *listEntry) error {
	if l.entry != nil {
		return fmt.Errorf("trying to push onto non list head")
	}
	if ent.entry == nil {
		return fmt.Errorf("trying to push head entry onto list")
	}

	ent.next = l
	ent.prev = l.prev
	l.prev.next = ent
	l.prev = ent

	return nil
}

func (l *listEntry) head() (*listEntry, error) {
	if l.entry != nil {
		return nil, fmt.Errorf("trying to get head from non list")
	}

	// Empty list
	if l.next == l {
		return nil, nil
	}
	return l.next, nil
}

// Check if a pod is a candidate for caching.

func podCachingCandidate(p *v1.Pod) bool {
	return onePodPerNode(p) &&
		// Pods with topology spread constraints are not cacheable.
		len(p.Spec.TopologySpreadConstraints) == 0 &&

		// For now ignore pods with resource claims
		p.Spec.ResourceClaims == nil &&

		// Pods with pod affinity or anti-affinity are not cacheable.
		(p.Spec.Affinity == nil || (p.Spec.Affinity.PodAffinity == nil && p.Spec.Affinity.PodAntiAffinity == nil))
}

// Determine if a pod is a single pod per node. For now only consider pods with a fixed host port reservation.

func onePodPerNode(p *v1.Pod) bool {
	for _, container := range p.Spec.Containers {
		for _, port := range container.Ports {
			if port.HostPort > 0 {
				return true
			}
		}
	}
	return false
}

// Compute a signature for a pod from its scheduling settings.

func podSchedulingSignature(p *v1.Pod) string {
	// Need to find something better than md5
	h := md5.New()
	enc := gob.NewEncoder(h)

	enc.Encode(p.Namespace)
	enc.Encode(p.Spec.SchedulerName)
	if p.Spec.Priority != nil {
		enc.Encode(*p.Spec.Priority)
	}
	enc.Encode(p.Spec.Tolerations)
	if p.Spec.Affinity != nil && p.Spec.Affinity.NodeAffinity != nil {
		enc.Encode(p.Spec.Affinity.NodeAffinity)
	}
	enc.Encode(p.Spec.NodeSelector)
	if p.Spec.RuntimeClassName != nil {
		enc.Encode(p.Spec.RuntimeClassName)
	}

	addContainersToHash(p.Spec.InitContainers, enc)
	addContainersToHash(p.Spec.Containers, enc)

	addVolumesToHash(p.Spec.Volumes, enc)

	return hex.Dump(h.Sum(nil))
}

func addContainersToHash(containers []v1.Container, enc *gob.Encoder) {
	for _, container := range containers {
		enc.Encode(container.Ports)
		enc.Encode(container.Resources)
	}
}

func addVolumesToHash(volumes []v1.Volume, enc *gob.Encoder) {
	for _, vol := range volumes {
		if vol.ConfigMap == nil && vol.Secret == nil {
			enc.Encode(vol)
		}
	}
}
