package scheduler

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/mohae/deepcopy"
)

// Cosistency check the cache. Ensure that entries show up in the right
// signature and hostname lists exactly once. Ensure we don't have empty lists.
// Check that the eviction list is in ascending time order and matches the signature
// map.
func validateCache(t *testing.T, c *PodHostCache) {
	signatureLists := map[*listCacheList]bool{}
	for sig, l := range c.signatures.entries {
		if !c.HostAvailableSig(sig) {
			t.Fatal("Doesn't show host available for signature in cache")
		}
		signatureLists[l] = true
		lent := l.list.Front()
		if lent == nil {
			t.Fatal("Empty list for ", sig)
		}
		hostnamesFound := map[string]bool{}
		for lent != nil {
			ent := lent.Value.(*listCacheItem).Value.(*entry)
			lent = lent.Next()
			if sig != ent.signature {
				t.Fatal("Signature in entry doesn't match list", sig, ent)
			}
			if _, found := hostnamesFound[ent.hostname]; found {
				t.Fatal("Duplicate of hostname found", ent)
			}
			hostnamesFound[ent.hostname] = true
			l2, found := c.hostnames.entries[ent.hostname]
			if !found {
				t.Fatal("Hostname not found in hostname map", ent)
			}
			e := l2.list.Front()
			found = false
			for e != nil {
				if e.Value.(*listCacheItem).Value == ent {
					found = true
					break
				}
				e = e.Next()
			}
			if !found {
				t.Fatal("Didn't find entry in hostname list", ent)
			}
		}
	}

	if len(c.signatures.entries) > 0 {
		l := c.signatures.evictionList.Front()
		var lastTime time.Time
		timeSet := false
		entriesFound := 0
		for l != nil {
			ent := l.Value.(*listCacheList)
			tm := ent.timestamp
			if timeSet && !(tm.After(lastTime) || tm.Equal(lastTime)) {
				t.Fatal("Timestamps out of order")
			}
			timeSet = true
			lastTime = tm
			l = l.Next()
			entriesFound += 1

			// Check list against map using the pigeonhole principle.
			if _, found := signatureLists[ent]; !found {
				t.Fatal("Signature entry in eviction list not in map")
			}
		}
		if entriesFound != len(c.signatures.entries) {
			t.Fatal("Eviction list length doesn't match sig map size")
		}
	}

	for hostname, l := range c.hostnames.entries {
		lent := l.list.Front()
		if lent == nil {
			t.Fatal("Empty list for ", hostname)
		}
		signaturesFound := map[string]bool{}
		for lent != nil {
			ent := lent.Value.(*listCacheItem).Value.(*entry)
			lent = lent.Next()
			if hostname != ent.hostname {
				t.Fatal("Hostname in entry doesn't match list", hostname, ent)
			}
			if _, found := signaturesFound[ent.signature]; found {
				t.Fatal("Duplicate of signature found", ent)
			}
			signaturesFound[ent.signature] = true
			l2, found := c.signatures.entries[ent.signature]
			if !found {
				t.Fatal("Signature not found in signature map", ent)
			}
			e := l2.list.Front()
			found = false
			for e != nil {
				if e.Value.(*listCacheItem).Value == ent {
					found = true
					break
				}
				e = e.Next()
			}
			if !found {
				t.Fatal("Didn't find entry in signature list", ent)
			}
		}
	}
}

func TestPodHostCacheBasic(t *testing.T) {
	c := NewPodHostCache()
	c.AddSignature("sig", []string{"h1", "h2"}, time.Now())
	validateCache(t, c)

	host, err := c.SuggestedHostSig("sig")
	if err != nil {
		t.Fatal("Error using", err)
	}
	if host != "h1" {
		t.Fatal(host)
	}
	c.InvalidateHost(host)
	validateCache(t, c)

	host, err = c.SuggestedHostSig("sig")
	if err != nil {
		t.Fatal("Error using", err)
	}
	if host != "h2" {
		t.Fatal(host)
	}
	c.InvalidateHost(host)
	validateCache(t, c)
}

func TestPodHostCacheDouble(t *testing.T) {
	c := NewPodHostCache()
	c.AddSignature("sig", []string{"h1", "h2"}, time.Now())
	validateCache(t, c)

	c.AddSignature("sig2", []string{"h1", "h3"}, time.Now())
	validateCache(t, c)

	if !c.HostAvailableSig("sig") {
		t.Fatal("Expected host")
	}
	host, err := c.SuggestedHostSig("sig")
	if err != nil {
		t.Fatal("Error using", err)
	}
	if host != "h1" {
		t.Fatal(host)
	}
	c.InvalidateHost(host)
	validateCache(t, c)

	if !c.HostAvailableSig("sig2") {
		t.Fatal("Expected host")
	}
	host, err = c.SuggestedHostSig("sig2")
	if err != nil {
		t.Fatal("Error using", err)
	}
	if host != "h3" {
		t.Fatal(host)
	}
	c.InvalidateHost(host)
	validateCache(t, c)

	if !c.HostAvailableSig("sig") {
		t.Fatal("Expected host")
	}
	host, err = c.SuggestedHostSig("sig")
	if err != nil {
		t.Fatal("Error using", err)
	}
	if host != "h2" {
		t.Fatal(host)
	}
	c.InvalidateHost(host)
	validateCache(t, c)
}

func TestPodHostCacheRemoveEmpty(t *testing.T) {
	c := NewPodHostCache()
	c.AddSignature("sig", []string{"h1"}, time.Now())
	validateCache(t, c)

	c.AddSignature("sig2", []string{"h1"}, time.Now())
	validateCache(t, c)

	if !c.HostAvailableSig("sig") {
		t.Fatal("Expected host")
	}
	host, err := c.SuggestedHostSig("sig")
	if err != nil {
		t.Fatal("Error using", err)
	}
	if host != "h1" {
		t.Fatal(host)
	}
	c.InvalidateHost(host)
	validateCache(t, c)

	if c.HostAvailableSig("sig") {
		t.Fatal("Expected no hosts")
	}
	_, err = c.SuggestedHostSig("sig")
	if err == nil {
		t.Fatal("Expected error in getting empty")
	}

	if c.HostAvailableSig("sig2") {
		t.Fatal("Expected no hosts")
	}
	_, err = c.SuggestedHostSig("sig2")
	if err == nil {
		t.Fatal("Expected error iun getting empty")
	}
	validateCache(t, c)
}

type SimplePodHostCache struct {
	values [][]bool
	hosts  map[string]bool
}

func newSimpleCache(sigs, hosts, prob int) *SimplePodHostCache {
	c := &SimplePodHostCache{values: [][]bool{}, hosts: map[string]bool{}}
	for i := range sigs {
		c.values = append(c.values, make([]bool, hosts))
		for j := range hosts {
			c.values[i][j] = (rand.Int() % prob) == 0
			if c.values[i][j] {
				c.hosts[fmt.Sprintf("h%d", j)] = true
			}
		}
	}
	return c
}

func (s *SimplePodHostCache) Get(sig int) []string {
	hosts := []string{}
	for i := 0; i < len(s.values[sig]); i++ {
		if s.values[sig][i] {
			hosts = append(hosts, fmt.Sprintf("h%d", i))
		}
	}
	return hosts
}

func testFuzz(t *testing.T, s *SimplePodHostCache) {
	c := NewPodHostCache()
	tm := time.Now()

	for i := range s.values {
		c.AddSignature(fmt.Sprintf("s%d", i), s.Get(i), tm)
	}

	hostsRemaining := deepcopy.Copy(s.hosts).(map[string]bool)
	for {
		sig := rand.Int() % len(s.values)
		sigName := fmt.Sprintf("s%d", sig)
		if c.HostAvailableSig(sigName) {
			h, err := c.SuggestedHostSig(sigName)
			if err != nil {
				t.Fatal(err)
			}
			if _, found := hostsRemaining[h]; !found {
				if _, found := s.hosts[h]; found {
					t.Fatalf("Found host %s twice", h)
				} else {
					t.Fatalf("Found host %s that shouldn't exist", h)
				}
			}
			hostNum := -1
			fmt.Sscanf(h, "h%d", &hostNum)
			if !s.values[sig][hostNum] {
				t.Fatalf("Found host %s in sig %s which shouldn't have it", h, sigName)
			}
			delete(hostsRemaining, h)
			c.InvalidateHost(h)
			validateCache(t, c)
		} else {
			for j := range len(s.values[sig]) {
				if s.values[sig][j] {
					if _, found := hostsRemaining[fmt.Sprintf("h%d", j)]; found {
						t.Fatalf("Didn't find host for sig %s when valid host %d exists", sigName, j)
					}
				}
			}
		}

		if len(hostsRemaining) == 0 {
			return
		}
	}
}

func TestPodHostCacheFuzz(t *testing.T) {
	sigs := 100
	hosts := 200
	prob := 5

	for i := range 100 {
		if i != 0 {
		}
		s := newSimpleCache(sigs, hosts, prob)
		testFuzz(t, s)
	}
}

func TestPodHostCacheEviction(t *testing.T) {
	// Set up a cache where entries expire quickly
	c := NewPodHostCache()

	// Add entry 1 at T0
	t0 := time.Now()
	c.AddSignature("sig-expired", []string{"h1"}, t0)

	c.AddSignature("sig-active", []string{"h2"}, t0.Add(expirationTime+2*time.Second))

	validateCache(t, c)

	// Fast-forward time (simulated): Expire "sig-expired" (t0 + 1s)
	t_expired := t0.Add(expirationTime + 1*time.Second)

	// Set the current time for the Evict call
	// NOTE: The real implementation needs a mechanism to inject time.
	// We'll call Evict with a time past the expiration, assuming the cache uses it.

	// Force eviction of stale entries
	c.signatures.Evict(t_expired)
	c.hostnames.Evict(t_expired)

	validateCache(t, c)

	// Check results
	if c.HostAvailableSig("sig-expired") {
		t.Errorf("Expected 'sig-expired' to be evicted, but it is still available")
	}

	if !c.HostAvailableSig("sig-active") {
		t.Errorf("Expected 'sig-active' to still be available, but it was evicted")
	}
}

func TestPodHostCacheSizeLimitEviction(t *testing.T) {
	// Set up cache using a small size limit
	c := NewPodHostCache()
	// NOTE: In a robust test environment, we would inject this limit into NewPodHostCache.
	// For this unit test, we'll rely on direct manipulation/validation based on hardcoded maxCacheSize if we can't inject.

	// We will simulate the size limit being 10, meaning we add 11 entries to ensure eviction.

	// Add entries 0 through 9 (10 total) at T0
	t0 := time.Now()
	for i := 0; i < maxCacheSize; i++ {
		sig := fmt.Sprintf("s%d", i)
		host := fmt.Sprintf("h%d", i)
		// Add each signature slightly later to ensure chronological order in the eviction list
		c.AddSignature(sig, []string{host}, t0.Add(time.Duration(i)*time.Millisecond))
	}

	// At this point, the cache has maxCacheSize entries.
	if len(c.signatures.entries) != maxCacheSize {
		t.Fatalf("Expected cache size %d, got %d before overflow", maxCacheSize, len(c.signatures.entries))
	}

	// Add the 11th entry at T1 (this should trigger eviction of the oldest, s0)
	sigOverflow := "s-overflow"
	hostOverflow := "h-overflow"
	c.AddSignature(sigOverflow, []string{hostOverflow}, t0.Add(1*time.Second))

	// Trigger eviction based on size limit
	c.signatures.Evict(t0)

	validateCache(t, c)

	// After eviction, the size should be exactly the limit
	if len(c.signatures.entries) != maxCacheSize {
		t.Fatalf("Expected cache size to be exactly %d after overflow, got %d", maxCacheSize, len(c.signatures.entries))
	}

	// Check if the oldest entry (s0) was evicted
	if c.HostAvailableSig("s0") {
		t.Errorf("Expected oldest entry 's0' to be evicted, but it is still available")
	}

	// Check if the newest entry (s-overflow) is present
	if !c.HostAvailableSig(sigOverflow) {
		t.Errorf("Expected newest entry 's-overflow' to be present, but it was evicted")
	}
}
