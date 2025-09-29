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
	signatureLists := map[*listCacheEntry]bool{}
	for sig, l := range c.signatures.entries {
		if !c.HostAvailableSig(sig) {
			t.Fatal("Doesn't show host available for signature in cache")
		}
		signatureLists[l] = true
		lent := l.list.head()
		if lent == nil {
			t.Fatal("Empty list for ", sig)
		}
		hostnamesFound := map[string]bool{}
		for lent != nil {
			ent := lent.entry.(*entry)
			lent = l.list.next(lent)
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
			e := l2.list.head()
			found = false
			for e != nil {
				if e.entry == ent {
					found = true
					break
				}
				e = l2.list.next(e)
			}
			if !found {
				t.Fatal("Didn't find entry in hostname list", ent)
			}
		}
	}

	if len(c.signatures.entries) > 0 {
		l := c.signatures.evictionList.head()
		var lastTime time.Time
		timeSet := false
		entriesFound := 0
		for l != nil {
			ent := l.entry.(*listCacheEntry)
			tm := ent.timestamp
			if timeSet && !(tm.After(lastTime) || tm.Equal(lastTime)) {
				t.Fatal("Timestamps out of order")
			}
			timeSet = true
			lastTime = tm
			l = c.signatures.evictionList.next(l)
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
		lent := l.list.head()
		if lent == nil {
			t.Fatal("Empty list for ", hostname)
		}
		signaturesFound := map[string]bool{}
		for lent != nil {
			ent := lent.entry.(*entry)
			lent = l.list.next(lent)
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
			e := l2.list.head()
			found = false
			for e != nil {
				if e.entry == ent {
					found = true
					break
				}
				e = l2.list.next(e)
			}
			if !found {
				t.Fatal("Didn't find entry in signature list", ent)
			}
		}
	}
}

func TestPodHostCacheBasic(t *testing.T) {
	c := NewPodHostCache()
	err := c.AddSignature("sig", []string{"h1", "h2"}, time.Now())
	if err != nil {
		t.Fatal("Error adding")
	}
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
	err := c.AddSignature("sig", []string{"h1", "h2"}, time.Now())
	if err != nil {
		t.Fatal("Error adding")
	}
	validateCache(t, c)

	err = c.AddSignature("sig2", []string{"h1", "h3"}, time.Now())
	if err != nil {
		t.Fatal("Error adding")
	}
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
	err := c.AddSignature("sig", []string{"h1"}, time.Now())
	if err != nil {
		t.Fatal("Error adding")
	}
	validateCache(t, c)

	err = c.AddSignature("sig2", []string{"h1"}, time.Now())
	if err != nil {
		t.Fatal("Error adding")
	}
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
