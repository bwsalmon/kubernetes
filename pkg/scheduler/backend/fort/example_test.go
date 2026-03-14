package fort

import (
	"reflect"
	"sort"
	"testing"

	"k8s.io/client-go/tools/cache"
)

func TestPodSpreadLiteInfo(t *testing.T) {
	lock := NewLockGroup()
	podSource := NewManualSharedInformerWithOptions(lock, DefaultKeyFunc)
	serviceSource := NewManualSharedInformerWithOptions(lock, DefaultKeyFunc)
	nodeSource := NewManualSharedInformerWithOptions(lock, DefaultKeyFunc)

	ps := NewPodSpreadLiteInfo(podSource, serviceSource, nodeSource)

	// Results tracker
	var latestResults map[string]*TServiceInfo = make(map[string]*TServiceInfo)
	ps.ServiceInfo.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			si := obj.(*TServiceInfo)
			latestResults[si.Service] = si
		},
		UpdateFunc: func(old, new any) {
			si := new.(*TServiceInfo)
			latestResults[si.Service] = si
		},
		DeleteFunc: func(obj any) {
			si := obj.(*TServiceInfo)
			delete(latestResults, si.Service)
		},
	})

	// 1. Setup Infrastructure
	// Node 1 in Zone A
	// Node 2 in Zone B
	// Node 3 in Zone A
	nodeSource.OnAdd(&TNode{Name: "n1", Domains: []string{"zone-a"}}, true)
	nodeSource.OnAdd(&TNode{Name: "n2", Domains: []string{"zone-b"}}, true)
	nodeSource.OnAdd(&TNode{Name: "n3", Domains: []string{"zone-a"}}, true)

	// Service S1 (data 'A') matches pods with label starting with 'A'
	serviceSource.OnAdd(&TService{Name: "s1", SomeData: "Alpha"}, true)

	// 2. Add Pods
	// Pod 1 on Node 1, Label 'Apple' (matches s1)
	// Pod 2 on Node 2, Label 'Ant' (matches s1)
	// Pod 3 on Node 3, Label 'Banana' (does NOT match s1)
	podSource.OnAdd(&TPod{NodeName: "n1", Label: "Apple"}, true)
	podSource.OnAdd(&TPod{NodeName: "n2", Label: "Ant"}, true)
	podSource.OnAdd(&TPod{NodeName: "n3", Label: "Banana"}, true)

	// Expectation for s1:
	// zone-a: 1 pod (on n1)
	// zone-b: 1 pod (on n2)
	validate := func(results map[string]*TServiceInfo, serviceName string, expected map[string]int64) {
		si, ok := results[serviceName]
		if !ok {
			t.Fatalf("Service %s not found in results", serviceName)
		}
		if len(si.DomainCounts) != len(expected) {
			t.Errorf("Expected %d domains, got %d", len(expected), len(si.DomainCounts))
		}
		for _, dc := range si.DomainCounts {
			exp, ok := expected[dc.Domain]
			if !ok {
				t.Errorf("Unexpected domain %s in results", dc.Domain)
				continue
			}
			if dc.Count != exp {
				t.Errorf("Domain %s: expected count %d, got %d", dc.Domain, exp, dc.Count)
			}
		}
	}

	expectedS1 := map[string]int64{"zone-a": 1, "zone-b": 1}
	validate(latestResults, "s1", expectedS1)

	// 3. Update Pod 3 to match s1 (Banana -> Avocado)
	podSource.OnUpdate(&TPod{NodeName: "n3", Label: "Banana"}, &TPod{NodeName: "n3", Label: "Avocado"})
	// Now zone-a should have 2 pods (n1 and n3)
	validate(latestResults, "s1", map[string]int64{"zone-a": 2, "zone-b": 1})

	// 4. Test Cloning
	t.Log("Testing Clone...")
	psClone := ps.Clone()
	
	var cloneResults map[string]*TServiceInfo = make(map[string]*TServiceInfo)
	psClone.ServiceInfo.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			si := obj.(*TServiceInfo)
			cloneResults[si.Service] = si
		},
		UpdateFunc: func(old, new any) {
			si := new.(*TServiceInfo)
			cloneResults[si.Service] = si
		},
	})

	// Initial clone state should match current state
	validate(cloneResults, "s1", map[string]int64{"zone-a": 2, "zone-b": 1})

	// Add new pod to original
	podSource.OnAdd(&TPod{NodeName: "n1", Label: "Apricot"}, false)
	// Original state: zone-a: 3, zone-b: 1
	validate(latestResults, "s1", map[string]int64{"zone-a": 3, "zone-b": 1})
	// Clone state should still be: zone-a: 2, zone-b: 1
	validate(cloneResults, "s1", map[string]int64{"zone-a": 2, "zone-b": 1})

	// Update clone independent of original
	psClone.PodUpdates.OnAdd(&TPod{NodeName: "n2", Label: "Axe"}, false)
	// Clone state: zone-a: 2, zone-b: 2
	validate(cloneResults, "s1", map[string]int64{"zone-a": 2, "zone-b": 2})
	// Original state should still be: zone-a: 3, zone-b: 1
	validate(latestResults, "s1", map[string]int64{"zone-a": 3, "zone-b": 1})
}

func TestPodSpreadLiteInfo_MultiDomainNode(t *testing.T) {
	lock := NewLockGroup()
	podSource := NewManualSharedInformerWithOptions(lock, DefaultKeyFunc)
	serviceSource := NewManualSharedInformerWithOptions(lock, DefaultKeyFunc)
	nodeSource := NewManualSharedInformerWithOptions(lock, DefaultKeyFunc)

	ps := NewPodSpreadLiteInfo(podSource, serviceSource, nodeSource)

	var latest *TServiceInfo
	ps.ServiceInfo.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) { latest = obj.(*TServiceInfo) },
		UpdateFunc: func(old, new any) { latest = new.(*TServiceInfo) },
	})

	// Node in two domains (e.g. Zone and Rack)
	nodeSource.OnAdd(&TNode{Name: "n1", Domains: []string{"zone-a", "rack-1"}}, true)
	serviceSource.OnAdd(&TService{Name: "s1", SomeData: "A"}, true)
	podSource.OnAdd(&TPod{NodeName: "n1", Label: "Apple"}, true)

	if latest == nil || len(latest.DomainCounts) != 2 {
		t.Fatalf("Expected 2 domains, got %v", latest)
	}

	// Sort domain counts for consistent comparison
	sort.Slice(latest.DomainCounts, func(i, j int) bool {
		return latest.DomainCounts[i].Domain < latest.DomainCounts[j].Domain
	})

	expected := []DomainCount{
		{Domain: "rack-1", Count: 1},
		{Domain: "zone-a", Count: 1},
	}
	if !reflect.DeepEqual(latest.DomainCounts, expected) {
		t.Errorf("Unexpected domain counts: %v", latest.DomainCounts)
	}
}
