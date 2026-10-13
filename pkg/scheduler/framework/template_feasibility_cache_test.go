/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package framework

import (
	"sync"
	"testing"

	fwk "k8s.io/kube-scheduler/framework"
)

func TestTemplateFeasibilityCache_Basic(t *testing.T) {
	cache := NewTemplateFeasibilityCache()

	sigA := "signature-worker-gpu"
	sigB := "signature-driver-cpu"

	// Initial check: cache miss
	status, found := cache.GetStaticFeasibility(sigA, "node-1")
	if found || status != nil {
		t.Fatalf("expected miss for unrecorded node, got found=%v, status=%v", found, status)
	}

	// Record success for node-1 on sigA
	cache.SetStaticFeasibility(sigA, "node-1", fwk.NewStatus(fwk.Success))

	// Record failure for node-2 on sigA
	failStatus := fwk.NewStatus(fwk.Unschedulable, "node selector mismatch")
	cache.SetStaticFeasibility(sigA, "node-2", failStatus)

	// Verify hit on node-1
	status, found = cache.GetStaticFeasibility(sigA, "node-1")
	if !found || !status.IsSuccess() {
		t.Fatalf("expected success on node-1 for sigA, got found=%v, status=%v", found, status)
	}

	// Verify hit on node-2
	status, found = cache.GetStaticFeasibility(sigA, "node-2")
	if !found || status.IsSuccess() || status.Message() != "node selector mismatch" {
		t.Fatalf("expected failure on node-2 for sigA, got found=%v, status=%v", found, status)
	}

	// Verify sigB on node-1 is a miss (distinct template signature)
	status, found = cache.GetStaticFeasibility(sigB, "node-1")
	if found || status != nil {
		t.Fatalf("expected miss for sigB on node-1, got found=%v, status=%v", found, status)
	}

	hits, misses, _ := cache.Stats()
	if hits != 2 || misses != 2 {
		t.Fatalf("expected 2 hits and 2 misses, got hits=%d, misses=%d", hits, misses)
	}
}

func TestTemplateFeasibilityCache_Clone(t *testing.T) {
	cache := NewTemplateFeasibilityCache()
	sig := "sig-clone-test"

	cache.SetStaticFeasibility(sig, "node-1", fwk.NewStatus(fwk.Success))
	cache.RecordPluginsSaved(10)

	clone := cache.Clone().(*TemplateFeasibilityCache)

	// Verify cloned data
	status, found := clone.GetStaticFeasibility(sig, "node-1")
	if !found || !status.IsSuccess() {
		t.Fatalf("expected cloned cache to contain node-1 success")
	}

	// Mutate clone; ensure original is isolated
	clone.SetStaticFeasibility(sig, "node-2", fwk.NewStatus(fwk.Unschedulable))

	_, foundInOriginal := cache.GetStaticFeasibility(sig, "node-2")
	if foundInOriginal {
		t.Fatalf("mutation in clone leaked into original cache")
	}
}

func TestTemplateFeasibilityCache_Concurrency(t *testing.T) {
	cache := NewTemplateFeasibilityCache()
	sig := "sig-concurrent-gang"

	var wg sync.WaitGroup
	// 20 concurrent readers and writers
	for i := 0; i < 20; i++ {
		wg.Add(1)
		nodeID := i % 5
		nodeName := "node-" + string(rune('A'+nodeID))
		go func(id int, n string) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if id%2 == 0 {
					cache.SetStaticFeasibility(sig, n, fwk.NewStatus(fwk.Success))
					cache.RecordPluginsSaved(4)
				} else {
					_, _ = cache.GetStaticFeasibility(sig, n)
				}
			}
		}(i, nodeName)
	}
	wg.Wait()
}

func TestTemplateFeasibilityCache_MultiTenantNamespaceIsolation(t *testing.T) {
	cache := NewTemplateFeasibilityCache()

	// In multi-tenant environments, pod templates across namespaces could have identical
	// PodSignatures if signature does not include namespace.
	// When scoped properly to per-gang PodGroupCycleState, distinct gangs in different namespaces
	// get isolated cycle states. However, within a shared cache, signatures must distinguish or
	// be scoped per gang.
	sigTenantA := "ns-tenant-a/sig-worker"
	sigTenantB := "ns-tenant-b/sig-worker"

	cache.SetStaticFeasibility(sigTenantA, "node-isolated-1", fwk.NewStatus(fwk.Success))
	cache.SetStaticFeasibility(sigTenantB, "node-isolated-1", fwk.NewStatus(fwk.Unschedulable, "namespace policy denied"))

	statusA, foundA := cache.GetStaticFeasibility(sigTenantA, "node-isolated-1")
	if !foundA || !statusA.IsSuccess() {
		t.Fatalf("expected tenant A to succeed on node-isolated-1, got %v", statusA)
	}

	statusB, foundB := cache.GetStaticFeasibility(sigTenantB, "node-isolated-1")
	if !foundB || statusB.IsSuccess() || statusB.Message() != "namespace policy denied" {
		t.Fatalf("expected tenant B to fail on node-isolated-1 with namespace policy denied, got %v", statusB)
	}
}

func TestTemplateFeasibilityCache_EdgeCasesAndInvalidation(t *testing.T) {
	cache := NewTemplateFeasibilityCache()
	sig := "sig-test-edge"

	// Test empty inputs
	if status, ok := cache.GetStaticFeasibility("", "node-1"); ok || status != nil {
		t.Fatalf("expected empty sig to be rejected")
	}
	if status, ok := cache.GetStaticFeasibility(sig, ""); ok || status != nil {
		t.Fatalf("expected empty node to be rejected")
	}

	// Cache status overwrite (simulating node update / taint addition)
	cache.SetStaticFeasibility(sig, "node-1", fwk.NewStatus(fwk.Success))
	status, ok := cache.GetStaticFeasibility(sig, "node-1")
	if !ok || !status.IsSuccess() {
		t.Fatalf("expected success")
	}

	// Dynamic update: node became unschedulable / tainted
	cache.SetStaticFeasibility(sig, "node-1", fwk.NewStatus(fwk.Unschedulable, "node tainted"))
	status, ok = cache.GetStaticFeasibility(sig, "node-1")
	if !ok || status.IsSuccess() || status.Message() != "node tainted" {
		t.Fatalf("expected updated failure status after node taint mutation")
	}
}
