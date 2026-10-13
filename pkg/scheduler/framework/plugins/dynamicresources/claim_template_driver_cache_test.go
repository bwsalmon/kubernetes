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

package dynamicresources

import (
	"sync"
	"testing"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

func TestClaimTemplateDriverCache_BasicAndStats(t *testing.T) {
	cache := NewClaimTemplateDriverCache()

	sig := "sig-h100-cluster"
	node1 := "node-1"
	node2 := "node-2"

	// Miss on empty cache
	if _, hit := cache.GetFeasibility(sig, node1); hit {
		t.Fatalf("expected miss on empty cache")
	}

	// Set feasibility
	feas1 := &ClaimDriverFeasibility{
		Feasible:      true,
		Driver:        "gpu.nvidia.com",
		RequiredCount: 8,
	}
	cache.SetFeasibility(sig, node1, feas1)

	feas2 := &ClaimDriverFeasibility{
		Feasible:      false,
		Reason:        "node does not host driver gpu.nvidia.com",
		Driver:        "gpu.nvidia.com",
		RequiredCount: 8,
	}
	cache.SetFeasibility(sig, node2, feas2)

	// Hit node 1
	got1, hit := cache.GetFeasibility(sig, node1)
	if !hit || !got1.Feasible || got1.Driver != "gpu.nvidia.com" {
		t.Fatalf("expected hit on node1 with feasible=true, got %+v", got1)
	}

	// Hit node 2
	got2, hit := cache.GetFeasibility(sig, node2)
	if !hit || got2.Feasible || got2.Reason != feas2.Reason {
		t.Fatalf("expected hit on node2 with feasible=false, got %+v", got2)
	}

	// Check Stats
	hits, misses, pruned := cache.Stats()
	if hits != 2 {
		t.Errorf("expected 2 hits, got %d", hits)
	}
	if misses != 1 {
		t.Errorf("expected 1 miss, got %d", misses)
	}
	if pruned != 1 {
		t.Errorf("expected 1 pruned, got %d", pruned)
	}
}

func TestClaimTemplateDriverCache_CloneImmutability(t *testing.T) {
	cache := NewClaimTemplateDriverCache()
	sig := "sig-1"
	node := "node-a"
	cache.SetFeasibility(sig, node, &ClaimDriverFeasibility{Feasible: true, Driver: "drv1"})

	cloned := cache.Clone().(*ClaimTemplateDriverCache)

	// Mutate original
	cache.SetFeasibility(sig, "node-b", &ClaimDriverFeasibility{Feasible: false, Driver: "drv1"})

	// Verify cloned does not have node-b
	if _, hit := cloned.GetFeasibility(sig, "node-b"); hit {
		t.Fatalf("cloned cache mutated by modification to original cache")
	}

	// Verify cloned has node-a
	if got, hit := cloned.GetFeasibility(sig, node); !hit || !got.Feasible {
		t.Fatalf("cloned cache missing node-a")
	}
}

func TestClaimTemplateDriverCache_ConcurrentSafe(t *testing.T) {
	cache := NewClaimTemplateDriverCache()
	sig := "sig-concurrent"

	var wg sync.WaitGroup
	numWorkers := 16
	opsPerWorker := 100

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				nodeName := "node-worker"
				if (workerID+i)%2 == 0 {
					cache.SetFeasibility(sig, nodeName, &ClaimDriverFeasibility{
						Feasible: true,
						Driver:   "drv",
					})
				} else {
					cache.GetFeasibility(sig, nodeName)
				}
			}
		}(w)
	}
	wg.Wait()
}

func TestClaimTemplateDriverCache_PodGroupCycleStateScoping(t *testing.T) {
	state := framework.NewCycleState()
	pgState := framework.NewCycleState()
	state.SetPodGroupCycleState(pgState)

	// Cache should be placed onto pgState
	cache := GetOrCreateClaimTemplateDriverCache(state)
	if cache == nil {
		t.Fatalf("expected non-nil cache")
	}

	sig := "sig-pg"
	cache.SetFeasibility(sig, "node-x", &ClaimDriverFeasibility{Feasible: true})

	// Retrieve again from a new pod cycle sharing the same pgState
	state2 := framework.NewCycleState()
	state2.SetPodGroupCycleState(pgState)
	cache2 := GetOrCreateClaimTemplateDriverCache(state2)
	if cache2 != cache {
		t.Fatalf("expected identical cache reference across gang pod cycles sharing pgState")
	}

	got, hit := cache2.GetFeasibility(sig, "node-x")
	if !hit || !got.Feasible {
		t.Fatalf("expected cached hit on second pod in gang cycle")
	}

	// In dependent cycle with nil pgState, fresh cache is created
	stateIndependent := framework.NewCycleState()
	cacheIndependent := GetOrCreateClaimTemplateDriverCache(stateIndependent)
	if _, hit := cacheIndependent.GetFeasibility(sig, "node-x"); hit {
		t.Fatalf("expected cache miss in independent cycle")
	}
}

type fakeClaimsTracker struct {
	fwk.ResourceClaimTracker
	claims map[string]*resourceapi.ResourceClaim
}

func (f *fakeClaimsTracker) Get(ns, name string) (*resourceapi.ResourceClaim, error) {
	key := ns + "/" + name
	if c, ok := f.claims[key]; ok {
		return c, nil
	}
	return nil, fwk.ErrNotFound
}

type fakeClassLister struct {
	fwk.DeviceClassLister
	classes map[string]*resourceapi.DeviceClass
}

func (f *fakeClassLister) Get(name string) (*resourceapi.DeviceClass, error) {
	if c, ok := f.classes[name]; ok {
		return c, nil
	}
	return nil, fwk.ErrNotFound
}

type fakeSharedDRAManager struct {
	fwk.SharedDRAManager
	tracker *fakeClaimsTracker
	classes *fakeClassLister
}

func (f *fakeSharedDRAManager) ResourceClaims() fwk.ResourceClaimTracker {
	return f.tracker
}

func (f *fakeSharedDRAManager) DeviceClasses() fwk.DeviceClassLister {
	return f.classes
}

func TestExtractPodDRARequirements(t *testing.T) {
	mgr := &fakeSharedDRAManager{
		tracker: &fakeClaimsTracker{
			claims: map[string]*resourceapi.ResourceClaim{
				"default/claim-gpu": {
					ObjectMeta: metav1.ObjectMeta{Name: "claim-gpu", Namespace: "default", UID: types.UID("c1")},
					Spec: resourceapi.ResourceClaimSpec{
						Devices: resourceapi.DeviceClaim{
							Requests: []resourceapi.DeviceRequest{
								{
									Name: "gpu-req",
									Exactly: &resourceapi.ExactDeviceRequest{
										DeviceClassName: "nvidia-h100",
										Count:           8,
									},
								},
							},
						},
					},
				},
			},
		},
		classes: &fakeClassLister{
			classes: map[string]*resourceapi.DeviceClass{
				"nvidia-h100": {
					ObjectMeta: metav1.ObjectMeta{Name: "nvidia-h100"},
					Spec: resourceapi.DeviceClassSpec{
						Config: []resourceapi.DeviceClassConfiguration{
							{
								DeviceConfiguration: resourceapi.DeviceConfiguration{
									Opaque: &resourceapi.OpaqueDeviceConfiguration{
										Driver: "gpu.nvidia.com",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-pod", Namespace: "default"},
		Spec: v1.PodSpec{
			ResourceClaims: []v1.PodResourceClaim{
				{
					Name: "claim-gpu",
				},
			},
		},
	}

	reqs, sig, err := ExtractPodDRARequirements(pod, mgr)
	if err != nil {
		t.Fatalf("ExtractPodDRARequirements failed: %v", err)
	}
	if len(reqs) != 1 {
		t.Fatalf("expected 1 req, got %d", len(reqs))
	}
	if reqs[0].Driver != "gpu.nvidia.com" || reqs[0].Count != 8 {
		t.Errorf("unexpected req: %+v", reqs[0])
	}
	if sig == "" {
		t.Errorf("expected non-empty signature")
	}

	// Verify deterministic signature
	_, sig2, _ := ExtractPodDRARequirements(pod, mgr)
	if sig != sig2 {
		t.Errorf("expected signature to be deterministic, got %s != %s", sig, sig2)
	}
}
