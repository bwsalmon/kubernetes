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

package preemption

import (
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	fwk "k8s.io/kube-scheduler/framework"
	internalcache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/dynamicresources"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	schedulertesting "k8s.io/kubernetes/pkg/scheduler/testing"
)

type testDRAResourceClaimsTracker struct {
	fwk.ResourceClaimTracker
	claims map[string]*resourceapi.ResourceClaim
}

func (t *testDRAResourceClaimsTracker) Get(ns, name string) (*resourceapi.ResourceClaim, error) {
	key := ns + "/" + name
	if c, ok := t.claims[key]; ok {
		return c, nil
	}
	return nil, fwk.ErrNotFound
}

type testDRADeviceClassLister struct {
	fwk.DeviceClassLister
	classes map[string]*resourceapi.DeviceClass
}

func (t *testDRADeviceClassLister) Get(name string) (*resourceapi.DeviceClass, error) {
	if c, ok := t.classes[name]; ok {
		return c, nil
	}
	return nil, fwk.ErrNotFound
}

type testDRAResourceSliceLister struct {
	fwk.ResourceSliceLister
	slices []*resourceapi.ResourceSlice
}

func (t *testDRAResourceSliceLister) ListWithDeviceTaintRules() ([]*resourceapi.ResourceSlice, error) {
	return t.slices, nil
}

type testSharedDRAManager struct {
	fwk.SharedDRAManager
	claimsTracker *testDRAResourceClaimsTracker
	classLister   *testDRADeviceClassLister
	sliceLister   *testDRAResourceSliceLister
}

func (m *testSharedDRAManager) ResourceClaims() fwk.ResourceClaimTracker {
	return m.claimsTracker
}

func (m *testSharedDRAManager) DeviceClasses() fwk.DeviceClassLister {
	return m.classLister
}

func (m *testSharedDRAManager) ResourceSlices() fwk.ResourceSliceLister {
	return m.sliceLister
}

func makeDeviceSlice(driver, pool string, nodeName *string, allNodes *bool, count int) *resourceapi.ResourceSlice {
	devices := make([]resourceapi.Device, count)
	for i := 0; i < count; i++ {
		devices[i] = resourceapi.Device{
			Name: fmt.Sprintf("dev-%d", i),
		}
	}
	return &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("%s-%s-slice", driver, pool),
		},
		Spec: resourceapi.ResourceSliceSpec{
			Driver:   driver,
			Pool:     resourceapi.ResourcePool{Name: pool},
			NodeName: nodeName,
			AllNodes: allNodes,
			Devices:  devices,
		},
	}
}

func makePodWithClaim(name, ns string, priority int32, claimName string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID(name),
		},
		Spec: v1.PodSpec{
			Priority: &priority,
			ResourceClaims: []v1.PodResourceClaim{
				{
					Name: claimName,
				},
			},
		},
	}
}

func makeDRAClaimWithCount(name, ns, className string, count int64) *resourceapi.ResourceClaim {
	return &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID(name),
		},
		Spec: resourceapi.ResourceClaimSpec{
			Devices: resourceapi.DeviceClaim{
				Requests: []resourceapi.DeviceRequest{
					{
						Name: "req-1",
						Exactly: &resourceapi.ExactDeviceRequest{
							DeviceClassName: className,
							Count:           count,
						},
					},
				},
			},
		},
	}
}

func makeAllocatedDRAClaim(name, ns, driver, pool string, deviceCount int) *resourceapi.ResourceClaim {
	results := make([]resourceapi.DeviceRequestAllocationResult, deviceCount)
	for i := 0; i < deviceCount; i++ {
		results[i] = resourceapi.DeviceRequestAllocationResult{
			Driver: driver,
			Pool:   pool,
			Device: fmt.Sprintf("dev-%d", i),
		}
	}
	return &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID(name),
		},
		Status: resourceapi.ResourceClaimStatus{
			Allocation: &resourceapi.AllocationResult{
				Devices: resourceapi.DeviceAllocationResult{
					Results: results,
				},
			},
		},
	}
}

func ptrString(s string) *string { return &s }
func ptrBool(b bool) *bool       { return &b }

func TestDRADevicePoolPreFilter_NonDRAPod_LazyEvaluation(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	logger := klog.FromContext(ctx)

	state := framework.NewCycleState()
	pod := makeTestPod("regular-pod", "default", 100, 1000, 1024)
	node := schedulertesting.MakeNode().Name("node-1").Capacity(map[v1.ResourceName]string{
		v1.ResourceCPU:    "4",
		v1.ResourceMemory: "8Gi",
	}).Obj()
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(node)

	// Nil or empty DRAManager
	fit, status := DRADevicePoolPreFilter(logger, state, pod, nodeInfo, nil, nil, nil)
	if !fit || status != nil {
		t.Fatalf("expected non-DRA pod to pass pre-filter with zero overhead, got fit=%v status=%v", fit, status)
	}
}

func TestDRADevicePoolPreFilter_NodeMissingDriver(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	logger := klog.FromContext(ctx)

	state := framework.NewCycleState()
	preemptor := makePodWithClaim("gpu-preemptor", "default", 1000, "claim-h100")
	claimH100 := makeDRAClaimWithCount("claim-h100", "default", "nvidia-h100", 8)

	classH100 := &resourceapi.DeviceClass{
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
	}

	// Node only has AMD GPUs (driver: amd.com/gpu)
	amdSlice := makeDeviceSlice("amd.com/gpu", "mi300", ptrString("node-amd"), nil, 8)

	draMgr := &testSharedDRAManager{
		claimsTracker: &testDRAResourceClaimsTracker{
			claims: map[string]*resourceapi.ResourceClaim{
				"default/claim-h100": claimH100,
			},
		},
		classLister: &testDRADeviceClassLister{
			classes: map[string]*resourceapi.DeviceClass{
				"nvidia-h100": classH100,
			},
		},
		sliceLister: &testDRAResourceSliceLister{
			slices: []*resourceapi.ResourceSlice{amdSlice},
		},
	}

	node := schedulertesting.MakeNode().Name("node-amd").Obj()
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(node)

	fit, status := DRADevicePoolPreFilter(logger, state, preemptor, nodeInfo, draMgr, nil, nil)
	if fit {
		t.Fatalf("expected node without NVIDIA driver to be pruned, got fit=true")
	}
	if status == nil || (status.Code() != fwk.Unschedulable && status.Code() != fwk.UnschedulableAndUnresolvable) {
		t.Fatalf("expected Unschedulable status, got %v", status)
	}
}

func TestDRADevicePoolPreFilter_InsufficientTotalDevices(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	logger := klog.FromContext(ctx)

	state := framework.NewCycleState()
	preemptor := makePodWithClaim("gpu-preemptor", "default", 1000, "claim-h100")
	claimH100 := makeDRAClaimWithCount("claim-h100", "default", "nvidia-h100", 8)

	classH100 := &resourceapi.DeviceClass{
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
	}

	// Node only has 4 NVIDIA GPUs, but preemptor requests 8
	nvidiaSlice := makeDeviceSlice("gpu.nvidia.com", "h100", ptrString("node-small"), nil, 4)

	draMgr := &testSharedDRAManager{
		claimsTracker: &testDRAResourceClaimsTracker{
			claims: map[string]*resourceapi.ResourceClaim{
				"default/claim-h100": claimH100,
			},
		},
		classLister: &testDRADeviceClassLister{
			classes: map[string]*resourceapi.DeviceClass{
				"nvidia-h100": classH100,
			},
		},
		sliceLister: &testDRAResourceSliceLister{
			slices: []*resourceapi.ResourceSlice{nvidiaSlice},
		},
	}

	node := schedulertesting.MakeNode().Name("node-small").Obj()
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(node)

	fit, status := DRADevicePoolPreFilter(logger, state, preemptor, nodeInfo, draMgr, nil, nil)
	if fit {
		t.Fatalf("expected node with only 4/8 devices to be pruned, got fit=true")
	}
	if status == nil || (status.Code() != fwk.Unschedulable && status.Code() != fwk.UnschedulableAndUnresolvable) {
		t.Fatalf("expected Unschedulable status, got %v", status)
	}
}

func TestDRADevicePoolPreFilter_DisruptionAccounting(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	logger := klog.FromContext(ctx)

	classH100 := &resourceapi.DeviceClass{
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
	}

	// Node has 8 NVIDIA GPUs
	nvidiaSlice := makeDeviceSlice("gpu.nvidia.com", "h100", ptrString("node-dgx"), nil, 8)

	// Preemptor needs 6 GPUs with Priority=500
	preemptor := makePodWithClaim("preemptor", "default", 500, "claim-preemptor")
	claimPreemptor := makeDRAClaimWithCount("claim-preemptor", "default", "nvidia-h100", 6)

	// Pod A: High priority pod (Priority=1000) holding 4 GPUs -> CANNOT be preempted
	highPod := makePodWithClaim("high-pod", "default", 1000, "claim-high")
	claimHigh := makeAllocatedDRAClaim("claim-high", "default", "gpu.nvidia.com", "h100", 4)

	// Pod B: Lower priority victim pod (Priority=100) holding 4 GPUs -> CAN be preempted
	lowPod := makePodWithClaim("low-pod", "default", 100, "claim-low")
	claimLow := makeAllocatedDRAClaim("claim-low", "default", "gpu.nvidia.com", "h100", 4)

	draMgr := &testSharedDRAManager{
		claimsTracker: &testDRAResourceClaimsTracker{
			claims: map[string]*resourceapi.ResourceClaim{
				"default/claim-preemptor": claimPreemptor,
				"default/claim-high":      claimHigh,
				"default/claim-low":       claimLow,
			},
		},
		classLister: &testDRADeviceClassLister{
			classes: map[string]*resourceapi.DeviceClass{
				"nvidia-h100": classH100,
			},
		},
		sliceLister: &testDRAResourceSliceLister{
			slices: []*resourceapi.ResourceSlice{nvidiaSlice},
		},
	}

	t.Run("Non-preemptible pod occupies devices exceeding remaining capacity -> pruned", func(t *testing.T) {
		state := framework.NewCycleState()
		node := schedulertesting.MakeNode().Name("node-dgx").Obj()
		nodeInfo := framework.NewNodeInfo()
		nodeInfo.SetNode(node)
		nodeInfo.AddPod(highPod) // 4 devices non-preemptible -> remaining available = 8 - 4 = 4 < 6 required

		fit, status := DRADevicePoolPreFilter(logger, state, preemptor, nodeInfo, draMgr, nil, nil)
		if fit {
			t.Fatalf("expected node to be pruned due to non-preemptible allocation, got fit=true")
		}
		if status == nil || (status.Code() != fwk.Unschedulable && status.Code() != fwk.UnschedulableAndUnresolvable) {
			t.Fatalf("expected Unschedulable, got %v", status)
		}
	})

	t.Run("Lower priority victim pod occupies devices -> can be preempted -> passes prefilter", func(t *testing.T) {
		state := framework.NewCycleState()
		node := schedulertesting.MakeNode().Name("node-dgx").Obj()
		nodeInfo := framework.NewNodeInfo()
		nodeInfo.SetNode(node)
		nodeInfo.AddPod(lowPod) // 4 devices held by victim -> available = 8 >= 6 required

		fit, status := DRADevicePoolPreFilter(logger, state, preemptor, nodeInfo, draMgr, nil, nil)
		if !fit || status != nil {
			t.Fatalf("expected node to pass prefilter when devices held by lower-priority victims, got fit=%v status=%v", fit, status)
		}
	})
}

func TestDRADevicePoolPreFilter_GangTemplateCacheHits(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	logger := klog.FromContext(ctx)

	classH100 := &resourceapi.DeviceClass{
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
	}

	// 8 H100 devices on node-gpu
	slice := makeDeviceSlice("gpu.nvidia.com", "h100", ptrString("node-gpu"), nil, 8)
	draMgr := &testSharedDRAManager{
		claimsTracker: &testDRAResourceClaimsTracker{
			claims: map[string]*resourceapi.ResourceClaim{
				"default/claim-gang-0": makeDRAClaimWithCount("claim-gang-0", "default", "nvidia-h100", 8),
				"default/claim-gang-1": makeDRAClaimWithCount("claim-gang-1", "default", "nvidia-h100", 8),
				"default/claim-gang-2": makeDRAClaimWithCount("claim-gang-2", "default", "nvidia-h100", 8),
				"default/claim-gang-3": makeDRAClaimWithCount("claim-gang-3", "default", "nvidia-h100", 8),
			},
		},
		classLister: &testDRADeviceClassLister{
			classes: map[string]*resourceapi.DeviceClass{
				"nvidia-h100": classH100,
			},
		},
		sliceLister: &testDRAResourceSliceLister{
			slices: []*resourceapi.ResourceSlice{slice},
		},
	}

	node := schedulertesting.MakeNode().Name("node-gpu").Obj()
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(node)

	// Shared PodGroupCycleState for gang
	pgState := framework.NewCycleState()

	// Pod 0 in gang: Cold miss
	state0 := framework.NewCycleState()
	state0.SetPodGroupCycleState(pgState)
	pod0 := makePodWithClaim("gang-pod-0", "default", 100, "claim-gang-0")

	fit0, status0 := DRADevicePoolPreFilter(logger, state0, pod0, nodeInfo, draMgr, nil, nil)
	if !fit0 || status0 != nil {
		t.Fatalf("pod0 failed prefilter: %v", status0)
	}

	cache := dynamicresources.GetOrCreateClaimTemplateDriverCache(state0)
	hits, misses, _ := cache.Stats()
	if hits != 0 || misses != 1 {
		t.Fatalf("expected 0 hits, 1 miss after pod0, got hits=%d misses=%d", hits, misses)
	}

	// Pod 1 in gang: Cache HIT!
	state1 := framework.NewCycleState()
	state1.SetPodGroupCycleState(pgState)
	pod1 := makePodWithClaim("gang-pod-1", "default", 100, "claim-gang-1")

	fit1, status1 := DRADevicePoolPreFilter(logger, state1, pod1, nodeInfo, draMgr, nil, nil)
	if !fit1 || status1 != nil {
		t.Fatalf("pod1 failed prefilter: %v", status1)
	}

	hits, misses, _ = cache.Stats()
	if hits != 1 || misses != 1 {
		t.Fatalf("expected 1 hit, 1 miss after pod1, got hits=%d misses=%d", hits, misses)
	}

	// Pod 2 and Pod 3 in gang: Cache HITS!
	for i := 2; i <= 3; i++ {
		stateN := framework.NewCycleState()
		stateN.SetPodGroupCycleState(pgState)
		podN := makePodWithClaim(fmt.Sprintf("gang-pod-%d", i), "default", 100, fmt.Sprintf("claim-gang-%d", i))
		fitN, statusN := DRADevicePoolPreFilter(logger, stateN, podN, nodeInfo, draMgr, nil, nil)
		if !fitN || statusN != nil {
			t.Fatalf("pod%d failed prefilter: %v", i, statusN)
		}
	}

	hits, misses, _ = cache.Stats()
	if hits != 3 || misses != 1 {
		t.Fatalf("expected 3 hits, 1 miss across gang pods, got hits=%d misses=%d", hits, misses)
	}
}

func TestCanNodeFitPreemptorHeadroomWithDRA_EndToEnd(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	logger := klog.FromContext(ctx)

	state := framework.NewCycleState()
	preemptor := makePodWithClaim("gpu-preemptor", "default", 1000, "claim-h100")
	claimH100 := makeDRAClaimWithCount("claim-h100", "default", "nvidia-h100", 8)

	classH100 := &resourceapi.DeviceClass{
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
	}

	slice := makeDeviceSlice("gpu.nvidia.com", "h100", ptrString("node-1"), nil, 8)
	draMgr := &testSharedDRAManager{
		claimsTracker: &testDRAResourceClaimsTracker{
			claims: map[string]*resourceapi.ResourceClaim{
				"default/claim-h100": claimH100,
			},
		},
		classLister: &testDRADeviceClassLister{
			classes: map[string]*resourceapi.DeviceClass{
				"nvidia-h100": classH100,
			},
		},
		sliceLister: &testDRAResourceSliceLister{
			slices: []*resourceapi.ResourceSlice{slice},
		},
	}

	node := schedulertesting.MakeNode().Name("node-1").Capacity(map[v1.ResourceName]string{
		v1.ResourceCPU:    "32",
		v1.ResourceMemory: "64Gi",
	}).Obj()
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(node)

	// Add lower priority victim
	victim := makeTestPod("victim-1", "default", 100, 1000, 1024)
	nodeInfo.AddPod(victim)

	fit, status := CanNodeFitPreemptorHeadroomWithDRA(logger, state, preemptor, nil, nodeInfo, draMgr, nil, nil, false)
	if !fit || status != nil {
		t.Fatalf("expected CanNodeFitPreemptorHeadroomWithDRA to pass, got fit=%v status=%v", fit, status)
	}
}

func TestDryRunPreemption_DRAPreFilter_PrunesInfeasibleNodes(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	logger := klog.FromContext(ctx)

	preemptor := makePodWithClaim("preemptor-h100", "default", 1000, "claim-h100")
	claimH100 := makeDRAClaimWithCount("claim-h100", "default", "nvidia-h100", 8)

	classH100 := &resourceapi.DeviceClass{
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
	}

	// 3 Nodes in cluster:
	// Node 1: CPU-only (0 GPUs)
	// Node 2: AMD GPU (8 GPUs, wrong driver)
	// Node 3: NVIDIA H100 (8 GPUs, matching driver)
	amdSlice := makeDeviceSlice("amd.com/gpu", "mi300", ptrString("node-2-amd"), nil, 8)
	nvidiaSlice := makeDeviceSlice("gpu.nvidia.com", "h100", ptrString("node-3-nvidia"), nil, 8)

	draMgr := &testSharedDRAManager{
		claimsTracker: &testDRAResourceClaimsTracker{
			claims: map[string]*resourceapi.ResourceClaim{
				"default/claim-h100": claimH100,
			},
		},
		classLister: &testDRADeviceClassLister{
			classes: map[string]*resourceapi.DeviceClass{
				"nvidia-h100": classH100,
			},
		},
		sliceLister: &testDRAResourceSliceLister{
			slices: []*resourceapi.ResourceSlice{amdSlice, nvidiaSlice},
		},
	}

	v1Pod := makeTestPod("v1", "default", 100, 1000, 1024)
	v1Pod.Spec.NodeName = "node-1-cpu"
	node1 := schedulertesting.MakeNode().Name("node-1-cpu").Capacity(map[v1.ResourceName]string{
		v1.ResourceCPU:    "32",
		v1.ResourceMemory: "64Gi",
	}).Obj()
	nodeInfo1 := framework.NewNodeInfo()
	nodeInfo1.SetNode(node1)
	nodeInfo1.AddPod(v1Pod)

	v2Pod := makeTestPod("v2", "default", 100, 1000, 1024)
	v2Pod.Spec.NodeName = "node-2-amd"
	node2 := schedulertesting.MakeNode().Name("node-2-amd").Capacity(map[v1.ResourceName]string{
		v1.ResourceCPU:    "32",
		v1.ResourceMemory: "64Gi",
	}).Obj()
	nodeInfo2 := framework.NewNodeInfo()
	nodeInfo2.SetNode(node2)
	nodeInfo2.AddPod(v2Pod)

	v3Pod := makeTestPod("v3", "default", 100, 1000, 1024)
	v3Pod.Spec.NodeName = "node-3-nvidia"
	node3 := schedulertesting.MakeNode().Name("node-3-nvidia").Capacity(map[v1.ResourceName]string{
		v1.ResourceCPU:    "32",
		v1.ResourceMemory: "64Gi",
	}).Obj()
	nodeInfo3 := framework.NewNodeInfo()
	nodeInfo3.SetNode(node3)
	nodeInfo3.AddPod(v3Pod)

	potentialNodes := []fwk.NodeInfo{nodeInfo1, nodeInfo2, nodeInfo3}

	// Create test framework handle
	snapshot := internalcache.NewSnapshot([]*v1.Pod{v1Pod, v2Pod, v3Pod}, []*v1.Node{node1, node2, node3})
	fh, err := frameworkruntime.NewFramework(ctx, nil, nil,
		frameworkruntime.WithSharedDRAManager(draMgr),
		frameworkruntime.WithSnapshotSharedLister(snapshot),
		frameworkruntime.WithMutableSnapshotLister(snapshot),
	)
	if err != nil {
		t.Fatalf("failed to create test framework handle: %v", err)
	}

	evaluator := &Evaluator{
		Interface: &FakePostFilterPlugin{},
		Handler:   fh,
	}

	state := framework.NewCycleState()
	candidates, nodeStatuses, err := evaluator.DryRunPreemption(ctx, state, preemptor, potentialNodes, nil, 0, int32(len(potentialNodes)))
	if err != nil {
		t.Fatalf("DryRunPreemption returned unexpected error: %v", err)
	}

	// Verify that node-1-cpu and node-2-amd were pruned by DRADevicePoolPreFilter in Layer 1
	st1 := nodeStatuses.Get("node-1-cpu")
	if st1 == nil || (st1.Code() != fwk.Unschedulable && st1.Code() != fwk.UnschedulableAndUnresolvable) {
		t.Fatalf("expected node-1-cpu to be pruned by DRA pre-filter, got status=%v", st1)
	}
	st2 := nodeStatuses.Get("node-2-amd")
	if st2 == nil || (st2.Code() != fwk.Unschedulable && st2.Code() != fwk.UnschedulableAndUnresolvable) {
		t.Fatalf("expected node-2-amd to be pruned by DRA pre-filter, got status=%v", st2)
	}

	// Verify that only node-3-nvidia passed DRA pre-filter
	if len(candidates) != 1 {
		t.Fatalf("expected exactly 1 candidate, got %d", len(candidates))
	}
	if candidates[0].Name() != "node-3-nvidia" {
		t.Errorf("expected candidate node-3-nvidia, got %s", candidates[0].Name())
	}
	_ = logger
}
