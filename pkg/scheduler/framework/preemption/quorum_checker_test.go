/*
Copyright The Kubernetes Authors.

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
	"context"
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2/ktesting"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

func makeQuorumTestPod(name string, namespace string, priority int32, cpuMilli int64, memBytes int64, gpuCount int64) *v1.Pod {
	reqs := v1.ResourceList{
		v1.ResourceCPU:    *resource.NewMilliQuantity(cpuMilli, resource.DecimalSI),
		v1.ResourceMemory: *resource.NewQuantity(memBytes, resource.BinarySI),
	}
	if gpuCount > 0 {
		reqs["nvidia.com/gpu"] = *resource.NewQuantity(gpuCount, resource.DecimalSI)
	}
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(fmt.Sprintf("pod-%s-%s", namespace, name)),
		},
		Spec: v1.PodSpec{
			Priority: &priority,
			Containers: []v1.Container{
				{
					Name: "main",
					Resources: v1.ResourceRequirements{
						Requests: reqs,
						Limits:   reqs,
					},
				},
			},
		},
	}
}

func TestCheckGangQuorumAndCapacityFeasibility(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)

	// Construct gang pods (4 pods each requesting 1000m CPU, 1024Mi Mem, 1 GPU)
	gangPods := make([]*v1.Pod, 4)
	for i := 0; i < 4; i++ {
		gangPods[i] = makeQuorumTestPod(fmt.Sprintf("gang-pod-%d", i), "default", 1000, 1000, 1024*1024*1024, 1)
	}

	t.Run("Viable node count less than minMember -> fast fail in O(1)", func(t *testing.T) {
		// Only 3 viable node headrooms when minMember = 4
		viableHeadrooms := []*ViableNodeHeadroom{
			{NodeName: "node-1", AvailableMilliCPU: 4000, AvailableMemory: 4 * 1024 * 1024 * 1024, AvailableScalars: map[v1.ResourceName]int64{"nvidia.com/gpu": 2}},
			{NodeName: "node-2", AvailableMilliCPU: 4000, AvailableMemory: 4 * 1024 * 1024 * 1024, AvailableScalars: map[v1.ResourceName]int64{"nvidia.com/gpu": 2}},
			{NodeName: "node-3", AvailableMilliCPU: 4000, AvailableMemory: 4 * 1024 * 1024 * 1024, AvailableScalars: map[v1.ResourceName]int64{"nvidia.com/gpu": 2}},
		}

		res := CheckGangQuorumAndCapacityFeasibility(gangPods, 4, viableHeadrooms, false)
		if res.Passed {
			t.Fatalf("Expected check to fail due to N_viable < minMember, but passed")
		}
		if res.Status.Code() != fwk.Unschedulable {
			t.Errorf("Expected Unschedulable code, got %v", res.Status.Code())
		}
		t.Logf("Quorum check early-exit success: %s", res.Status.Message())
	})

	t.Run("Scalar capacity insufficient (Knapsack GPU bound) -> fast fail in O(1)", func(t *testing.T) {
		// 4 viable nodes, but total GPUs across viable nodes = 3 (gang needs 4)
		viableHeadrooms := []*ViableNodeHeadroom{
			{NodeName: "node-1", AvailableMilliCPU: 4000, AvailableMemory: 4 * 1024 * 1024 * 1024, AvailableScalars: map[v1.ResourceName]int64{"nvidia.com/gpu": 1}},
			{NodeName: "node-2", AvailableMilliCPU: 4000, AvailableMemory: 4 * 1024 * 1024 * 1024, AvailableScalars: map[v1.ResourceName]int64{"nvidia.com/gpu": 1}},
			{NodeName: "node-3", AvailableMilliCPU: 4000, AvailableMemory: 4 * 1024 * 1024 * 1024, AvailableScalars: map[v1.ResourceName]int64{"nvidia.com/gpu": 1}},
			{NodeName: "node-4", AvailableMilliCPU: 4000, AvailableMemory: 4 * 1024 * 1024 * 1024, AvailableScalars: map[v1.ResourceName]int64{"nvidia.com/gpu": 0}},
		}

		res := CheckGangQuorumAndCapacityFeasibility(gangPods, 4, viableHeadrooms, false)
		if res.Passed {
			t.Fatalf("Expected check to fail due to insufficient GPUs, but passed")
		}
		if res.Status.Code() != fwk.Unschedulable {
			t.Errorf("Expected Unschedulable code, got %v", res.Status.Code())
		}
		t.Logf("Knapsack check early-exit success: %s", res.Status.Message())
	})

	t.Run("CPU capacity insufficient -> fast fail in O(1)", func(t *testing.T) {
		// 4 viable nodes, but total CPU = 3500m (gang needs 4000m)
		viableHeadrooms := []*ViableNodeHeadroom{
			{NodeName: "node-1", AvailableMilliCPU: 1000, AvailableMemory: 4 * 1024 * 1024 * 1024, AvailableScalars: map[v1.ResourceName]int64{"nvidia.com/gpu": 1}},
			{NodeName: "node-2", AvailableMilliCPU: 1000, AvailableMemory: 4 * 1024 * 1024 * 1024, AvailableScalars: map[v1.ResourceName]int64{"nvidia.com/gpu": 1}},
			{NodeName: "node-3", AvailableMilliCPU: 1000, AvailableMemory: 4 * 1024 * 1024 * 1024, AvailableScalars: map[v1.ResourceName]int64{"nvidia.com/gpu": 1}},
			{NodeName: "node-4", AvailableMilliCPU: 500, AvailableMemory: 4 * 1024 * 1024 * 1024, AvailableScalars: map[v1.ResourceName]int64{"nvidia.com/gpu": 1}},
		}

		res := CheckGangQuorumAndCapacityFeasibility(gangPods, 4, viableHeadrooms, false)
		if res.Passed {
			t.Fatalf("Expected check to fail due to insufficient CPU, but passed")
		}
		t.Logf("CPU Knapsack check early-exit success: %s", res.Status.Message())
	})

	t.Run("Memory capacity insufficient -> fast fail in O(1)", func(t *testing.T) {
		// 4 viable nodes, but total memory = 3Gi (gang needs 4Gi)
		viableHeadrooms := []*ViableNodeHeadroom{
			{NodeName: "node-1", AvailableMilliCPU: 2000, AvailableMemory: 1024 * 1024 * 1024, AvailableScalars: map[v1.ResourceName]int64{"nvidia.com/gpu": 1}},
			{NodeName: "node-2", AvailableMilliCPU: 2000, AvailableMemory: 1024 * 1024 * 1024, AvailableScalars: map[v1.ResourceName]int64{"nvidia.com/gpu": 1}},
			{NodeName: "node-3", AvailableMilliCPU: 2000, AvailableMemory: 1024 * 1024 * 1024, AvailableScalars: map[v1.ResourceName]int64{"nvidia.com/gpu": 1}},
			{NodeName: "node-4", AvailableMilliCPU: 2000, AvailableMemory: 512 * 1024 * 1024, AvailableScalars: map[v1.ResourceName]int64{"nvidia.com/gpu": 1}},
		}

		res := CheckGangQuorumAndCapacityFeasibility(gangPods, 4, viableHeadrooms, false)
		if res.Passed {
			t.Fatalf("Expected check to fail due to insufficient Memory, but passed")
		}
		t.Logf("Memory Knapsack check early-exit success: %s", res.Status.Message())
	})

	t.Run("All quorum and scalar knapsack constraints satisfied -> passes", func(t *testing.T) {
		viableHeadrooms := []*ViableNodeHeadroom{
			{NodeName: "node-1", AvailableMilliCPU: 2000, AvailableMemory: 2 * 1024 * 1024 * 1024, AvailableScalars: map[v1.ResourceName]int64{"nvidia.com/gpu": 1}},
			{NodeName: "node-2", AvailableMilliCPU: 2000, AvailableMemory: 2 * 1024 * 1024 * 1024, AvailableScalars: map[v1.ResourceName]int64{"nvidia.com/gpu": 1}},
			{NodeName: "node-3", AvailableMilliCPU: 2000, AvailableMemory: 2 * 1024 * 1024 * 1024, AvailableScalars: map[v1.ResourceName]int64{"nvidia.com/gpu": 1}},
			{NodeName: "node-4", AvailableMilliCPU: 2000, AvailableMemory: 2 * 1024 * 1024 * 1024, AvailableScalars: map[v1.ResourceName]int64{"nvidia.com/gpu": 1}},
		}

		res := CheckGangQuorumAndCapacityFeasibility(gangPods, 4, viableHeadrooms, false)
		if !res.Passed {
			t.Fatalf("Expected check to pass, got failure: %v", res.Status.Message())
		}
		t.Logf("Feasibility verified: ViableNodeCount=%d, AvailableCPU=%dm, RequiredCPU=%dm", res.ViableNodeCount, res.TotalPreemptibleCPU, res.TotalRequiredCPU)
	})

	t.Run("CalculateNodePreemptibleHeadroom integration with NodeInfo", func(t *testing.T) {
		pod := makeQuorumTestPod("preemptor", "default", 1000, 1000, 1024*1024, 0)
		victim := makeQuorumTestPod("victim", "default", 500, 1000, 1024*1024, 0)
		nonPreemptible := makeQuorumTestPod("non-preemptible", "default", 1500, 1000, 1024*1024, 0)

		node := &v1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "test-node"},
			Status: v1.NodeStatus{
				Allocatable: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("4000m"),
					v1.ResourceMemory: resource.MustParse("4096Mi"),
					v1.ResourcePods:   resource.MustParse("110"),
				},
			},
		}

		nodeInfo := framework.NewNodeInfo(victim, nonPreemptible)
		nodeInfo.SetNode(node)

		headroom, status := CalculateNodePreemptibleHeadroom(logger, pod, nil, nodeInfo, nil, nil, false)
		if !status.IsSuccess() {
			t.Fatalf("Expected success, got status: %v", status.Message())
		}
		if headroom.AvailableMilliCPU != 3000 {
			t.Errorf("Expected 3000m CPU available, got %d", headroom.AvailableMilliCPU)
		}
		expectedMem := int64((4096 - 1) * 1024 * 1024)
		if headroom.AvailableMemory != expectedMem {
			t.Errorf("Expected %d memory available, got %d", expectedMem, headroom.AvailableMemory)
		}
	})
	_ = ctx
}

func TestSuffixQuorumPruningInBranchAndBound(t *testing.T) {
	ctx := context.Background()

	t.Run("Candidate nodes < minMember pruned immediately in B&B", func(t *testing.T) {
		p0 := makeTestPod("p0", "default", 1000, 1000, 1024)
		p1 := makeTestPod("p1", "default", 1000, 1000, 1024)
		p2 := makeTestPod("p2", "default", 1000, 1000, 1024)

		prob := &GangPlacementProblem{
			PreemptorPods:  []*v1.Pod{p0, p1, p2},
			CandidateNodes: []string{"node-0", "node-1"}, // 2 candidate nodes < 3 minMember
			NodeAllocatableCPU: map[string]int64{"node-0": 2000, "node-1": 2000},
			NodeAllocatableMem: map[string]int64{"node-0": 2048, "node-1": 2048},
			PerPodCandidates: [][]GangPodCandidate{
				{{NodeName: "node-0", Cost: ZeroPreemptionCost(), Feasible: true, RequiredMilliCPU: 1000, RequiredMemory: 1024}},
				{{NodeName: "node-1", Cost: ZeroPreemptionCost(), Feasible: true, RequiredMilliCPU: 1000, RequiredMemory: 1024}},
				{{NodeName: "node-0", Cost: ZeroPreemptionCost(), Feasible: true, RequiredMilliCPU: 1000, RequiredMemory: 1024}},
			},
			MinMember: 3,
		}

		opts := DefaultBranchAndBoundOptions()
		opts.EnableSuffixQuorumPruning = true
		res := BranchAndBoundGangSearch(ctx, prob, opts)
		if res.Status.IsSuccess() {
			t.Fatalf("Expected unschedulable when candidate nodes < minMember, got success")
		}
		if res.StatesExplored != 0 {
			t.Errorf("Expected 0 states explored due to immediate quorum pruning, got %d", res.StatesExplored)
		}
		t.Logf("Immediate quorum pruning in BnB verified: status=%v, message=%s", res.Status.Code(), res.Status.Message())
	})

	t.Run("Suffix node slot pruning during search recursion", func(t *testing.T) {
		// Gang of 3 pods, NodeMaxPods is 1 per node across 3 nodes.
		// node-0 can only hold pod 0 or pod 1.
		// node-1 is excluded for pod 2.
		p0 := makeTestPod("p0", "default", 1000, 1000, 1024)
		p1 := makeTestPod("p1", "default", 1000, 1000, 1024)
		p2 := makeTestPod("p2", "default", 1000, 1000, 1024)

		prob := &GangPlacementProblem{
			PreemptorPods:  []*v1.Pod{p0, p1, p2},
			CandidateNodes: []string{"node-0", "node-1", "node-2"},
			NodeAllocatableCPU: map[string]int64{"node-0": 2000, "node-1": 2000, "node-2": 2000},
			NodeAllocatableMem: map[string]int64{"node-0": 2048, "node-1": 2048, "node-2": 2048},
			NodeMaxPods:        map[string]int{"node-0": 1, "node-1": 1, "node-2": 1},
			PerPodCandidates: [][]GangPodCandidate{
				{
					{NodeName: "node-0", Cost: ZeroPreemptionCost(), Feasible: true, RequiredMilliCPU: 1000, RequiredMemory: 1024},
					{NodeName: "node-1", Cost: ZeroPreemptionCost(), Feasible: true, RequiredMilliCPU: 1000, RequiredMemory: 1024},
				},
				{
					{NodeName: "node-0", Cost: ZeroPreemptionCost(), Feasible: true, RequiredMilliCPU: 1000, RequiredMemory: 1024},
					{NodeName: "node-1", Cost: ZeroPreemptionCost(), Feasible: true, RequiredMilliCPU: 1000, RequiredMemory: 1024},
				},
				{
					{NodeName: "node-2", Cost: ZeroPreemptionCost(), Feasible: true, RequiredMilliCPU: 1000, RequiredMemory: 1024},
				},
			},
			MinMember: 3,
		}

		opts := DefaultBranchAndBoundOptions()
		opts.EnableSuffixQuorumPruning = true
		res := BranchAndBoundGangSearch(ctx, prob, opts)
		if !res.Status.IsSuccess() {
			t.Fatalf("Expected success for valid 3-pod placement, got: %v", res.Status.Message())
		}
		t.Logf("BnB suffix quorum pruning search finished successfully: states=%d, pruned=%d", res.StatesExplored, res.BranchesPruned)
	})
}
