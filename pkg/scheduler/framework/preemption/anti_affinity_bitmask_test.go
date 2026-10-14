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
	"context"
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/interpodaffinity"
)

func createMutualAntiAffinityPod(name string, gangLabel string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			UID:       types.UID(name + "-uid"),
			Labels: map[string]string{
				"app": gangLabel,
			},
		},
		Spec: v1.PodSpec{
			Affinity: &v1.Affinity{
				PodAntiAffinity: &v1.PodAntiAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
						{
							LabelSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									"app": gangLabel,
								},
							},
							TopologyKey: v1.LabelHostname,
						},
					},
				},
			},
		},
	}
}

// TestBranchAndBound_AntiAffinityEquivalenceWithExhaustive verifies mathematical equivalence
// between Exhaustive search and Branch-and-Bound search under strict pod anti-affinity.
func TestBranchAndBound_AntiAffinityEquivalenceWithExhaustive(t *testing.T) {
	gangSize := 4
	gangPods := make([]*v1.Pod, gangSize)
	for i := 0; i < gangSize; i++ {
		gangPods[i] = createMutualAntiAffinityPod(fmt.Sprintf("gang-pod-%d", i), "distributed-dnn")
	}

	nodes := []string{"node-0", "node-1", "node-2", "node-3"}
	matrix, err := interpodaffinity.NewInterPodAntiAffinityBitmaskMatrix(gangPods, nodes, nil, v1.LabelHostname)
	if err != nil {
		t.Fatalf("failed to create matrix: %v", err)
	}

	// Create candidate nodes with varied victim costs
	perPodCandidates := make([][]GangPodCandidate, gangSize)
	for i := 0; i < gangSize; i++ {
		perPodCandidates[i] = make([]GangPodCandidate, len(nodes))
		for nIdx, n := range nodes {
			victimPriority := int32((nIdx + 1) * 10)
			vPod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("victim-%s", n),
					Namespace: "default",
					UID:       types.UID(fmt.Sprintf("victim-%s-uid", n)),
				},
				Spec: v1.PodSpec{Priority: &victimPriority},
			}
			perPodCandidates[i][nIdx] = GangPodCandidate{
				NodeName:         n,
				Feasible:         true,
				Victims:          []*v1.Pod{vPod},
				Cost:             ComputeVictimsCost([]*v1.Pod{vPod}, 0),
				RequiredMilliCPU: 1000,
				RequiredMemory:   1024,
			}
		}
	}

	prob := &GangPlacementProblem{
		PreemptorPods:       gangPods,
		CandidateNodes:      nodes,
		PerPodCandidates:    perPodCandidates,
		AntiAffinityMatrix:  matrix,
		MinMember:           gangSize,
	}

	// 1. Run Baseline Exhaustive Search
	exhaustiveResult := ExhaustiveGangSearch(context.Background(), prob)
	if !exhaustiveResult.Status.IsSuccess() {
		t.Fatalf("Exhaustive search failed: %v", exhaustiveResult.Status.Message())
	}

	// 2. Run Branch-and-Bound with Greedy
	bnbOpts := DefaultBranchAndBoundOptions()
	bnbResult := BranchAndBoundGangSearch(context.Background(), prob, bnbOpts)
	if !bnbResult.Status.IsSuccess() {
		t.Fatalf("BnB search failed: %v", bnbResult.Status.Message())
	}

	// 3. Run Branch-and-Bound without Greedy (pure pruning)
	bnbNoGreedyOpts := BranchAndBoundOptions{
		EnableEarlyExit:         true,
		UseGreedyUpperBound:     false,
		UseBestFirstBranchOrder: true,
	}
	bnbNoGreedyResult := BranchAndBoundGangSearch(context.Background(), prob, bnbNoGreedyOpts)
	if !bnbNoGreedyResult.Status.IsSuccess() {
		t.Fatalf("BnB (No Greedy) search failed: %v", bnbNoGreedyResult.Status.Message())
	}

	// Verify exact cost equivalence
	if !exhaustiveResult.TotalCost.Equal(bnbResult.TotalCost) {
		t.Errorf("Cost mismatch: Exhaustive=%+v, BnB=%+v", exhaustiveResult.TotalCost, bnbResult.TotalCost)
	}
	if !exhaustiveResult.TotalCost.Equal(bnbNoGreedyResult.TotalCost) {
		t.Errorf("Cost mismatch: Exhaustive=%+v, BnB(NoGreedy)=%+v", exhaustiveResult.TotalCost, bnbNoGreedyResult.TotalCost)
	}

	// Verify each pod is placed on a unique node (no anti-affinity violation)
	assignedNodes := make(map[string]bool)
	for podUID, nodeName := range bnbResult.PodAssignments {
		if assignedNodes[nodeName] {
			t.Errorf("Anti-affinity violation: multiple pods placed on node %s (pod %s)", nodeName, podUID)
		}
		assignedNodes[nodeName] = true
	}
	if len(assignedNodes) != gangSize {
		t.Errorf("Expected %d unique nodes, got %d", gangSize, len(assignedNodes))
	}
}

// TestBranchAndBound_AntiAffinityPigeonholeInfeasible tests that when nodes < gangSize,
// mutual anti-affinity makes placement impossible and search correctly returns Unschedulable.
func TestBranchAndBound_AntiAffinityPigeonholeInfeasible(t *testing.T) {
	gangSize := 4
	gangPods := make([]*v1.Pod, gangSize)
	for i := 0; i < gangSize; i++ {
		gangPods[i] = createMutualAntiAffinityPod(fmt.Sprintf("gang-pod-%d", i), "pigeonhole-test")
	}

	// Only 3 nodes for 4 mutual anti-affinity pods
	nodes := []string{"node-0", "node-1", "node-2"}
	matrix, err := interpodaffinity.NewInterPodAntiAffinityBitmaskMatrix(gangPods, nodes, nil, v1.LabelHostname)
	if err != nil {
		t.Fatalf("failed to create matrix: %v", err)
	}

	perPodCandidates := make([][]GangPodCandidate, gangSize)
	for i := 0; i < gangSize; i++ {
		perPodCandidates[i] = make([]GangPodCandidate, len(nodes))
		for nIdx, n := range nodes {
			perPodCandidates[i][nIdx] = GangPodCandidate{
				NodeName: n,
				Feasible: true,
				Cost:     ZeroPreemptionCost(),
			}
		}
	}

	prob := &GangPlacementProblem{
		PreemptorPods:       gangPods,
		CandidateNodes:      nodes,
		PerPodCandidates:    perPodCandidates,
		AntiAffinityMatrix:  matrix,
		MinMember:           gangSize,
	}

	exRes := ExhaustiveGangSearch(context.Background(), prob)
	if exRes.Status.IsSuccess() {
		t.Errorf("Expected exhaustive search to fail on pigeonhole infeasible instance")
	}

	bnbRes := BranchAndBoundGangSearch(context.Background(), prob, DefaultBranchAndBoundOptions())
	if bnbRes.Status.IsSuccess() {
		t.Errorf("Expected BnB search to fail on pigeonhole infeasible instance")
	}
}

// TestBranchAndBound_AntiAffinityExistingPods verifies that existing pods on candidate nodes
// correctly pre-populate the bitmask matrix and reject placement on occupied nodes.
func TestBranchAndBound_AntiAffinityExistingPods(t *testing.T) {
	gangSize := 2
	gangPods := make([]*v1.Pod, gangSize)
	for i := 0; i < gangSize; i++ {
		gangPods[i] = createMutualAntiAffinityPod(fmt.Sprintf("pod-%d", i), "isolated-service")
	}

	nodes := []string{"node-0", "node-1", "node-occupied"}
	existingPods := map[string][]*v1.Pod{
		"node-occupied": {
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "existing-conflicting-pod",
					Namespace: "default",
					Labels: map[string]string{
						"app": "isolated-service",
					},
				},
			},
		},
	}

	matrix, err := interpodaffinity.NewInterPodAntiAffinityBitmaskMatrix(gangPods, nodes, existingPods, v1.LabelHostname)
	if err != nil {
		t.Fatalf("failed to create matrix: %v", err)
	}

	perPodCandidates := make([][]GangPodCandidate, gangSize)
	for i := 0; i < gangSize; i++ {
		perPodCandidates[i] = make([]GangPodCandidate, len(nodes))
		for nIdx, n := range nodes {
			perPodCandidates[i][nIdx] = GangPodCandidate{
				NodeName: n,
				Feasible: true,
				Cost:     ZeroPreemptionCost(),
			}
		}
	}

	prob := &GangPlacementProblem{
		PreemptorPods:       gangPods,
		CandidateNodes:      nodes,
		PerPodCandidates:    perPodCandidates,
		AntiAffinityMatrix:  matrix,
		MinMember:           gangSize,
	}

	bnbRes := BranchAndBoundGangSearch(context.Background(), prob, DefaultBranchAndBoundOptions())
	if !bnbRes.Status.IsSuccess() {
		t.Fatalf("expected BnB to succeed placing 2 pods on the 2 clean nodes: %v", bnbRes.Status.Message())
	}

	for _, nodeName := range bnbRes.PodAssignments {
		if nodeName == "node-occupied" {
			t.Errorf("pod placed on node-occupied which was rejected by bitmask matrix snapshot!")
		}
	}
}
