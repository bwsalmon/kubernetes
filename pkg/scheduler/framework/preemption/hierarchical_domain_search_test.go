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
)

// TestHierarchicalDomainSearch_MultiZonePruning tests pruning of non-viable failure domains
// and bit-for-bit equivalence with unpruned BranchAndBound search.
func TestHierarchicalDomainSearch_MultiZonePruning(t *testing.T) {
	ctx := context.Background()

	// 16 pods gang
	gangSize := 16
	gangPods := make([]*v1.Pod, gangSize)
	for i := 0; i < gangSize; i++ {
		gangPods[i] = makeTestPod(fmt.Sprintf("gang-pod-%d", i), "default", 1000, 1000, 1024)
	}

	// 3 zones: zone-a (8 nodes), zone-b (8 nodes), zone-c (8 nodes) = 24 nodes total
	// zone-a and zone-b have viable nodes.
	// zone-c has 0 capacity for these pods (e.g. all nodes full or infeasible).
	nodeNames := make([]string, 24)
	nodeAllocCPU := make(map[string]int64)
	nodeAllocMem := make(map[string]int64)
	nodeDomains := make(map[string]string)

	for j := 0; j < 24; j++ {
		name := fmt.Sprintf("node-%02d", j)
		nodeNames[j] = name
		var zone string
		if j < 8 {
			zone = "zone-a"
			nodeAllocCPU[name] = 8000
			nodeAllocMem[name] = 8192
		} else if j < 16 {
			zone = "zone-b"
			nodeAllocCPU[name] = 8000
			nodeAllocMem[name] = 8192
		} else {
			zone = "zone-c"
			nodeAllocCPU[name] = 0 // Infeasible in zone-c
			nodeAllocMem[name] = 0
		}
		nodeDomains[name] = zone
	}

	perPodCandidates := make([][]GangPodCandidate, gangSize)
	for i := 0; i < gangSize; i++ {
		cands := make([]GangPodCandidate, 24)
		for j := 0; j < 24; j++ {
			feasible := j < 16
			prio := int32((j % 4) * 10)
			victims := []*v1.Pod{}
			if feasible {
				victims = append(victims, makeTestPod(fmt.Sprintf("v-%d-%d", i, j), "default", prio, 1000, 1024))
			}
			cost := ComputeVictimsCost(victims, 0)
			cands[j] = GangPodCandidate{
				NodeName:         nodeNames[j],
				Cost:             cost,
				Victims:          victims,
				Feasible:         feasible,
				RequiredMilliCPU: 1000,
				RequiredMemory:   1024,
			}
		}
		perPodCandidates[i] = cands
	}

	constraint := TopologySpreadConstraint{
		TopologyKey: "topology.kubernetes.io/zone",
		MaxSkew:     8,
		NodeDomains: nodeDomains,
	}

	prob := &GangPlacementProblem{
		PreemptorPods:       gangPods,
		CandidateNodes:      nodeNames,
		NodeAllocatableCPU:  nodeAllocCPU,
		NodeAllocatableMem:  nodeAllocMem,
		PerPodCandidates:    perPodCandidates,
		TopologyConstraints: []TopologySpreadConstraint{constraint},
		MinMember:           gangSize,
	}

	// 1. Run Baseline BranchAndBound without domain pruning
	bnbStart := DefaultBranchAndBoundOptions()
	baselineRes := BranchAndBoundGangSearch(ctx, prob, bnbStart)
	if !baselineRes.Status.IsSuccess() {
		t.Fatalf("Baseline BnB search failed: %v", baselineRes.Status.Message())
	}

	// 2. Run Hierarchical Domain Search
	hOpts := DefaultHierarchicalDomainSearchOptions()
	hRes := HierarchicalDomainGangSearch(ctx, prob, constraint, hOpts)
	if !hRes.PlacementResult.Status.IsSuccess() {
		t.Fatalf("Hierarchical search failed: %v", hRes.PlacementResult.Status.Message())
	}

	// 3. Verify zone-c was pruned
	if hRes.PrunedDomains != 1 {
		t.Errorf("Expected 1 pruned domain (zone-c), got %d pruned domains", hRes.PrunedDomains)
	}
	if hRes.PrunedNodes != 8 {
		t.Errorf("Expected 8 pruned nodes from zone-c, got %d pruned nodes", hRes.PrunedNodes)
	}

	// 4. Verify 100% Bit-for-Bit Decision Fidelity with Baseline
	if !hRes.PlacementResult.TotalCost.Equal(baselineRes.TotalCost) {
		t.Errorf("Cost mismatch: Hierarchical %+v vs Baseline %+v", hRes.PlacementResult.TotalCost, baselineRes.TotalCost)
	}
	if len(hRes.PlacementResult.SelectedVictims) != len(baselineRes.SelectedVictims) {
		t.Errorf("Selected victims count mismatch: Hierarchical %d vs Baseline %d",
			len(hRes.PlacementResult.SelectedVictims), len(baselineRes.SelectedVictims))
	}

	t.Logf("MultiZonePruning Success: TotalDomains=%d, ViableDomains=%d, PrunedDomains=%d, PrunedNodes=%d, Duration=%v",
		hRes.TotalDomains, hRes.ViableDomains, hRes.PrunedDomains, hRes.PrunedNodes, hRes.TotalDuration)
}

// TestHierarchicalDomainSearch_CoLocationRackQuorumPruning tests gang rack co-location requirements.
// If a rack cannot hold MinMember pods, it is completely pruned prior to node-level search.
func TestHierarchicalDomainSearch_CoLocationRackQuorumPruning(t *testing.T) {
	ctx := context.Background()

	gangSize := 8
	gangPods := make([]*v1.Pod, gangSize)
	for i := 0; i < gangSize; i++ {
		gangPods[i] = makeTestPod(fmt.Sprintf("rack-gang-pod-%d", i), "default", 1000, 1000, 1024)
	}

	// 4 Racks: rack-1 (4 nodes, fits 4 pods), rack-2 (4 nodes, fits 4 pods),
	// rack-3 (8 nodes, fits 8 pods), rack-4 (8 nodes, fits 8 pods)
	// For a gang of 8 requiring co-location, rack-1 and rack-2 must be mathematically pruned as a whole!
	nodeNames := make([]string, 24)
	nodeAllocCPU := make(map[string]int64)
	nodeAllocMem := make(map[string]int64)
	nodeDomains := make(map[string]string)

	for j := 0; j < 24; j++ {
		name := fmt.Sprintf("node-r-%02d", j)
		nodeNames[j] = name
		var rack string
		if j < 4 {
			rack = "rack-1" // max 4 pods
			nodeAllocCPU[name] = 1000
			nodeAllocMem[name] = 1024
		} else if j < 8 {
			rack = "rack-2" // max 4 pods
			nodeAllocCPU[name] = 1000
			nodeAllocMem[name] = 1024
		} else if j < 16 {
			rack = "rack-3" // fits 8 pods
			nodeAllocCPU[name] = 1000
			nodeAllocMem[name] = 1024
		} else {
			rack = "rack-4" // fits 8 pods
			nodeAllocCPU[name] = 1000
			nodeAllocMem[name] = 1024
		}
		nodeDomains[name] = rack
	}

	perPodCandidates := make([][]GangPodCandidate, gangSize)
	for i := 0; i < gangSize; i++ {
		cands := make([]GangPodCandidate, 24)
		for j := 0; j < 24; j++ {
			prio := int32((j % 5) * 10)
			victims := []*v1.Pod{
				makeTestPod(fmt.Sprintf("v-r-%d-%d", i, j), "default", prio, 1000, 1024),
			}
			cost := ComputeVictimsCost(victims, 0)
			cands[j] = GangPodCandidate{
				NodeName:         nodeNames[j],
				Cost:             cost,
				Victims:          victims,
				Feasible:         true,
				RequiredMilliCPU: 1000,
				RequiredMemory:   1024,
			}
		}
		perPodCandidates[i] = cands
	}

	constraint := TopologySpreadConstraint{
		TopologyKey: "topology.kubernetes.io/rack",
		MaxSkew:     0,
		NodeDomains: nodeDomains,
	}

	prob := &GangPlacementProblem{
		PreemptorPods:       gangPods,
		CandidateNodes:      nodeNames,
		NodeAllocatableCPU:  nodeAllocCPU,
		NodeAllocatableMem:  nodeAllocMem,
		PerPodCandidates:    perPodCandidates,
		TopologyConstraints: []TopologySpreadConstraint{constraint},
		MinMember:           gangSize,
	}

	hOpts := DefaultHierarchicalDomainSearchOptions()
	hOpts.RequireCoLocation = true // Require co-location in single domain

	hRes := HierarchicalDomainGangSearch(ctx, prob, constraint, hOpts)
	if !hRes.PlacementResult.Status.IsSuccess() {
		t.Fatalf("Hierarchical search failed: %v", hRes.PlacementResult.Status.Message())
	}

	// Verify rack-1 and rack-2 were pruned (4 nodes each = 8 nodes pruned)
	if hRes.PrunedDomains != 2 {
		t.Errorf("Expected 2 pruned domains (rack-1 and rack-2), got %d pruned domains", hRes.PrunedDomains)
	}
	if hRes.PrunedNodes != 8 {
		t.Errorf("Expected 8 pruned nodes, got %d pruned nodes", hRes.PrunedNodes)
	}

	// Verify all assigned pods landed in the same viable rack (rack-3 or rack-4)
	var assignedRack string
	for _, assignedNode := range hRes.PlacementResult.PodAssignments {
		rack := nodeDomains[assignedNode]
		if assignedRack == "" {
			assignedRack = rack
		} else if assignedRack != rack {
			t.Errorf("Co-location violation: pods landed across multiple racks (%s and %s)", assignedRack, rack)
		}
	}
	if assignedRack != "rack-3" && assignedRack != "rack-4" {
		t.Errorf("Expected placement in rack-3 or rack-4, got %s", assignedRack)
	}

	t.Logf("CoLocationRackQuorumPruning Success: AssignedRack=%s, PrunedDomains=%d, PrunedNodes=%d",
		assignedRack, hRes.PrunedDomains, hRes.PrunedNodes)
}

// TestHierarchicalDomainSearch_ConservativeFidelity ensures that domain pruning NEVER
// discards a failure domain that contains valid feasible solutions.
func TestHierarchicalDomainSearch_ConservativeFidelity(t *testing.T) {
	ctx := context.Background()

	gangSize := 4
	gangPods := make([]*v1.Pod, gangSize)
	for i := 0; i < gangSize; i++ {
		gangPods[i] = makeTestPod(fmt.Sprintf("cons-pod-%d", i), "default", 1000, 1000, 1024)
	}

	// 3 Zones with marginal capacities:
	// zone-a: 2 nodes, 1 pod each = 2 pods
	// zone-b: 2 nodes, 1 pod each = 2 pods
	// zone-c: 1 node, 0 pods (infeasible)
	nodeNames := []string{"node-a1", "node-a2", "node-b1", "node-b2", "node-c1"}
	nodeAllocCPU := map[string]int64{
		"node-a1": 1000, "node-a2": 1000,
		"node-b1": 1000, "node-b2": 1000,
		"node-c1": 0,
	}
	nodeAllocMem := map[string]int64{
		"node-a1": 1024, "node-a2": 1024,
		"node-b1": 1024, "node-b2": 1024,
		"node-c1": 0,
	}
	nodeDomains := map[string]string{
		"node-a1": "zone-a", "node-a2": "zone-a",
		"node-b1": "zone-b", "node-b2": "zone-b",
		"node-c1": "zone-c",
	}

	perPodCandidates := make([][]GangPodCandidate, gangSize)
	for i := 0; i < gangSize; i++ {
		cands := make([]GangPodCandidate, len(nodeNames))
		for j, nName := range nodeNames {
			feasible := nName != "node-c1"
			victims := []*v1.Pod{}
			if feasible {
				victims = append(victims, makeTestPod(fmt.Sprintf("v-%d-%d", i, j), "default", 10, 1000, 1024))
			}
			cost := ComputeVictimsCost(victims, 0)
			cands[j] = GangPodCandidate{
				NodeName:         nName,
				Cost:             cost,
				Victims:          victims,
				Feasible:         feasible,
				RequiredMilliCPU: 1000,
				RequiredMemory:   1024,
			}
		}
		perPodCandidates[i] = cands
	}

	constraint := TopologySpreadConstraint{
		TopologyKey: "topology.kubernetes.io/zone",
		MaxSkew:     2,
		NodeDomains: nodeDomains,
	}

	prob := &GangPlacementProblem{
		PreemptorPods:       gangPods,
		CandidateNodes:      nodeNames,
		NodeAllocatableCPU:  nodeAllocCPU,
		NodeAllocatableMem:  nodeAllocMem,
		PerPodCandidates:    perPodCandidates,
		TopologyConstraints: []TopologySpreadConstraint{constraint},
		MinMember:           gangSize,
	}

	// Baseline Exhaustive Search
	baselineRes := ExhaustiveGangSearch(ctx, prob)
	if !baselineRes.Status.IsSuccess() {
		t.Fatalf("Exhaustive baseline failed: %v", baselineRes.Status.Message())
	}

	// Hierarchical Domain Search
	hRes := HierarchicalDomainGangSearch(ctx, prob, constraint, DefaultHierarchicalDomainSearchOptions())
	if !hRes.PlacementResult.Status.IsSuccess() {
		t.Fatalf("Hierarchical search failed: %v", hRes.PlacementResult.Status.Message())
	}

	// Verify exact equivalence
	if !hRes.PlacementResult.TotalCost.Equal(baselineRes.TotalCost) {
		t.Fatalf("Cost mismatch against exhaustive oracle! Hierarchical=%+v, Exhaustive=%+v",
			hRes.PlacementResult.TotalCost, baselineRes.TotalCost)
	}

	// Confirm zone-c was pruned, but zone-a and zone-b were preserved
	if hRes.PrunedDomains != 1 {
		t.Errorf("Expected 1 pruned domain, got %d", hRes.PrunedDomains)
	}
	if hRes.ViableDomains != 2 {
		t.Errorf("Expected 2 viable domains, got %d", hRes.ViableDomains)
	}

	t.Logf("ConservativeFidelity Verified: Bit-for-bit identical to Exhaustive Oracle.")
}

// TestHierarchicalDomainSearch_AllDomainsInfeasible tests immediate Unschedulable short-circuiting
// when no domains meet gang quorum.
func TestHierarchicalDomainSearch_AllDomainsInfeasible(t *testing.T) {
	ctx := context.Background()

	gangSize := 8
	gangPods := make([]*v1.Pod, gangSize)
	for i := 0; i < gangSize; i++ {
		gangPods[i] = makeTestPod(fmt.Sprintf("inf-pod-%d", i), "default", 1000, 1000, 1024)
	}

	nodeNames := []string{"node-1", "node-2"}
	nodeAllocCPU := map[string]int64{"node-1": 1000, "node-2": 1000}
	nodeAllocMem := map[string]int64{"node-1": 1024, "node-2": 1024}
	nodeDomains := map[string]string{"node-1": "zone-a", "node-2": "zone-b"}

	perPodCandidates := make([][]GangPodCandidate, gangSize)
	for i := 0; i < gangSize; i++ {
		perPodCandidates[i] = []GangPodCandidate{
			{NodeName: "node-1", Feasible: false},
			{NodeName: "node-2", Feasible: false},
		}
	}

	constraint := TopologySpreadConstraint{
		TopologyKey: "topology.kubernetes.io/zone",
		MaxSkew:     2,
		NodeDomains: nodeDomains,
	}

	prob := &GangPlacementProblem{
		PreemptorPods:       gangPods,
		CandidateNodes:      nodeNames,
		NodeAllocatableCPU:  nodeAllocCPU,
		NodeAllocatableMem:  nodeAllocMem,
		PerPodCandidates:    perPodCandidates,
		TopologyConstraints: []TopologySpreadConstraint{constraint},
		MinMember:           gangSize,
	}

	hRes := HierarchicalDomainGangSearch(ctx, prob, constraint, DefaultHierarchicalDomainSearchOptions())
	if hRes.PlacementResult.Status.IsSuccess() {
		t.Fatalf("Expected Unschedulable status, got success")
	}
	if hRes.PlacementResult.StatesExplored != 0 {
		t.Errorf("Expected 0 states explored when all domains pruned, got %d", hRes.PlacementResult.StatesExplored)
	}
	t.Logf("AllDomainsInfeasible short-circuited in %v with 0 states explored", hRes.TotalDuration)
}
