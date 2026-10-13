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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestFabricDistance_Tiers(t *testing.T) {
	tests := []struct {
		name     string
		c1       NetworkTopologyCoordinate
		c2       NetworkTopologyCoordinate
		wantDist int
	}{
		{
			name: "Same node",
			c1:   NetworkTopologyCoordinate{NodeName: "n1", LeafRack: "leaf-1", SpinePod: "pod-1", DataCenter: "dc-1"},
			c2:   NetworkTopologyCoordinate{NodeName: "n1", LeafRack: "leaf-1", SpinePod: "pod-1", DataCenter: "dc-1"},
			wantDist: HopSameNode,
		},
		{
			name: "Same Leaf / TOR rack",
			c1:   NetworkTopologyCoordinate{NodeName: "n1", LeafRack: "leaf-1", SpinePod: "pod-1", DataCenter: "dc-1"},
			c2:   NetworkTopologyCoordinate{NodeName: "n2", LeafRack: "leaf-1", SpinePod: "pod-1", DataCenter: "dc-1"},
			wantDist: HopSameLeaf,
		},
		{
			name: "Same Spine / Network Pod across leaves",
			c1:   NetworkTopologyCoordinate{NodeName: "n1", LeafRack: "leaf-1", SpinePod: "pod-1", DataCenter: "dc-1"},
			c2:   NetworkTopologyCoordinate{NodeName: "n2", LeafRack: "leaf-2", SpinePod: "pod-1", DataCenter: "dc-1"},
			wantDist: HopSameSpine,
		},
		{
			name: "Cross Spine Pods in same DC",
			c1:   NetworkTopologyCoordinate{NodeName: "n1", LeafRack: "leaf-1", SpinePod: "pod-1", DataCenter: "dc-1"},
			c2:   NetworkTopologyCoordinate{NodeName: "n2", LeafRack: "leaf-3", SpinePod: "pod-2", DataCenter: "dc-1"},
			wantDist: HopCrossFabric,
		},
		{
			name: "Cross DataCenter",
			c1:   NetworkTopologyCoordinate{NodeName: "n1", LeafRack: "leaf-1", SpinePod: "pod-1", DataCenter: "dc-1"},
			c2:   NetworkTopologyCoordinate{NodeName: "n2", LeafRack: "leaf-1", SpinePod: "pod-1", DataCenter: "dc-2"},
			wantDist: HopCrossFabric,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := FabricDistance(tc.c1, tc.c2)
			if got != tc.wantDist {
				t.Errorf("FabricDistance = %d, want %d", got, tc.wantDist)
			}
			// Symmetric property: d(a, b) == d(b, a)
			gotRev := FabricDistance(tc.c2, tc.c1)
			if gotRev != tc.wantDist {
				t.Errorf("FabricDistance(c2, c1) = %d, want %d", gotRev, tc.wantDist)
			}
		})
	}
}

func TestExtractNetworkCoordinate(t *testing.T) {
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-gpu-42",
			Labels: map[string]string{
				LabelTopologyDataCenter: "us-central1-a",
				LabelTopologySpinePod:   "spine-pod-3",
				LabelTopologyLeafRack:   "tor-rack-7",
			},
		},
	}
	coord := ExtractNetworkCoordinate(node)
	if coord.NodeName != "node-gpu-42" {
		t.Errorf("NodeName = %s, want node-gpu-42", coord.NodeName)
	}
	if coord.DataCenter != "us-central1-a" {
		t.Errorf("DataCenter = %s, want us-central1-a", coord.DataCenter)
	}
	if coord.SpinePod != "spine-pod-3" {
		t.Errorf("SpinePod = %s, want spine-pod-3", coord.SpinePod)
	}
	if coord.LeafRack != "tor-rack-7" {
		t.Errorf("LeafRack = %s, want tor-rack-7", coord.LeafRack)
	}
}

func TestFabricDistanceMatrix(t *testing.T) {
	nodes := []string{"n0", "n1", "n2", "n3"}
	coords := map[string]NetworkTopologyCoordinate{
		"n0": {NodeName: "n0", LeafRack: "leaf-0", SpinePod: "spine-0", DataCenter: "dc-0"},
		"n1": {NodeName: "n1", LeafRack: "leaf-0", SpinePod: "spine-0", DataCenter: "dc-0"},
		"n2": {NodeName: "n2", LeafRack: "leaf-1", SpinePod: "spine-0", DataCenter: "dc-0"},
		"n3": {NodeName: "n3", LeafRack: "leaf-2", SpinePod: "spine-1", DataCenter: "dc-0"},
	}

	matrix := NewFabricDistanceMatrix(nodes, coords)
	if matrix.Distance("n0", "n0") != HopSameNode {
		t.Errorf("n0-n0 = %d, want %d", matrix.Distance("n0", "n0"), HopSameNode)
	}
	if matrix.Distance("n0", "n1") != HopSameLeaf {
		t.Errorf("n0-n1 = %d, want %d", matrix.Distance("n0", "n1"), HopSameLeaf)
	}
	if matrix.Distance("n0", "n2") != HopSameSpine {
		t.Errorf("n0-n2 = %d, want %d", matrix.Distance("n0", "n2"), HopSameSpine)
	}
	if matrix.Distance("n0", "n3") != HopCrossFabric {
		t.Errorf("n0-n3 = %d, want %d", matrix.Distance("n0", "n3"), HopCrossFabric)
	}
	if matrix.Distance("n2", "n3") != HopCrossFabric {
		t.Errorf("n2-n3 = %d, want %d", matrix.Distance("n2", "n3"), HopCrossFabric)
	}
}

// TestNetworkTopologyDiameterBounding_EquivalenceWithExhaustive ensures that Branch-and-Bound
// with network diameter bounding sphere pruning yields exact equivalence to Exhaustive search.
func TestNetworkTopologyDiameterBounding_EquivalenceWithExhaustive(t *testing.T) {
	ctx := context.Background()
	gangSize := 4
	nodesCount := 8

	gangPods := make([]*v1.Pod, gangSize)
	for i := 0; i < gangSize; i++ {
		gangPods[i] = makeTestPod(fmt.Sprintf("gang-pod-%d", i), "default", 1000, 1000, 1024)
	}

	nodeNames := make([]string, nodesCount)
	nodeAllocCPU := make(map[string]int64)
	nodeAllocMem := make(map[string]int64)
	coords := make(map[string]NetworkTopologyCoordinate)

	// Nodes 0..3 on leaf-0 (spine-0)
	// Nodes 4..5 on leaf-1 (spine-0)
	// Nodes 6..7 on leaf-2 (spine-1)
	for j := 0; j < nodesCount; j++ {
		name := fmt.Sprintf("node-%d", j)
		nodeNames[j] = name
		nodeAllocCPU[name] = 16000
		nodeAllocMem[name] = 32768

		var leaf, spine string
		if j < 4 {
			leaf = "leaf-0"
			spine = "spine-0"
		} else if j < 6 {
			leaf = "leaf-1"
			spine = "spine-0"
		} else {
			leaf = "leaf-2"
			spine = "spine-1"
		}
		coords[name] = NetworkTopologyCoordinate{
			NodeName:   name,
			LeafRack:   leaf,
			SpinePod:   spine,
			DataCenter: "dc-1",
		}
	}

	perPodCandidates := make([][]GangPodCandidate, gangSize)
	for i := 0; i < gangSize; i++ {
		cands := make([]GangPodCandidate, nodesCount)
		for j := 0; j < nodesCount; j++ {
			prio := int32((j % 3) * 100)
			pdb := 0
			if j%5 == 0 {
				pdb = 1
			}
			victims := []*v1.Pod{
				makeTestPod(fmt.Sprintf("victim-%d-%d", i, j), "default", prio, 1000, 1024),
			}
			cands[j] = GangPodCandidate{
				NodeName:         nodeNames[j],
				Cost:             ComputeVictimsCost(victims, pdb),
				Victims:          victims,
				PDBViolations:    pdb,
				Feasible:         true,
				RequiredMilliCPU: 1000,
				RequiredMemory:   1024,
			}
		}
		perPodCandidates[i] = cands
	}

	// Test maxDiameter = 1 (Same leaf switch only)
	boundingSpecLeaf := NewCollectiveCommunicationBoundingSpec(HopSameLeaf, coords, nodeNames)
	probLeaf := &GangPlacementProblem{
		PreemptorPods:                   gangPods,
		CandidateNodes:                  nodeNames,
		NodeAllocatableCPU:              nodeAllocCPU,
		NodeAllocatableMem:              nodeAllocMem,
		PerPodCandidates:                perPodCandidates,
		CollectiveCommunicationBounding: boundingSpecLeaf,
		MinMember:                       gangSize,
	}

	baselineResLeaf := ExhaustiveGangSearch(ctx, probLeaf)
	if !baselineResLeaf.Status.IsSuccess() {
		t.Fatalf("Exhaustive search failed: %v", baselineResLeaf.Status)
	}

	bnbResLeaf := BranchAndBoundGangSearch(ctx, probLeaf, DefaultBranchAndBoundOptions())
	if !bnbResLeaf.Status.IsSuccess() {
		t.Fatalf("Branch and Bound search failed: %v", bnbResLeaf.Status)
	}

	if !bnbResLeaf.TotalCost.Equal(baselineResLeaf.TotalCost) {
		t.Errorf("Cost mismatch: BnB=%+v vs Baseline=%+v", bnbResLeaf.TotalCost, baselineResLeaf.TotalCost)
	}
	if bnbResLeaf.CollectiveDiameter > HopSameLeaf {
		t.Errorf("Expected diameter <= %d, got %d", HopSameLeaf, bnbResLeaf.CollectiveDiameter)
	}
	t.Logf("Leaf bounding verified: Diameter=%d, BnB Explored=%d, Pruned=%d",
		bnbResLeaf.CollectiveDiameter, bnbResLeaf.StatesExplored, bnbResLeaf.BranchesPruned)

	// Test maxDiameter = 2 (Same spine switch pod only)
	boundingSpecSpine := NewCollectiveCommunicationBoundingSpec(HopSameSpine, coords, nodeNames)
	probSpine := &GangPlacementProblem{
		PreemptorPods:                   gangPods,
		CandidateNodes:                  nodeNames,
		NodeAllocatableCPU:              nodeAllocCPU,
		NodeAllocatableMem:              nodeAllocMem,
		PerPodCandidates:                perPodCandidates,
		CollectiveCommunicationBounding: boundingSpecSpine,
		MinMember:                       gangSize,
	}

	baselineResSpine := ExhaustiveGangSearch(ctx, probSpine)
	bnbResSpine := BranchAndBoundGangSearch(ctx, probSpine, DefaultBranchAndBoundOptions())
	if !bnbResSpine.TotalCost.Equal(baselineResSpine.TotalCost) {
		t.Errorf("Cost mismatch for spine bounding: BnB=%+v vs Baseline=%+v", bnbResSpine.TotalCost, baselineResSpine.TotalCost)
	}
	if bnbResSpine.CollectiveDiameter > HopSameSpine {
		t.Errorf("Expected diameter <= %d, got %d", HopSameSpine, bnbResSpine.CollectiveDiameter)
	}
	t.Logf("Spine bounding verified: Diameter=%d, BnB Explored=%d, Pruned=%d",
		bnbResSpine.CollectiveDiameter, bnbResSpine.StatesExplored, bnbResSpine.BranchesPruned)
}

// TestNetworkTopologyDiameterBounding_PrunesCrossFabricViolations verifies that when
// candidate nodes across high-latency spine hops exist, the bounding sphere strictly prunes them.
func TestNetworkTopologyDiameterBounding_PrunesCrossFabricViolations(t *testing.T) {
	ctx := context.Background()
	gangSize := 3

	gangPods := make([]*v1.Pod, gangSize)
	for i := 0; i < gangSize; i++ {
		gangPods[i] = makeTestPod(fmt.Sprintf("pod-%d", i), "default", 1000, 1000, 1024)
	}

	// 3 nodes on leaf-A (spine-0), 3 nodes on leaf-B (spine-1)
	nodeNames := []string{"n0", "n1", "n2", "n3", "n4", "n5"}
	coords := map[string]NetworkTopologyCoordinate{
		"n0": {NodeName: "n0", LeafRack: "leaf-A", SpinePod: "spine-0", DataCenter: "dc-1"},
		"n1": {NodeName: "n1", LeafRack: "leaf-A", SpinePod: "spine-0", DataCenter: "dc-1"},
		"n2": {NodeName: "n2", LeafRack: "leaf-A", SpinePod: "spine-0", DataCenter: "dc-1"},
		"n3": {NodeName: "n3", LeafRack: "leaf-B", SpinePod: "spine-1", DataCenter: "dc-1"},
		"n4": {NodeName: "n4", LeafRack: "leaf-B", SpinePod: "spine-1", DataCenter: "dc-1"},
		"n5": {NodeName: "n5", LeafRack: "leaf-B", SpinePod: "spine-1", DataCenter: "dc-1"},
	}

	// Give leaf-B lower cost to tempt solver across spine hops
	perPodCandidates := make([][]GangPodCandidate, gangSize)
	for i := 0; i < gangSize; i++ {
		cands := make([]GangPodCandidate, len(nodeNames))
		for j, nName := range nodeNames {
			prio := int32(500)
			if j >= 3 {
				prio = int32(10) // Tempting low-cost victim on distant spine!
			}
			victims := []*v1.Pod{
				makeTestPod(fmt.Sprintf("v-%d-%s", i, nName), "default", prio, 1000, 1024),
			}
			cands[j] = GangPodCandidate{
				NodeName:         nName,
				Cost:             ComputeVictimsCost(victims, 0),
				Victims:          victims,
				Feasible:         true,
				RequiredMilliCPU: 1000,
				RequiredMemory:   1024,
			}
		}
		perPodCandidates[i] = cands
	}

	boundingSpec := NewCollectiveCommunicationBoundingSpec(HopSameLeaf, coords, nodeNames)
	prob := &GangPlacementProblem{
		PreemptorPods:                   gangPods,
		CandidateNodes:                  nodeNames,
		PerPodCandidates:                perPodCandidates,
		CollectiveCommunicationBounding: boundingSpec,
		MinMember:                       gangSize,
	}

	opts := BranchAndBoundOptions{
		EnableEarlyExit:         false,
		UseGreedyUpperBound:     false,
		UseBestFirstBranchOrder: false,
		MaxExploredStates:       50000,
	}
	res := BranchAndBoundGangSearch(ctx, prob, opts)
	if !res.Status.IsSuccess() {
		t.Fatalf("Search failed: %v", res.Status)
	}

	if res.CollectiveDiameter > HopSameLeaf {
		t.Errorf("CollectiveDiameter = %d, expected <= %d", res.CollectiveDiameter, HopSameLeaf)
	}
	if res.BranchesPruned == 0 {
		t.Errorf("Expected branchesPruned > 0 from network bounding sphere, got %d", res.BranchesPruned)
	}
	t.Logf("Cross fabric pruning verified: Explored=%d, Pruned=%d, CollectiveDiameter=%d",
		res.StatesExplored, res.BranchesPruned, res.CollectiveDiameter)
}

// TestNetworkTopologyDiameterBounding_InfeasibleWhenNoBoundingSphereCoversGang verifies that
// if the cluster cannot place the gang within the bounding sphere diameter, Unschedulable is returned.
func TestNetworkTopologyDiameterBounding_InfeasibleWhenNoBoundingSphereCoversGang(t *testing.T) {
	ctx := context.Background()
	gangSize := 4

	gangPods := make([]*v1.Pod, gangSize)
	for i := 0; i < gangSize; i++ {
		gangPods[i] = makeTestPod(fmt.Sprintf("pod-%d", i), "default", 1000, 1000, 1024)
	}

	// 4 nodes, each on a completely different spine switch
	nodeNames := []string{"n0", "n1", "n2", "n3"}
	coords := map[string]NetworkTopologyCoordinate{
		"n0": {NodeName: "n0", LeafRack: "leaf-0", SpinePod: "spine-0", DataCenter: "dc-1"},
		"n1": {NodeName: "n1", LeafRack: "leaf-1", SpinePod: "spine-1", DataCenter: "dc-1"},
		"n2": {NodeName: "n2", LeafRack: "leaf-2", SpinePod: "spine-2", DataCenter: "dc-1"},
		"n3": {NodeName: "n3", LeafRack: "leaf-3", SpinePod: "spine-3", DataCenter: "dc-1"},
	}

	// Each node can only host 1 pod
	nodeMaxPods := map[string]int{
		"n0": 1,
		"n1": 1,
		"n2": 1,
		"n3": 1,
	}

	perPodCandidates := make([][]GangPodCandidate, gangSize)
	for i := 0; i < gangSize; i++ {
		cands := make([]GangPodCandidate, len(nodeNames))
		for j, nName := range nodeNames {
			victims := []*v1.Pod{
				makeTestPod(fmt.Sprintf("v-%d-%s", i, nName), "default", 100, 1000, 1024),
			}
			cands[j] = GangPodCandidate{
				NodeName:         nName,
				Cost:             ComputeVictimsCost(victims, 0),
				Victims:          victims,
				Feasible:         true,
				RequiredMilliCPU: 1000,
				RequiredMemory:   1024,
			}
		}
		perPodCandidates[i] = cands
	}

	// Max allowed diameter = HopSameSpine (2), but nodes are on spine-0, 1, 2, 3 (distance = 3)
	boundingSpec := NewCollectiveCommunicationBoundingSpec(HopSameSpine, coords, nodeNames)
	prob := &GangPlacementProblem{
		PreemptorPods:                   gangPods,
		CandidateNodes:                  nodeNames,
		NodeMaxPods:                     nodeMaxPods,
		PerPodCandidates:                perPodCandidates,
		CollectiveCommunicationBounding: boundingSpec,
		MinMember:                       gangSize,
	}

	res := BranchAndBoundGangSearch(ctx, prob, DefaultBranchAndBoundOptions())
	if res.Status.IsSuccess() {
		t.Fatalf("Expected Unschedulable due to network diameter violation, but search succeeded: %+v", res)
	}
	t.Logf("Correctly reported unschedulable: status=%v", res.Status.Message())
}
