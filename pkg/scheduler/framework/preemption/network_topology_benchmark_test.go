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

// buildFatTreeBenchmarkProblem generates a 5,000+ node fat-tree cluster topology.
// For k=28: 28 pods, 14 leaves per pod, 14 hosts per leaf = 5,488 nodes total.
func buildFatTreeBenchmarkProblem(gangSize int, k int, candidatePoolSize int, maxDiameter int) *GangPlacementProblem {
	labelsMap := BuildFatTreeTopologyLabels(k) // ~5,488 nodes
	totalNodes := len(labelsMap)

	nodeNames := make([]string, 0, totalNodes)
	for name := range labelsMap {
		nodeNames = append(nodeNames, name)
	}

	// Subsample candidatePoolSize nodes across the fabric
	if candidatePoolSize > totalNodes {
		candidatePoolSize = totalNodes
	}
	selectedNodes := nodeNames[:candidatePoolSize]

	nodeCoords := make(map[string]NetworkTopologyCoordinate, candidatePoolSize)
	for _, n := range selectedNodes {
		l := labelsMap[n]
		nodeCoords[n] = NetworkTopologyCoordinate{
			NodeName:   n,
			DataCenter: l[LabelTopologyDataCenter],
			SpinePod:   l[LabelTopologySpinePod],
			LeafRack:   l[LabelTopologyLeafRack],
		}
	}

	gangPods := make([]*v1.Pod, gangSize)
	for i := 0; i < gangSize; i++ {
		gangPods[i] = makeTestPod(fmt.Sprintf("collective-pod-%d", i), "default", 1000, 1000, 1024)
	}

	nodeAllocCPU := make(map[string]int64, candidatePoolSize)
	nodeAllocMem := make(map[string]int64, candidatePoolSize)
	for _, n := range selectedNodes {
		nodeAllocCPU[n] = 32000
		nodeAllocMem[n] = 65536
	}

	perPodCandidates := make([][]GangPodCandidate, gangSize)
	for i := 0; i < gangSize; i++ {
		cands := make([]GangPodCandidate, candidatePoolSize)
		for j, n := range selectedNodes {
			prio := int32((j % 7) * 100)
			pdb := 0
			if j%6 == 0 {
				pdb = 1
			}
			victims := []*v1.Pod{
				makeTestPod(fmt.Sprintf("v-%d-%d", i, j), "default", prio, 1000, 1024),
			}
			cands[j] = GangPodCandidate{
				NodeName:         n,
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

	var boundingSpec *CollectiveCommunicationBoundingSpec
	if maxDiameter >= 0 {
		boundingSpec = NewCollectiveCommunicationBoundingSpec(maxDiameter, nodeCoords, selectedNodes)
	}

	return &GangPlacementProblem{
		PreemptorPods:                   gangPods,
		CandidateNodes:                  selectedNodes,
		NodeAllocatableCPU:              nodeAllocCPU,
		NodeAllocatableMem:              nodeAllocMem,
		PerPodCandidates:                perPodCandidates,
		CollectiveCommunicationBounding: boundingSpec,
		MinMember:                       gangSize,
	}
}

// BenchmarkFatTree5000_UnboundedBaseline benchmarks gang preemption search without topology bounding across 5,488-node fat-tree.
func BenchmarkFatTree5000_UnboundedBaseline(b *testing.B) {
	ctx := context.Background()
	prob := buildFatTreeBenchmarkProblem(8, 28, 64, -1) // Unbounded diameter
	opts := DefaultBranchAndBoundOptions()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := BranchAndBoundGangSearch(ctx, prob, opts)
		if !res.Status.IsSuccess() {
			b.Fatalf("Search failed: %v", res.Status)
		}
	}
}

// BenchmarkFatTree5000_SpineDiameterBounded benchmarks gang preemption with spine diameter bounding (MaxDiameter = 2).
func BenchmarkFatTree5000_SpineDiameterBounded(b *testing.B) {
	ctx := context.Background()
	prob := buildFatTreeBenchmarkProblem(8, 28, 64, HopSameSpine) // Diameter <= 2 (same spine switch pod)
	opts := DefaultBranchAndBoundOptions()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := BranchAndBoundGangSearch(ctx, prob, opts)
		if !res.Status.IsSuccess() {
			b.Fatalf("Search failed: %v", res.Status)
		}
	}
}

// BenchmarkFatTree5000_LeafDiameterBounded benchmarks gang preemption with leaf diameter bounding (MaxDiameter = 1).
func BenchmarkFatTree5000_LeafDiameterBounded(b *testing.B) {
	ctx := context.Background()
	prob := buildFatTreeBenchmarkProblem(8, 28, 64, HopSameLeaf) // Diameter <= 1 (same TOR/leaf switch)
	opts := DefaultBranchAndBoundOptions()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := BranchAndBoundGangSearch(ctx, prob, opts)
		if !res.Status.IsSuccess() {
			b.Fatalf("Search failed: %v", res.Status)
		}
	}
}

// BenchmarkFatTree5000_LargeGang16_Bounded benchmarks 16-pod distributed collective gang search.
func BenchmarkFatTree5000_LargeGang16_Bounded(b *testing.B) {
	ctx := context.Background()
	prob := buildFatTreeBenchmarkProblem(16, 28, 64, HopSameSpine)
	opts := DefaultBranchAndBoundOptions()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := BranchAndBoundGangSearch(ctx, prob, opts)
		if !res.Status.IsSuccess() {
			b.Fatalf("Search failed: %v", res.Status)
		}
	}
}

// BenchmarkFatTree5000_MatrixLookupSpeed benchmarks O(1) pairwise lookup speed on 5,488-node fabric.
func BenchmarkFatTree5000_MatrixLookupSpeed(b *testing.B) {
	labelsMap := BuildFatTreeTopologyLabels(28) // 5,488 nodes
	nodeNames := make([]string, 0, len(labelsMap))
	coords := make(map[string]NetworkTopologyCoordinate, len(labelsMap))
	for name, l := range labelsMap {
		nodeNames = append(nodeNames, name)
		coords[name] = NetworkTopologyCoordinate{
			NodeName:   name,
			DataCenter: l[LabelTopologyDataCenter],
			SpinePod:   l[LabelTopologySpinePod],
			LeafRack:   l[LabelTopologyLeafRack],
		}
	}

	candidateNodes := nodeNames[:128]
	matrix := NewFabricDistanceMatrix(candidateNodes, coords)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n1 := candidateNodes[i%128]
		n2 := candidateNodes[(i+1)%128]
		_ = matrix.Distance(n1, n2)
	}
}

// TestFatTree5000_SearchConvergenceAndDiameter verifies search convergence and collective diameter optimization across 5,488 nodes.
func TestFatTree5000_SearchConvergenceAndDiameter(t *testing.T) {
	ctx := context.Background()
	k := 28 // 5,488 nodes
	gangSize := 8
	candidatePool := 64

	// 1. Unbounded Search
	probUnbounded := buildFatTreeBenchmarkProblem(gangSize, k, candidatePool, -1)
	resUnbounded := BranchAndBoundGangSearch(ctx, probUnbounded, DefaultBranchAndBoundOptions())
	if !resUnbounded.Status.IsSuccess() {
		t.Fatalf("Unbounded search failed: %v", resUnbounded.Status)
	}

	// 2. Spine Bounded Search (MaxDiameter = HopSameSpine = 2)
	probSpine := buildFatTreeBenchmarkProblem(gangSize, k, candidatePool, HopSameSpine)
	resSpine := BranchAndBoundGangSearch(ctx, probSpine, DefaultBranchAndBoundOptions())
	if !resSpine.Status.IsSuccess() {
		t.Fatalf("Spine bounded search failed: %v", resSpine.Status)
	}

	// 3. Leaf Bounded Search (MaxDiameter = HopSameLeaf = 1)
	probLeaf := buildFatTreeBenchmarkProblem(gangSize, k, candidatePool, HopSameLeaf)
	resLeaf := BranchAndBoundGangSearch(ctx, probLeaf, DefaultBranchAndBoundOptions())
	if !resLeaf.Status.IsSuccess() {
		t.Fatalf("Leaf bounded search failed: %v", resLeaf.Status)
	}

	t.Logf("5,000-Node Fat-Tree Search Convergence:")
	t.Logf("Unbounded:   Diameter=%d, Explored=%d, Pruned=%d, Duration=%v",
		resUnbounded.CollectiveDiameter, resUnbounded.StatesExplored, resUnbounded.BranchesPruned, resUnbounded.SearchDuration)
	t.Logf("SpineBounded: Diameter=%d, Explored=%d, Pruned=%d, Duration=%v",
		resSpine.CollectiveDiameter, resSpine.StatesExplored, resSpine.BranchesPruned, resSpine.SearchDuration)
	t.Logf("LeafBounded:  Diameter=%d, Explored=%d, Pruned=%d, Duration=%v",
		resLeaf.CollectiveDiameter, resLeaf.StatesExplored, resLeaf.BranchesPruned, resLeaf.SearchDuration)

	if resSpine.CollectiveDiameter > HopSameSpine {
		t.Errorf("Spine diameter %d exceeds %d", resSpine.CollectiveDiameter, HopSameSpine)
	}
	if resLeaf.CollectiveDiameter > HopSameLeaf {
		t.Errorf("Leaf diameter %d exceeds %d", resLeaf.CollectiveDiameter, HopSameLeaf)
	}
}
