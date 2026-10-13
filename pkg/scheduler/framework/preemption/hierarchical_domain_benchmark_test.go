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

// buildHierarchicalBenchmarkProblem generates large-scale cluster problems across multiple failure domains.
// e.g. numZones failure domains, nodesPerZone nodes per domain.
// half the domains have sufficient headroom/victims, while half are congested or infeasible.
func buildHierarchicalBenchmarkProblem(gangSize int, numZones int, nodesPerZone int) (*GangPlacementProblem, TopologySpreadConstraint) {
	totalNodes := numZones * nodesPerZone
	gangPods := make([]*v1.Pod, gangSize)
	for i := 0; i < gangSize; i++ {
		gangPods[i] = makeTestPod(fmt.Sprintf("pod-%d", i), "default", 1000, 1000, 1024)
	}

	nodeNames := make([]string, totalNodes)
	nodeAllocCPU := make(map[string]int64)
	nodeAllocMem := make(map[string]int64)
	nodeDomains := make(map[string]string)

	for z := 0; z < numZones; z++ {
		zoneName := fmt.Sprintf("zone-%d", z)
		// Odd zones have high allocatable headroom; even zones have 0 headroom (congested)
		isViableZone := (z % 2) != 0

		for n := 0; n < nodesPerZone; n++ {
			nodeIdx := z*nodesPerZone + n
			name := fmt.Sprintf("node-%d-%d", z, n)
			nodeNames[nodeIdx] = name
			nodeDomains[name] = zoneName

			if isViableZone {
				nodeAllocCPU[name] = 16000
				nodeAllocMem[name] = 32768
			} else {
				nodeAllocCPU[name] = 0 // Infeasible domain
				nodeAllocMem[name] = 0
			}
		}
	}

	perPodCandidates := make([][]GangPodCandidate, gangSize)
	for i := 0; i < gangSize; i++ {
		cands := make([]GangPodCandidate, totalNodes)
		for j := 0; j < totalNodes; j++ {
			nName := nodeNames[j]
			zone := nodeDomains[nName]
			isViable := (nodeAllocCPU[nName] > 0)

			victims := []*v1.Pod{}
			if isViable {
				prio := int32((j % 5) * 100)
				victims = append(victims, makeTestPod(fmt.Sprintf("v-%d-%d", i, j), "default", prio, 1000, 1024))
			}
			cost := ComputeVictimsCost(victims, 0)
			cands[j] = GangPodCandidate{
				NodeName:         nName,
				Cost:             cost,
				Victims:          victims,
				Feasible:         isViable,
				RequiredMilliCPU: 1000,
				RequiredMemory:   1024,
			}
			_ = zone
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

	return prob, constraint
}

// Gang of 16 pods across 8 zones (4 viable, 4 infeasible) x 8 nodes = 64 nodes total
func BenchmarkBaselineBnB_Gang16_Nodes64_Zones8(b *testing.B) {
	ctx := context.Background()
	prob, _ := buildHierarchicalBenchmarkProblem(16, 8, 8)
	opts := DefaultBranchAndBoundOptions()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := BranchAndBoundGangSearch(ctx, prob, opts)
		if !res.Status.IsSuccess() {
			b.Fatalf("Baseline BnB search failed")
		}
	}
}

func BenchmarkHierarchicalDomainSearch_Gang16_Nodes64_Zones8(b *testing.B) {
	ctx := context.Background()
	prob, constraint := buildHierarchicalBenchmarkProblem(16, 8, 8)
	opts := DefaultHierarchicalDomainSearchOptions()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := HierarchicalDomainGangSearch(ctx, prob, constraint, opts)
		if !res.PlacementResult.Status.IsSuccess() {
			b.Fatalf("Hierarchical search failed")
		}
	}
}

// Gang of 64 pods across 16 zones (8 viable, 8 infeasible) x 16 nodes = 256 nodes total
func BenchmarkBaselineBnB_Gang64_Nodes256_Zones16(b *testing.B) {
	ctx := context.Background()
	prob, _ := buildHierarchicalBenchmarkProblem(64, 16, 16)
	opts := DefaultBranchAndBoundOptions()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := BranchAndBoundGangSearch(ctx, prob, opts)
		if !res.Status.IsSuccess() {
			b.Fatalf("Baseline BnB search failed")
		}
	}
}

func BenchmarkHierarchicalDomainSearch_Gang64_Nodes256_Zones16(b *testing.B) {
	ctx := context.Background()
	prob, constraint := buildHierarchicalBenchmarkProblem(64, 16, 16)
	opts := DefaultHierarchicalDomainSearchOptions()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := HierarchicalDomainGangSearch(ctx, prob, constraint, opts)
		if !res.PlacementResult.Status.IsSuccess() {
			b.Fatalf("Hierarchical search failed")
		}
	}
}

// Gang of 256 pods across 32 zones (16 viable, 16 infeasible) x 32 nodes = 1024 nodes total
func BenchmarkBaselineBnB_Gang256_Nodes1024_Zones32(b *testing.B) {
	ctx := context.Background()
	prob, _ := buildHierarchicalBenchmarkProblem(256, 32, 32)
	opts := DefaultBranchAndBoundOptions()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := BranchAndBoundGangSearch(ctx, prob, opts)
		if !res.Status.IsSuccess() {
			b.Fatalf("Baseline BnB search failed")
		}
	}
}

func BenchmarkHierarchicalDomainSearch_Gang256_Nodes1024_Zones32(b *testing.B) {
	ctx := context.Background()
	prob, constraint := buildHierarchicalBenchmarkProblem(256, 32, 32)
	opts := DefaultHierarchicalDomainSearchOptions()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := HierarchicalDomainGangSearch(ctx, prob, constraint, opts)
		if !res.PlacementResult.Status.IsSuccess() {
			b.Fatalf("Hierarchical search failed")
		}
	}
}
