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

func buildBenchmarkProblem(gangSize int, nodesCount int) *GangPlacementProblem {
	gangPods := make([]*v1.Pod, gangSize)
	for i := 0; i < gangSize; i++ {
		gangPods[i] = makeTestPod(fmt.Sprintf("pod-%d", i), "default", 1000, 1000, 1024)
	}

	nodeNames := make([]string, nodesCount)
	nodeAllocCPU := make(map[string]int64)
	nodeAllocMem := make(map[string]int64)
	for j := 0; j < nodesCount; j++ {
		name := fmt.Sprintf("node-%d", j)
		nodeNames[j] = name
		nodeAllocCPU[name] = 16000
		nodeAllocMem[name] = 32768
	}

	perPodCandidates := make([][]GangPodCandidate, gangSize)
	for i := 0; i < gangSize; i++ {
		cands := make([]GangPodCandidate, nodesCount)
		for j := 0; j < nodesCount; j++ {
			prio := int32((j % 5) * 100)
			pdb := 0
			if j%4 == 0 {
				pdb = 1
			}
			victims := []*v1.Pod{
				makeTestPod(fmt.Sprintf("v-%d-%d", i, j), "default", prio, 1000, 1024),
			}
			cost := ComputeVictimsCost(victims, pdb)
			cands[j] = GangPodCandidate{
				NodeName:         nodeNames[j],
				Cost:             cost,
				Victims:          victims,
				PDBViolations:    pdb,
				Feasible:         true,
				RequiredMilliCPU: 1000,
				RequiredMemory:   1024,
			}
		}
		perPodCandidates[i] = cands
	}

	return &GangPlacementProblem{
		PreemptorPods:      gangPods,
		CandidateNodes:     nodeNames,
		NodeAllocatableCPU: nodeAllocCPU,
		NodeAllocatableMem: nodeAllocMem,
		PerPodCandidates:   perPodCandidates,
		MinMember:          gangSize,
	}
}

func BenchmarkExhaustiveSearch_Gang4_Nodes6(b *testing.B) {
	ctx := context.Background()
	prob := buildBenchmarkProblem(4, 6)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := ExhaustiveGangSearch(ctx, prob)
		if !res.Status.IsSuccess() {
			b.Fatalf("Exhaustive search failed")
		}
	}
}

func BenchmarkBranchAndBound_Gang4_Nodes6(b *testing.B) {
	ctx := context.Background()
	prob := buildBenchmarkProblem(4, 6)
	opts := DefaultBranchAndBoundOptions()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := BranchAndBoundGangSearch(ctx, prob, opts)
		if !res.Status.IsSuccess() {
			b.Fatalf("Branch and bound search failed")
		}
	}
}

func BenchmarkExhaustiveSearch_Gang6_Nodes6(b *testing.B) {
	ctx := context.Background()
	prob := buildBenchmarkProblem(6, 6)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := ExhaustiveGangSearch(ctx, prob)
		if !res.Status.IsSuccess() {
			b.Fatalf("Exhaustive search failed")
		}
	}
}

func BenchmarkBranchAndBound_Gang6_Nodes6(b *testing.B) {
	ctx := context.Background()
	prob := buildBenchmarkProblem(6, 6)
	opts := DefaultBranchAndBoundOptions()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := BranchAndBoundGangSearch(ctx, prob, opts)
		if !res.Status.IsSuccess() {
			b.Fatalf("Branch and bound search failed")
		}
	}
}

func BenchmarkBranchAndBound_Gang12_Nodes16(b *testing.B) {
	ctx := context.Background()
	prob := buildBenchmarkProblem(12, 16)
	opts := DefaultBranchAndBoundOptions()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := BranchAndBoundGangSearch(ctx, prob, opts)
		if !res.Status.IsSuccess() {
			b.Fatalf("Branch and bound search failed")
		}
	}
}

func BenchmarkBranchAndBound_Gang24_Nodes32(b *testing.B) {
	ctx := context.Background()
	prob := buildBenchmarkProblem(24, 32)
	opts := DefaultBranchAndBoundOptions()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := BranchAndBoundGangSearch(ctx, prob, opts)
		if !res.Status.IsSuccess() {
			b.Fatalf("Branch and bound search failed")
		}
	}
}
