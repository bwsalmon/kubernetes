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
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/interpodaffinity"
)

func buildDense1000NodeContentionProblem(gangSize int, withMatrix bool) *GangPlacementProblem {
	const numNodes = 1000
	gangPods := make([]*v1.Pod, gangSize)
	for i := 0; i < gangSize; i++ {
		gangPods[i] = createMutualAntiAffinityPod(fmt.Sprintf("train-pod-%d", i), "mega-gang-llm")
	}

	nodes := make([]string, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = fmt.Sprintf("dense-node-%04d", i)
	}

	// Each pod considers multiple candidate nodes:
	// - Candidates with potential anti-affinity collisions (lower tentative cost)
	// - Conflict-free distinct primary node
	perPodCandidates := make([][]GangPodCandidate, gangSize)
	for i := 0; i < gangSize; i++ {
		perPodCandidates[i] = []GangPodCandidate{
			{
				NodeName: nodes[(i+1)%numNodes],
				Feasible: true,
				Cost:     PreemptionCost{HighestPriority: 5, SumPriorities: 5, VictimCount: 1},
			},
			{
				NodeName: nodes[(i+2)%numNodes],
				Feasible: true,
				Cost:     PreemptionCost{HighestPriority: 6, SumPriorities: 6, VictimCount: 1},
			},
			{
				NodeName: nodes[i%numNodes],
				Feasible: true,
				Cost:     PreemptionCost{HighestPriority: 10, SumPriorities: 10, VictimCount: 1},
			},
			{
				NodeName: nodes[(i*3)%numNodes],
				Feasible: true,
				Cost:     PreemptionCost{HighestPriority: 20, SumPriorities: 20, VictimCount: 1},
			},
		}
	}

	var matrix *interpodaffinity.InterPodAntiAffinityBitmaskMatrix
	if withMatrix {
		var err error
		matrix, err = interpodaffinity.NewInterPodAntiAffinityBitmaskMatrix(gangPods, nodes, nil, v1.LabelHostname)
		if err != nil {
			panic(err)
		}
	}

	return &GangPlacementProblem{
		PreemptorPods:       gangPods,
		CandidateNodes:      nodes,
		PerPodCandidates:    perPodCandidates,
		AntiAffinityMatrix:  matrix,
		MinMember:           gangSize,
	}
}

// BenchmarkBranchAndBound_Idea13_Dense1000NodeCluster evaluates multi-node gang preemption search
// on large training gangs (64, 128, 256 pods) across 1,000 nodes with strict inter-pod anti-affinity.
func BenchmarkBranchAndBound_Idea13_Dense1000NodeCluster(b *testing.B) {
	for _, gangSize := range []int{64, 128, 256} {
		b.Run(fmt.Sprintf("BitmaskMatrix_Gang_%d_Nodes_1000", gangSize), func(b *testing.B) {
			prob := buildDense1000NodeContentionProblem(gangSize, true)
			opts := DefaultBranchAndBoundOptions()
			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				res := BranchAndBoundGangSearch(ctx, prob, opts)
				if !res.Status.IsSuccess() {
					b.Fatalf("search failed: %v", res.Status.Message())
				}
			}
		})
	}
}

// BenchmarkBranchAndBound_Idea13_NoGreedyPurePruning evaluates pure branch-and-bound pruning
// without greedy upper bound on 64, 128, 256 pods on 1,000 nodes.
func BenchmarkBranchAndBound_Idea13_NoGreedyPurePruning(b *testing.B) {
	for _, gangSize := range []int{64, 128, 256} {
		b.Run(fmt.Sprintf("PurePrune_Gang_%d_Nodes_1000", gangSize), func(b *testing.B) {
			prob := buildDense1000NodeContentionProblem(gangSize, true)
			opts := BranchAndBoundOptions{
				EnableEarlyExit:         true,
				UseGreedyUpperBound:     false,
				UseBestFirstBranchOrder: true,
			}
			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				res := BranchAndBoundGangSearch(ctx, prob, opts)
				if !res.Status.IsSuccess() {
					b.Fatalf("search failed: %v", res.Status.Message())
				}
			}
		})
	}
}
