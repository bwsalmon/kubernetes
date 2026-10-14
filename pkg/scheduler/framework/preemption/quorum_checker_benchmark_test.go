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
)

func makeBenchPod(name string, gangId string, priority int32, cpuMilli int64, memBytes int64, gpuCount int64) *v1.Pod {
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
			Namespace: "default",
			UID:       types.UID(fmt.Sprintf("pod-%s-%s", gangId, name)),
			Labels: map[string]string{
				"gang": gangId,
			},
		},
		Spec: v1.PodSpec{
			Priority: &priority,
			Containers: []v1.Container{
				{
					Name: "worker",
					Resources: v1.ResourceRequirements{
						Requests: reqs,
						Limits:   reqs,
					},
				},
			},
		},
	}
}

// buildSaturatedClusterViableHeadrooms constructs a cluster of viable node headrooms under saturated conditions.
// viableFraction controls what fraction of the nodes are viable after Layer 1 (0.0 to 1.0).
func buildSaturatedClusterViableHeadrooms(nodeCount int, viableFraction float64, availPerNodeGPU int64) []*ViableNodeHeadroom {
	viableCount := int(float64(nodeCount) * viableFraction)
	headrooms := make([]*ViableNodeHeadroom, viableCount)
	for i := 0; i < viableCount; i++ {
		scalars := make(map[v1.ResourceName]int64)
		if availPerNodeGPU > 0 {
			scalars["nvidia.com/gpu"] = availPerNodeGPU
		}
		headrooms[i] = &ViableNodeHeadroom{
			NodeName:         fmt.Sprintf("node-%d", i),
			AvailableMilliCPU: 8000,
			AvailableMemory:   16 * 1024 * 1024 * 1024,
			AvailableStorage:  50 * 1024 * 1024 * 1024,
			AvailableScalars:  scalars,
		}
	}
	return headrooms
}

// BenchmarkGangQuorumKnapsackEarlyExit benchmarks the O(1) aggregate check across gang scales (64, 128, 256, 512 pods)
// on saturated clusters (500 to 2000 nodes).
func BenchmarkGangQuorumKnapsackEarlyExit(b *testing.B) {
	gangSizes := []int{64, 128, 256, 512}
	clusterSizes := []int{500, 1000, 2000}

	for _, gangSize := range gangSizes {
		gangPods := make([]*v1.Pod, gangSize)
		for p := 0; p < gangSize; p++ {
			gangPods[p] = makeBenchPod(fmt.Sprintf("gang-p-%d", p), "large-gang", 1000, 2000, 4*1024*1024*1024, 1)
		}

		for _, clusterSize := range clusterSizes {
			// Scenario A: Quorum Fast-Fail (Viable nodes < minMember)
			// Cluster is heavily saturated: only (gangSize - 5) nodes passed Layer 1
			b.Run(fmt.Sprintf("QuorumFastFail_Gang%d_Cluster%d", gangSize, clusterSize), func(b *testing.B) {
				viableNodes := buildSaturatedClusterViableHeadrooms(clusterSize, float64(gangSize-5)/float64(clusterSize), 2)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					res := CheckGangQuorumAndCapacityFeasibility(gangPods, gangSize, viableNodes, false)
					if res.Passed {
						b.Fatalf("expected fail, got pass")
					}
				}
			})

			// Scenario B: Scalar Knapsack GPU Fast-Fail (Viable node count >= minMember, but aggregate GPUs insufficient)
			b.Run(fmt.Sprintf("KnapsackGPUFastFail_Gang%d_Cluster%d", gangSize, clusterSize), func(b *testing.B) {
				// Viable node count is gangSize, but available GPUs per node is 0 (or total < gangSize)
				viableNodes := buildSaturatedClusterViableHeadrooms(clusterSize, float64(gangSize)/float64(clusterSize), 0)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					res := CheckGangQuorumAndCapacityFeasibility(gangPods, gangSize, viableNodes, false)
					if res.Passed {
						b.Fatalf("expected fail, got pass")
					}
				}
			})

			// Scenario C: Quorum & Knapsack Passed (Gang is viable, proceed to Layer 2/3)
			b.Run(fmt.Sprintf("FeasiblePassed_Gang%d_Cluster%d", gangSize, clusterSize), func(b *testing.B) {
				viableNodes := buildSaturatedClusterViableHeadrooms(clusterSize, float64(gangSize*2)/float64(clusterSize), 4)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					res := CheckGangQuorumAndCapacityFeasibility(gangPods, gangSize, viableNodes, false)
					if !res.Passed {
						b.Fatalf("expected pass, got fail: %v", res.Status.Message())
					}
				}
			})
		}
	}
}

// BenchmarkSuffixQuorumBranchAndBoundVsBaseline compares Branch-and-Bound search throughput
// and latency with Idea 15 Early-Exit against unpruned candidate evaluation on saturated clusters with large gangs (64-512 pods).
func BenchmarkSuffixQuorumBranchAndBoundVsBaseline(b *testing.B) {
	gangSizes := []int{64, 128, 256, 512}

	for _, gangSize := range gangSizes {
		nodesCount := gangSize - 4 // Saturated condition: fewer candidate nodes than required gang members

		gangPods := make([]*v1.Pod, gangSize)
		for i := 0; i < gangSize; i++ {
			gangPods[i] = makeBenchPod(fmt.Sprintf("p-%d", i), "gang", 1000, 1000, 1024, 0)
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
				cands[j] = GangPodCandidate{
					NodeName:         nodeNames[j],
					Cost:             ZeroPreemptionCost(),
					Feasible:         true,
					RequiredMilliCPU: 1000,
					RequiredMemory:   1024,
				}
			}
			perPodCandidates[i] = cands
		}

		nodeMaxPods := make(map[string]int, nodesCount)
		for j := 0; j < nodesCount; j++ {
			nodeMaxPods[nodeNames[j]] = 1
		}

		prob := &GangPlacementProblem{
			PreemptorPods:      gangPods,
			CandidateNodes:     nodeNames,
			NodeAllocatableCPU: nodeAllocCPU,
			NodeAllocatableMem: nodeAllocMem,
			NodeMaxPods:        nodeMaxPods,
			PerPodCandidates:   perPodCandidates,
			MinMember:          gangSize,
		}

		ctx := context.Background()

		// Idea 15 Suffix Quorum Early-Exit (fast fail in O(1) time without state exploration)
		b.Run(fmt.Sprintf("Idea15_EarlyExit_Gang%d", gangSize), func(b *testing.B) {
			opts := DefaultBranchAndBoundOptions()
			opts.EnableSuffixQuorumPruning = true
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				res := BranchAndBoundGangSearch(ctx, prob, opts)
				if res.Status.IsSuccess() {
					b.Fatalf("expected unschedulable, got success")
				}
			}
		})

		// Baseline: Unpruned exhaustive candidate evaluation (evaluating all candidate pairs before finding failure)
		b.Run(fmt.Sprintf("Baseline_CandidateScanning_Gang%d", gangSize), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Evaluate each pod's candidate list sequentially without early quorum rejection
				totalCands := 0
				for p := 0; p < len(prob.PerPodCandidates); p++ {
					for c := 0; c < len(prob.PerPodCandidates[p]); c++ {
						if prob.PerPodCandidates[p][c].Feasible {
							totalCands++
						}
					}
				}
				if totalCands == 0 {
					b.Fatalf("unexpected")
				}
			}
		})
	}
}
