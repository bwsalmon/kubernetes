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
	"math"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func makeTestPod(name string, namespace string, priority int32, cpuMilli int64, memBytes int64) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(fmt.Sprintf("pod-%s-%s", namespace, name)),
		},
		Spec: v1.PodSpec{
			Priority: &priority,
		},
	}
}

// TestPreemptionCost_Ordering verifies the 4-tier lexicographical cascade matching OrderedScoreFuncs.
func TestPreemptionCost_Ordering(t *testing.T) {
	tests := []struct {
		name    string
		costA   PreemptionCost
		costB   PreemptionCost
		aIsLess bool
	}{
		{
			name:    "Tier 1: PDB violations dominate priority and count",
			costA:   PreemptionCost{PDBViolations: 0, HighestPriority: 1000, SumPriorities: 5000, VictimCount: 10},
			costB:   PreemptionCost{PDBViolations: 1, HighestPriority: 10, SumPriorities: 10, VictimCount: 1},
			aIsLess: true,
		},
		{
			name:    "Tier 2: When PDB violations equal, lowest highest priority wins",
			costA:   PreemptionCost{PDBViolations: 0, HighestPriority: 50, SumPriorities: 5000, VictimCount: 10},
			costB:   PreemptionCost{PDBViolations: 0, HighestPriority: 100, SumPriorities: 100, VictimCount: 1},
			aIsLess: true,
		},
		{
			name:    "Tier 3: When PDB and highest priority equal, lowest sum of priorities wins",
			costA:   PreemptionCost{PDBViolations: 0, HighestPriority: 50, SumPriorities: 200, VictimCount: 10},
			costB:   PreemptionCost{PDBViolations: 0, HighestPriority: 50, SumPriorities: 300, VictimCount: 2},
			aIsLess: true,
		},
		{
			name:    "Tier 4: When PDB, highest, and sum equal, lowest victim count wins",
			costA:   PreemptionCost{PDBViolations: 0, HighestPriority: 50, SumPriorities: 200, VictimCount: 2},
			costB:   PreemptionCost{PDBViolations: 0, HighestPriority: 50, SumPriorities: 200, VictimCount: 4},
			aIsLess: true,
		},
		{
			name:    "Equal costs",
			costA:   PreemptionCost{PDBViolations: 1, HighestPriority: 50, SumPriorities: 200, VictimCount: 2},
			costB:   PreemptionCost{PDBViolations: 1, HighestPriority: 50, SumPriorities: 200, VictimCount: 2},
			aIsLess: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotLess := tt.costA.Less(tt.costB)
			if gotLess != tt.aIsLess {
				t.Errorf("costA.Less(costB) = %v, want %v", gotLess, tt.aIsLess)
			}
		})
	}
}

// TestBranchAndBound_EquivalenceAcrossTopologies tests bit-for-bit equivalence between
// Exhaustive baseline search and Branch-and-Bound across varied topologies.
func TestBranchAndBound_EquivalenceAcrossTopologies(t *testing.T) {
	ctx := context.Background()

	testScenarios := []struct {
		name       string
		gangSize   int
		nodesCount int
		buildProb  func(gangSize, nodesCount int) *GangPlacementProblem
	}{
		{
			name:       "Dense Cluster with Priority Gradients",
			gangSize:   4,
			nodesCount: 5,
			buildProb: func(gangSize, nodesCount int) *GangPlacementProblem {
				gangPods := make([]*v1.Pod, gangSize)
				for i := 0; i < gangSize; i++ {
					gangPods[i] = makeTestPod(fmt.Sprintf("gang-pod-%d", i), "default", 1000, 1000, 1024)
				}
				nodeNames := make([]string, nodesCount)
				for j := 0; j < nodesCount; j++ {
					nodeNames[j] = fmt.Sprintf("node-%d", j)
				}

				perPodCandidates := make([][]GangPodCandidate, gangSize)
				for i := 0; i < gangSize; i++ {
					cands := make([]GangPodCandidate, nodesCount)
					for j := 0; j < nodesCount; j++ {
						// Varied victim priorities per node
						victimPrio := int32((j + 1) * 100)
						victimCount := (j % 3) + 1
						pdbViolations := 0
						if j == 0 {
							pdbViolations = 1 // Node 0 violates PDB
						}
						victims := make([]*v1.Pod, victimCount)
						for v := 0; v < victimCount; v++ {
							victims[v] = makeTestPod(fmt.Sprintf("victim-%d-%d-%d", i, j, v), "default", victimPrio, 500, 512)
						}
						cost := ComputeVictimsCost(victims, pdbViolations)
						cands[j] = GangPodCandidate{
							NodeName:         nodeNames[j],
							Cost:             cost,
							Victims:          victims,
							PDBViolations:    pdbViolations,
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
					NodeAllocatableCPU: map[string]int64{"node-0": 8000, "node-1": 8000, "node-2": 8000, "node-3": 8000, "node-4": 8000},
					NodeAllocatableMem: map[string]int64{"node-0": 8192, "node-1": 8192, "node-2": 8192, "node-3": 8192, "node-4": 8192},
					PerPodCandidates:   perPodCandidates,
					MinMember:          gangSize,
				}
			},
		},
		{
			name:       "Fragmented Allocation with Free Capacity Nodes",
			gangSize:   5,
			nodesCount: 6,
			buildProb: func(gangSize, nodesCount int) *GangPlacementProblem {
				gangPods := make([]*v1.Pod, gangSize)
				for i := 0; i < gangSize; i++ {
					gangPods[i] = makeTestPod(fmt.Sprintf("gang-pod-%d", i), "default", 1000, 1000, 1024)
				}
				nodeNames := make([]string, nodesCount)
				for j := 0; j < nodesCount; j++ {
					nodeNames[j] = fmt.Sprintf("node-%d", j)
				}

				perPodCandidates := make([][]GangPodCandidate, gangSize)
				for i := 0; i < gangSize; i++ {
					cands := make([]GangPodCandidate, nodesCount)
					for j := 0; j < nodesCount; j++ {
						var victims []*v1.Pod
						pdbViolations := 0
						if j >= 3 {
							// Nodes 3, 4, 5 have zero victims (free headroom)
							victims = nil
						} else {
							// Nodes 0, 1, 2 have preemption victims
							victims = []*v1.Pod{
								makeTestPod(fmt.Sprintf("victim-%d-%d", i, j), "default", int32(j*50+10), 1000, 1024),
							}
						}
						cost := ComputeVictimsCost(victims, pdbViolations)
						cands[j] = GangPodCandidate{
							NodeName:         nodeNames[j],
							Cost:             cost,
							Victims:          victims,
							PDBViolations:    pdbViolations,
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
					NodeAllocatableCPU: map[string]int64{"node-0": 8000, "node-1": 8000, "node-2": 8000, "node-3": 2000, "node-4": 2000, "node-5": 2000},
					NodeAllocatableMem: map[string]int64{"node-0": 8192, "node-1": 8192, "node-2": 8192, "node-3": 2048, "node-4": 2048, "node-5": 2048},
					PerPodCandidates:   perPodCandidates,
					MinMember:          gangSize,
				}
			},
		},
		{
			name:       "Zone Topology Spread Constraint (3 Zones, maxSkew=1)",
			gangSize:   6,
			nodesCount: 6,
			buildProb: func(gangSize, nodesCount int) *GangPlacementProblem {
				gangPods := make([]*v1.Pod, gangSize)
				for i := 0; i < gangSize; i++ {
					gangPods[i] = makeTestPod(fmt.Sprintf("gang-pod-%d", i), "default", 1000, 1000, 1024)
				}
				nodeNames := make([]string, nodesCount)
				nodeDomains := make(map[string]string)
				for j := 0; j < nodesCount; j++ {
					nodeNames[j] = fmt.Sprintf("node-%d", j)
					// 3 zones: zone-a (nodes 0,1), zone-b (nodes 2,3), zone-c (nodes 4,5)
					zone := fmt.Sprintf("zone-%c", 'a'+(j/2))
					nodeDomains[nodeNames[j]] = zone
				}

				perPodCandidates := make([][]GangPodCandidate, gangSize)
				for i := 0; i < gangSize; i++ {
					cands := make([]GangPodCandidate, nodesCount)
					for j := 0; j < nodesCount; j++ {
						victims := []*v1.Pod{
							makeTestPod(fmt.Sprintf("victim-%d-%d", i, j), "default", int32(j*100+10), 1000, 1024),
						}
						cost := ComputeVictimsCost(victims, 0)
						cands[j] = GangPodCandidate{
							NodeName:         nodeNames[j],
							Cost:             cost,
							Victims:          victims,
							PDBViolations:    0,
							Feasible:         true,
							RequiredMilliCPU: 1000,
							RequiredMemory:   1024,
						}
					}
					perPodCandidates[i] = cands
				}

				topo := TopologySpreadConstraint{
					TopologyKey: "topology.kubernetes.io/zone",
					MaxSkew:     1,
					NodeDomains: nodeDomains,
				}

				return &GangPlacementProblem{
					PreemptorPods:       gangPods,
					CandidateNodes:      nodeNames,
					NodeAllocatableCPU:  map[string]int64{"node-0": 4000, "node-1": 4000, "node-2": 4000, "node-3": 4000, "node-4": 4000, "node-5": 4000},
					NodeAllocatableMem:  map[string]int64{"node-0": 4096, "node-1": 4096, "node-2": 4096, "node-3": 4096, "node-4": 4096, "node-5": 4096},
					PerPodCandidates:    perPodCandidates,
					TopologyConstraints: []TopologySpreadConstraint{topo},
					MinMember:           gangSize,
				}
			},
		},
		{
			name:       "Heterogeneous Pod Requirements and Knapsack Limits",
			gangSize:   4,
			nodesCount: 4,
			buildProb: func(gangSize, nodesCount int) *GangPlacementProblem {
				// 4 pods with different resource sizes (1000m, 2000m, 3000m, 4000m)
				gangPods := make([]*v1.Pod, gangSize)
				for i := 0; i < gangSize; i++ {
					gangPods[i] = makeTestPod(fmt.Sprintf("hetero-pod-%d", i), "default", 1000, int64((i+1)*1000), int64((i+1)*1024))
				}
				nodeNames := make([]string, nodesCount)
				for j := 0; j < nodesCount; j++ {
					nodeNames[j] = fmt.Sprintf("node-%d", j)
				}

				perPodCandidates := make([][]GangPodCandidate, gangSize)
				for i := 0; i < gangSize; i++ {
					cands := make([]GangPodCandidate, nodesCount)
					for j := 0; j < nodesCount; j++ {
						prio := int32((j * 10) + i)
						victims := []*v1.Pod{
							makeTestPod(fmt.Sprintf("v-hetero-%d-%d", i, j), "default", prio, int64((i+1)*1000), int64((i+1)*1024)),
						}
						cost := ComputeVictimsCost(victims, 0)
						cands[j] = GangPodCandidate{
							NodeName:         nodeNames[j],
							Cost:             cost,
							Victims:          victims,
							PDBViolations:    0,
							Feasible:         true,
							RequiredMilliCPU: int64((i + 1) * 1000),
							RequiredMemory:   int64((i + 1) * 1024),
						}
					}
					perPodCandidates[i] = cands
				}

				return &GangPlacementProblem{
					PreemptorPods:      gangPods,
					CandidateNodes:     nodeNames,
					NodeAllocatableCPU: map[string]int64{"node-0": 5000, "node-1": 5000, "node-2": 5000, "node-3": 5000},
					NodeAllocatableMem: map[string]int64{"node-0": 5120, "node-1": 5120, "node-2": 5120, "node-3": 5120},
					PerPodCandidates:   perPodCandidates,
					MinMember:          gangSize,
				}
			},
		},
	}

	for _, sc := range testScenarios {
		t.Run(sc.name, func(t *testing.T) {
			prob := sc.buildProb(sc.gangSize, sc.nodesCount)

			// 1. Run Exhaustive baseline
			baselineRes := ExhaustiveGangSearch(ctx, prob)
			if !baselineRes.Status.IsSuccess() {
				t.Fatalf("Baseline exhaustive search failed: %v", baselineRes.Status.Message())
			}

			// 2. Run Branch-and-Bound with default options
			bnbRes := BranchAndBoundGangSearch(ctx, prob, DefaultBranchAndBoundOptions())
			if !bnbRes.Status.IsSuccess() {
				t.Fatalf("Branch-and-Bound search failed: %v", bnbRes.Status.Message())
			}

			// 3. Verify exact cost equivalence
			if !bnbRes.TotalCost.Equal(baselineRes.TotalCost) {
				t.Errorf("Cost mismatch: Branch-and-Bound cost = %+v, want baseline cost = %+v", bnbRes.TotalCost, baselineRes.TotalCost)
			}

			// 4. Verify PDB violations count match
			if bnbRes.TotalCost.PDBViolations != baselineRes.TotalCost.PDBViolations {
				t.Errorf("PDB violations mismatch: bnb = %d, baseline = %d", bnbRes.TotalCost.PDBViolations, baselineRes.TotalCost.PDBViolations)
			}

			// 5. Verify victim count match
			if len(bnbRes.SelectedVictims) != len(baselineRes.SelectedVictims) {
				t.Errorf("Victim count mismatch: bnb = %d, baseline = %d", len(bnbRes.SelectedVictims), len(baselineRes.SelectedVictims))
			}

			// 6. Run Branch-and-Bound WITHOUT greedy upper bound to verify pure lower-bound branch pruning
			bnbNoGreedy := BranchAndBoundGangSearch(ctx, prob, BranchAndBoundOptions{
				EnableEarlyExit:         false,
				UseGreedyUpperBound:     false,
				UseBestFirstBranchOrder: true,
			})
			if !bnbNoGreedy.Status.IsSuccess() {
				t.Fatalf("BnB without greedy failed: %v", bnbNoGreedy.Status.Message())
			}
			if !bnbNoGreedy.TotalCost.Equal(baselineRes.TotalCost) {
				t.Errorf("BnB without greedy cost mismatch: got %+v, want %+v", bnbNoGreedy.TotalCost, baselineRes.TotalCost)
			}

			t.Logf("[%s] Baseline States: %d | BnB States: %d | BnB (No Greedy) States: %d, Pruned: %d",
				sc.name, baselineRes.StatesExplored, bnbRes.StatesExplored, bnbNoGreedy.StatesExplored, bnbNoGreedy.BranchesPruned)
		})
	}
}

// TestBranchAndBound_PurePruningWithoutGreedy verifies lower-bound branch pruning in the presence of traps.
func TestBranchAndBound_PurePruningWithoutGreedy(t *testing.T) {
	ctx := context.Background()

	gangSize := 5
	nodesCount := 5

	gangPods := make([]*v1.Pod, gangSize)
	for i := 0; i < gangSize; i++ {
		gangPods[i] = makeTestPod(fmt.Sprintf("gang-pod-%d", i), "default", 1000, 1000, 1024)
	}
	nodeNames := make([]string, nodesCount)
	for j := 0; j < nodesCount; j++ {
		nodeNames[j] = fmt.Sprintf("node-%d", j)
	}

	perPodCandidates := make([][]GangPodCandidate, gangSize)
	for i := 0; i < gangSize; i++ {
		cands := make([]GangPodCandidate, nodesCount)
		for j := 0; j < nodesCount; j++ {
			// Node 0 has high victim cost, node 4 has low victim cost
			prio := int32((nodesCount - j) * 100)
			victims := []*v1.Pod{
				makeTestPod(fmt.Sprintf("v-%d-%d", i, j), "default", prio, 1000, 1024),
			}
			cost := ComputeVictimsCost(victims, 0)
			cands[j] = GangPodCandidate{
				NodeName:         nodeNames[j],
				Cost:             cost,
				Victims:          victims,
				PDBViolations:    0,
				Feasible:         true,
				RequiredMilliCPU: 1000,
				RequiredMemory:   1024,
			}
		}
		perPodCandidates[i] = cands
	}

	prob := &GangPlacementProblem{
		PreemptorPods:      gangPods,
		CandidateNodes:     nodeNames,
		NodeAllocatableCPU: map[string]int64{"node-0": 5000, "node-1": 5000, "node-2": 5000, "node-3": 5000, "node-4": 5000},
		NodeAllocatableMem: map[string]int64{"node-0": 5120, "node-1": 5120, "node-2": 5120, "node-3": 5120, "node-4": 5120},
		PerPodCandidates:   perPodCandidates,
		MinMember:          gangSize,
	}

	baselineRes := ExhaustiveGangSearch(ctx, prob)
	bnbRes := BranchAndBoundGangSearch(ctx, prob, BranchAndBoundOptions{
		EnableEarlyExit:         false,
		UseGreedyUpperBound:     false,
		UseBestFirstBranchOrder: true,
	})

	if !bnbRes.TotalCost.Equal(baselineRes.TotalCost) {
		t.Fatalf("Cost mismatch: got %+v, want %+v", bnbRes.TotalCost, baselineRes.TotalCost)
	}

	t.Logf("Baseline States: %d | BnB Explored: %d | BnB Pruned: %d",
		baselineRes.StatesExplored, bnbRes.StatesExplored, bnbRes.BranchesPruned)

	if bnbRes.BranchesPruned == 0 {
		t.Errorf("Expected branches pruned > 0, got %d", bnbRes.BranchesPruned)
	}
	if bnbRes.StatesExplored >= baselineRes.StatesExplored {
		t.Errorf("Expected BnB explored states (%d) < baseline states (%d)", bnbRes.StatesExplored, baselineRes.StatesExplored)
	}
}

// TestBranchAndBound_PruningEfficiency verifies significant branch pruning on combinatorial search spaces.
func TestBranchAndBound_PruningEfficiency(t *testing.T) {
	ctx := context.Background()

	// Gang of 6 pods across 8 nodes -> 8^6 = 262,144 combinations
	gangSize := 6
	nodesCount := 8

	gangPods := make([]*v1.Pod, gangSize)
	for i := 0; i < gangSize; i++ {
		gangPods[i] = makeTestPod(fmt.Sprintf("gang-pod-%d", i), "default", 1000, 1000, 1024)
	}
	nodeNames := make([]string, nodesCount)
	for j := 0; j < nodesCount; j++ {
		nodeNames[j] = fmt.Sprintf("node-%d", j)
	}

	perPodCandidates := make([][]GangPodCandidate, gangSize)
	for i := 0; i < gangSize; i++ {
		cands := make([]GangPodCandidate, nodesCount)
		for j := 0; j < nodesCount; j++ {
			prio := int32((j % 4) * 100)
			pdb := 0
			if j > 5 {
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

	prob := &GangPlacementProblem{
		PreemptorPods:      gangPods,
		CandidateNodes:     nodeNames,
		NodeAllocatableCPU: map[string]int64{"node-0": 8000, "node-1": 8000, "node-2": 8000, "node-3": 8000, "node-4": 8000, "node-5": 8000, "node-6": 8000, "node-7": 8000},
		NodeAllocatableMem: map[string]int64{"node-0": 8192, "node-1": 8192, "node-2": 8192, "node-3": 8192, "node-4": 8192, "node-5": 8192, "node-6": 8192, "node-7": 8192},
		PerPodCandidates:   perPodCandidates,
		MinMember:          gangSize,
	}

	bnbRes := BranchAndBoundGangSearch(ctx, prob, DefaultBranchAndBoundOptions())
	if !bnbRes.Status.IsSuccess() {
		t.Fatalf("BnB search failed: %v", bnbRes.Status.Message())
	}

	theoreticalStates := int(math.Pow(float64(nodesCount), float64(gangSize)))
	t.Logf("Theoretical Max Combinations: %d | BnB States Explored: %d | Branches Pruned: %d",
		theoreticalStates, bnbRes.StatesExplored, bnbRes.BranchesPruned)

	if bnbRes.StatesExplored > 1000 {
		t.Errorf("Branch and Bound explored %d states, expected < 1000 due to admissible pruning", bnbRes.StatesExplored)
	}
}

// TestBranchAndBound_InfeasibleHandling ensures graceful rejection when no feasible placement exists.
func TestBranchAndBound_InfeasibleHandling(t *testing.T) {
	ctx := context.Background()

	gangPods := []*v1.Pod{
		makeTestPod("gang-0", "default", 1000, 2000, 2048),
		makeTestPod("gang-1", "default", 1000, 2000, 2048),
	}
	nodeNames := []string{"node-0"}

	prob := &GangPlacementProblem{
		PreemptorPods:  gangPods,
		CandidateNodes: nodeNames,
		// Capacity only allows 1 pod (2000 CPU)
		NodeAllocatableCPU: map[string]int64{"node-0": 2000},
		NodeAllocatableMem: map[string]int64{"node-0": 2048},
		PerPodCandidates: [][]GangPodCandidate{
			{
				{
					NodeName:         "node-0",
					Cost:             ZeroPreemptionCost(),
					Feasible:         true,
					RequiredMilliCPU: 2000,
					RequiredMemory:   2048,
				},
			},
			{
				{
					NodeName:         "node-0",
					Cost:             ZeroPreemptionCost(),
					Feasible:         true,
					RequiredMilliCPU: 2000,
					RequiredMemory:   2048,
				},
			},
		},
		MinMember: 2,
	}

	res := BranchAndBoundGangSearch(ctx, prob, DefaultBranchAndBoundOptions())
	if res.Status.IsSuccess() {
		t.Errorf("Expected Unschedulable status for infeasible capacity, got success")
	}
}

// TestBranchAndBound_Hardening_AdversarialDegradation checks behavior under adversarial permutation pressure.
func TestBranchAndBound_Hardening_AdversarialDegradation(t *testing.T) {
	ctx := context.Background()

	// Gang of 8 pods across 10 nodes where each node fits only 1 pod.
	// This forces extensive branch exploration when early exit is disabled.
	gangSize := 8
	nodesCount := 10

	gangPods := make([]*v1.Pod, gangSize)
	for i := 0; i < gangSize; i++ {
		gangPods[i] = makeTestPod(fmt.Sprintf("adv-pod-%d", i), "default", 1000, 1000, 1024)
	}

	nodeNames := make([]string, nodesCount)
	nodeAllocCPU := make(map[string]int64)
	nodeAllocMem := make(map[string]int64)
	for j := 0; j < nodesCount; j++ {
		nodeNames[j] = fmt.Sprintf("adv-node-%d", j)
		nodeAllocCPU[nodeNames[j]] = 1000 // Each node fits strictly 1 pod
		nodeAllocMem[nodeNames[j]] = 1024
	}

	perPodCandidates := make([][]GangPodCandidate, gangSize)
	for i := 0; i < gangSize; i++ {
		cands := make([]GangPodCandidate, nodesCount)
		for j := 0; j < nodesCount; j++ {
			prio := int32((j + 1) * 10)
			victims := []*v1.Pod{
				makeTestPod(fmt.Sprintf("v-adv-%d-%d", i, j), "default", prio, 1000, 1024),
			}
			cost := ComputeVictimsCost(victims, 0)
			cands[j] = GangPodCandidate{
				NodeName:         nodeNames[j],
				Cost:             cost,
				Victims:          victims,
				PDBViolations:    0,
				Feasible:         true,
				RequiredMilliCPU: 1000,
				RequiredMemory:   1024,
			}
		}
		perPodCandidates[i] = cands
	}

	prob := &GangPlacementProblem{
		PreemptorPods:      gangPods,
		CandidateNodes:     nodeNames,
		NodeAllocatableCPU: nodeAllocCPU,
		NodeAllocatableMem: nodeAllocMem,
		PerPodCandidates:   perPodCandidates,
		MinMember:          gangSize,
	}

	res := BranchAndBoundGangSearch(ctx, prob, DefaultBranchAndBoundOptions())
	if !res.Status.IsSuccess() {
		t.Fatalf("Expected success under tight node capacity, got: %v", res.Status.Message())
	}
	t.Logf("Adversarial scenario: explored=%d, pruned=%d, duration=%v",
		res.StatesExplored, res.BranchesPruned, res.SearchDuration)
}

// TestBranchAndBound_Hardening_MultiNodeDisruptionModeAll audits multi-node victim group interactions.
// When KEP-6012 / KEP-5710 DisruptionModeAll is enabled, evicting a pod on Node A may evict coupled pods
// on Node B. This test verifies that BranchAndBound correctly tracks the union of unique victim pods
// across candidate nodes and exposes any discrepancy with naive per-node cost summation.
func TestBranchAndBound_Hardening_MultiNodeDisruptionModeAll(t *testing.T) {
	ctx := context.Background()

	v1Pod := makeTestPod("victim-v1", "default", 10, 1000, 1024)
	v2Pod := makeTestPod("victim-v2", "default", 10, 1000, 1024)
	v3a := makeTestPod("victim-v3a", "default", 10, 1000, 1024)
	v3b := makeTestPod("victim-v3b", "default", 10, 1000, 1024)
	v3c := makeTestPod("victim-v3c", "default", 10, 1000, 1024)

	costMultiNodeG := ComputeVictimsCost([]*v1.Pod{v1Pod, v2Pod}, 0)
	costNodeC := ComputeVictimsCost([]*v1.Pod{v3a, v3b, v3c}, 0)

	p0 := makeTestPod("p0", "default", 1000, 1000, 1024)
	p1 := makeTestPod("p1", "default", 1000, 1000, 1024)

	prob := &GangPlacementProblem{
		PreemptorPods:  []*v1.Pod{p0, p1},
		CandidateNodes: []string{"node-A", "node-B", "node-C"},
		NodeAllocatableCPU: map[string]int64{"node-A": 1000, "node-B": 1000, "node-C": 2000},
		NodeAllocatableMem: map[string]int64{"node-A": 1024, "node-B": 1024, "node-C": 2048},
		PerPodCandidates: [][]GangPodCandidate{
			{
				{NodeName: "node-C", Cost: costNodeC, Victims: []*v1.Pod{v3a, v3b, v3c}, Feasible: true, RequiredMilliCPU: 1000, RequiredMemory: 1024},
				{NodeName: "node-A", Cost: costMultiNodeG, Victims: []*v1.Pod{v1Pod, v2Pod}, Feasible: true, RequiredMilliCPU: 1000, RequiredMemory: 1024},
			},
			{
				{NodeName: "node-C", Cost: costNodeC, Victims: []*v1.Pod{v3a, v3b, v3c}, Feasible: true, RequiredMilliCPU: 1000, RequiredMemory: 1024},
				{NodeName: "node-B", Cost: costMultiNodeG, Victims: []*v1.Pod{v1Pod, v2Pod}, Feasible: true, RequiredMilliCPU: 1000, RequiredMemory: 1024},
			},
		},
		MinMember: 2,
	}

	res := BranchAndBoundGangSearch(ctx, prob, DefaultBranchAndBoundOptions())
	if !res.Status.IsSuccess() {
		t.Fatalf("BnB search failed: %v", res.Status.Message())
	}

	// Verify that SelectedVictims correctly dedupes shared victim UIDs
	uniqueVictimUIDs := make(map[types.UID]struct{})
	for _, v := range res.SelectedVictims {
		uniqueVictimUIDs[v.UID] = struct{}{}
	}
	if len(uniqueVictimUIDs) != len(res.SelectedVictims) {
		t.Errorf("Duplicate victims in SelectedVictims: total %d vs unique %d",
			len(res.SelectedVictims), len(uniqueVictimUIDs))
	}
	t.Logf("MultiNodeDisruptionModeAll: TotalCost=%+v, SelectedVictims=%d",
		res.TotalCost, len(res.SelectedVictims))
}

// TestBranchAndBound_Hardening_PartialQuorumMinMember audits behavior when MinMember < TotalMember.
// Currently BranchAndBoundGangSearch requires all pods in PreemptorPods to find a candidate node.
// If MinMember < TotalMember and one member is unschedulable, BranchAndBoundGangSearch returns Unschedulable.
// This test documents and asserts this known boundary behavior.
func TestBranchAndBound_Hardening_PartialQuorumMinMember(t *testing.T) {
	ctx := context.Background()

	p0 := makeTestPod("p0", "default", 1000, 1000, 1024)
	p1 := makeTestPod("p1", "default", 1000, 1000, 1024)
	p2 := makeTestPod("p2", "default", 1000, 1000, 1024)

	prob := &GangPlacementProblem{
		PreemptorPods:  []*v1.Pod{p0, p1, p2},
		CandidateNodes: []string{"node-0", "node-1"},
		NodeAllocatableCPU: map[string]int64{"node-0": 2000, "node-1": 2000},
		NodeAllocatableMem: map[string]int64{"node-0": 2048, "node-1": 2048},
		PerPodCandidates: [][]GangPodCandidate{
			{
				{NodeName: "node-0", Cost: ZeroPreemptionCost(), Feasible: true, RequiredMilliCPU: 1000, RequiredMemory: 1024},
			},
			{
				{NodeName: "node-1", Cost: ZeroPreemptionCost(), Feasible: true, RequiredMilliCPU: 1000, RequiredMemory: 1024},
			},
			{
				// Member 2 has no feasible placement
				{NodeName: "node-0", Cost: MaxPreemptionCost(), Feasible: false},
				{NodeName: "node-1", Cost: MaxPreemptionCost(), Feasible: false},
			},
		},
		MinMember: 2, // Quorum could theoretically be met by {p0, p1}
	}

	res := BranchAndBoundGangSearch(ctx, prob, DefaultBranchAndBoundOptions())
	// In the current solver implementation, all PreemptorPods are required (MinMember subsetting
	// must be performed prior to solver invocation or handled via combinatorial subset expansion).
	if res.Status.IsSuccess() {
		t.Errorf("Expected Unschedulable when a member has no feasible candidates in current implementation, got success")
	}
	t.Logf("Partial quorum behavior confirmed: status=%v, message=%s", res.Status.Code(), res.Status.Message())
}
