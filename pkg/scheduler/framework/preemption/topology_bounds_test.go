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
	"math"
	"testing"
)

// bruteForceFeasible checks if remainingPods can be partitioned into D domains
// such that max(A_d + x_d) - min(A_d + x_d) <= maxSkew.
func bruteForceFeasible(initialCounts []int, remainingPods int, maxSkew int) bool {
	numDomains := len(initialCounts)
	if numDomains <= 1 || maxSkew <= 0 {
		return true
	}

	var canPartition func(domainIdx int, podsLeft int, current []int) bool
	canPartition = func(domainIdx int, podsLeft int, current []int) bool {
		if domainIdx == numDomains-1 {
			current[domainIdx] += podsLeft
			minVal := math.MaxInt32
			maxVal := math.MinInt32
			for _, c := range current {
				if c < minVal {
					minVal = c
				}
				if c > maxVal {
					maxVal = c
				}
			}
			current[domainIdx] -= podsLeft
			return (maxVal - minVal) <= maxSkew
		}

		for p := 0; p <= podsLeft; p++ {
			current[domainIdx] += p
			if canPartition(domainIdx+1, podsLeft-p, current) {
				current[domainIdx] -= p
				return true
			}
			current[domainIdx] -= p
		}
		return false
	}

	work := make([]int, numDomains)
	copy(work, initialCounts)
	return canPartition(0, remainingPods, work)
}

func TestCheckFeasibility_OracleEquivalence(t *testing.T) {
	testCases := []struct {
		name          string
		initial       map[string]int
		remainingPods int
		maxSkew       int
	}{
		{
			name:          "3 zones, 0 initial, 4 pods, maxSkew=1 (Feasible: 2,1,1)",
			initial:       map[string]int{"zone-a": 0, "zone-b": 0, "zone-c": 0},
			remainingPods: 4,
			maxSkew:       1,
		},
		{
			name:          "3 zones, 0 initial, 6 pods, maxSkew=1 (Feasible: 2,2,2)",
			initial:       map[string]int{"zone-a": 0, "zone-b": 0, "zone-c": 0},
			remainingPods: 6,
			maxSkew:       1,
		},
		{
			name:          "3 zones, skewed initial (2,0,0), 4 pods, maxSkew=1 (Feasible: 2,2,2)",
			initial:       map[string]int{"zone-a": 2, "zone-b": 0, "zone-c": 0},
			remainingPods: 4,
			maxSkew:       1,
		},
		{
			name:          "3 zones, heavily skewed initial (5,0,0), 2 pods, maxSkew=1 (Infeasible)",
			initial:       map[string]int{"zone-a": 5, "zone-b": 0, "zone-c": 0},
			remainingPods: 2,
			maxSkew:       1,
		},
		{
			name:          "3 zones, (3,0,0), 3 pods, maxSkew=1 (Feasible: 3,2,1 or 3,1,2)",
			initial:       map[string]int{"zone-a": 3, "zone-b": 0, "zone-c": 0},
			remainingPods: 3,
			maxSkew:       1,
		},
		{
			name:          "4 zones, (0,0,0,0), 7 pods, maxSkew=1 (Feasible: 2,2,2,1)",
			initial:       map[string]int{"zone-a": 0, "zone-b": 0, "zone-c": 0, "zone-d": 0},
			remainingPods: 7,
			maxSkew:       1,
		},
		{
			name:          "4 zones, (4,0,0,0), 4 pods, maxSkew=1 (Infeasible)",
			initial:       map[string]int{"zone-a": 4, "zone-b": 0, "zone-c": 0, "zone-d": 0},
			remainingPods: 4,
			maxSkew:       1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			domSlice := make([]string, 0, len(tc.initial))
			initSlice := make([]int, 0, len(tc.initial))
			for d := range tc.initial {
				domSlice = append(domSlice, d)
				initSlice = append(initSlice, tc.initial[d])
			}

			constraint := TopologySpreadConstraint{
				TopologyKey:         "topology.kubernetes.io/zone",
				MaxSkew:             tc.maxSkew,
				Domains:             domSlice,
				InitialDomainCounts: tc.initial,
			}

			analyticalRes := constraint.CheckFeasibility(nil, tc.remainingPods)
			oracleRes := bruteForceFeasible(initSlice, tc.remainingPods, tc.maxSkew)

			if analyticalRes != oracleRes {
				t.Fatalf("Analytical feasibility mismatch for %s: analytical=%v, oracle=%v",
					tc.name, analyticalRes, oracleRes)
			}
		})
	}
}

func TestCanDomainAcceptPod_StrictAdmissibility(t *testing.T) {
	// Exhaustively verify strict mathematical admissibility across combinations
	domains := []string{"zone-a", "zone-b", "zone-c"}

	for m := 1; m <= 8; m++ {
		for s := 1; s <= 2; s++ {
			for initA := 0; initA <= 3; initA++ {
				for initB := 0; initB <= 3; initB++ {
					for initC := 0; initC <= 3; initC++ {
						initial := map[string]int{"zone-a": initA, "zone-b": initB, "zone-c": initC}
						constraint := TopologySpreadConstraint{
							TopologyKey:         "topology.kubernetes.io/zone",
							MaxSkew:             s,
							Domains:             domains,
							InitialDomainCounts: initial,
						}

						for _, target := range domains {
							analytical := constraint.CanDomainAcceptPod(target, nil, m)

							// Oracle: can we place 1 in target and partition (m-1) across domains?
							oracleInit := []int{initA, initB, initC}
							switch target {
							case "zone-a":
								oracleInit[0]++
							case "zone-b":
								oracleInit[1]++
							case "zone-c":
								oracleInit[2]++
							}
							oracle := bruteForceFeasible(oracleInit, m-1, s)

							if analytical != oracle {
								t.Fatalf("Strict admissibility violated for M=%d, S=%d, init=(%d,%d,%d), target=%s: analytical=%v, oracle=%v",
									m, s, initA, initB, initC, target, analytical, oracle)
							}
						}
					}
				}
			}
		}
	}
}

func TestComputeDomainBounds(t *testing.T) {
	domains := []string{"zone-a", "zone-b", "zone-c"}
	constraint := TopologySpreadConstraint{
		TopologyKey: "topology.kubernetes.io/zone",
		MaxSkew:     1,
		Domains:     domains,
		InitialDomainCounts: map[string]int{
			"zone-a": 0,
			"zone-b": 0,
			"zone-c": 0,
		},
	}

	low, up := constraint.ComputeDomainBounds(nil, 4)
	// For M=4, S=1 across 3 zones:
	// Allowed distributions are permutations of (2, 1, 1).
	// So each zone can receive at least 1 and at most 2 pods.
	for _, d := range domains {
		if low[d] != 1 {
			t.Errorf("Expected domain %s lower bound to be 1, got %d", d, low[d])
		}
		if up[d] != 2 {
			t.Errorf("Expected domain %s upper bound to be 2, got %d", d, up[d])
		}
	}
}
