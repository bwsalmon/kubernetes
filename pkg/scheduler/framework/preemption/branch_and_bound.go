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
	"sort"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	fwk "k8s.io/kube-scheduler/framework"
)

// PreemptionCost encapsulates the multi-metric objective vector used in candidate node selection.
// Metrics strictly adhere to the 4-tier cascade in OrderedScoreFuncs / pickOneNodeForPreemption:
// Tier 1: Minimum count of PDB-violating victims (PDBViolations)
// Tier 2: Minimum highest priority among all evicted victims (HighestPriority)
// Tier 3: Minimum sum of victim priorities normalized by (MaxInt32 + 1) (SumPriorities)
// Tier 4: Minimum total number of evicted victim pods (VictimCount)
type PreemptionCost struct {
	PDBViolations   int64
	HighestPriority int64
	SumPriorities   int64
	VictimCount     int64
}

// ZeroPreemptionCost returns an empty cost representing zero disruption.
func ZeroPreemptionCost() PreemptionCost {
	return PreemptionCost{
		PDBViolations:   0,
		HighestPriority: math.MinInt64,
		SumPriorities:   0,
		VictimCount:     0,
	}
}

// MaxPreemptionCost returns an infinite upper bound cost.
func MaxPreemptionCost() PreemptionCost {
	return PreemptionCost{
		PDBViolations:   math.MaxInt64,
		HighestPriority: math.MaxInt64,
		SumPriorities:   math.MaxInt64,
		VictimCount:     math.MaxInt64,
	}
}

// Less returns true if c is strictly preferable to o under OrderedScoreFuncs lexicographic ordering.
func (c PreemptionCost) Less(o PreemptionCost) bool {
	if c.PDBViolations != o.PDBViolations {
		return c.PDBViolations < o.PDBViolations
	}
	if c.HighestPriority != o.HighestPriority {
		return c.HighestPriority < o.HighestPriority
	}
	if c.SumPriorities != o.SumPriorities {
		return c.SumPriorities < o.SumPriorities
	}
	return c.VictimCount < o.VictimCount
}

// Equal returns true if c and o have identical cost metrics.
func (c PreemptionCost) Equal(o PreemptionCost) bool {
	return c.PDBViolations == o.PDBViolations &&
		c.HighestPriority == o.HighestPriority &&
		c.SumPriorities == o.SumPriorities &&
		c.VictimCount == o.VictimCount
}

// Compare returns -1 if c < o, 1 if c > o, and 0 if c == o.
func (c PreemptionCost) Compare(o PreemptionCost) int {
	if c.Less(o) {
		return -1
	}
	if o.Less(c) {
		return 1
	}
	return 0
}

// Add combines two costs by summing additive metrics and taking max of highest priority.
func (c PreemptionCost) Add(o PreemptionCost) PreemptionCost {
	maxPriority := c.HighestPriority
	if o.HighestPriority > maxPriority {
		maxPriority = o.HighestPriority
	}
	return PreemptionCost{
		PDBViolations:   c.PDBViolations + o.PDBViolations,
		HighestPriority: maxPriority,
		SumPriorities:   c.SumPriorities + o.SumPriorities,
		VictimCount:     c.VictimCount + o.VictimCount,
	}
}

// ComputeVictimsCost calculates the PreemptionCost of a given slice of victim pods and PDB violation count.
func ComputeVictimsCost(victims []*v1.Pod, pdbViolations int) PreemptionCost {
	if len(victims) == 0 {
		return ZeroPreemptionCost()
	}

	var highestPriority int64 = math.MinInt64
	var sumPriorities int64
	for _, p := range victims {
		prio := int64(corev1helpers.PodPriority(p))
		if prio > highestPriority {
			highestPriority = prio
		}
		// Adjust priority with MaxInt32 + 1 to keep non-negative as in pickOneNodeForPreemption
		sumPriorities += prio + int64(math.MaxInt32+1)
	}

	return PreemptionCost{
		PDBViolations:   int64(pdbViolations),
		HighestPriority: highestPriority,
		SumPriorities:   sumPriorities,
		VictimCount:     int64(len(victims)),
	}
}

// TopologySpreadConstraint models domain constraints (e.g. topology.kubernetes.io/zone).
type TopologySpreadConstraint struct {
	TopologyKey string
	MaxSkew     int
	NodeDomains map[string]string // nodeName -> domainValue
}

// ValidateSkew checks if the distribution of pods across domains satisfies maxSkew.
func (t *TopologySpreadConstraint) ValidateSkew(domainCounts map[string]int) bool {
	if len(domainCounts) <= 1 || t.MaxSkew <= 0 {
		return true
	}
	minVal := math.MaxInt32
	maxVal := math.MinInt32
	for _, count := range domainCounts {
		if count < minVal {
			minVal = count
		}
		if count > maxVal {
			maxVal = count
		}
	}
	return (maxVal - minVal) <= t.MaxSkew
}

// GangPodCandidate represents a feasible candidate placement for pod i on node j.
type GangPodCandidate struct {
	NodeName       string
	Cost           PreemptionCost
	Victims        []*v1.Pod
	PDBViolations  int
	DomainVictims  []Victim
	Feasible       bool
	RequiredMilliCPU int64
	RequiredMemory   int64
}

// GangPlacementProblem encapsulates the full problem space for gang preemption placement.
type GangPlacementProblem struct {
	PreemptorPods       []*v1.Pod
	CandidateNodes      []string
	NodeAllocatableCPU  map[string]int64 // Millicores available for preemptor pods
	NodeAllocatableMem  map[string]int64 // Memory bytes available for preemptor pods
	NodeMaxPods         map[string]int
	PerPodCandidates    [][]GangPodCandidate // [podIndex][candidateIndex]
	TopologyConstraints []TopologySpreadConstraint
	MinMember           int
}

// GangPlacementResult stores the output of the multi-node gang placement search.
type GangPlacementResult struct {
	PodAssignments      map[types.UID]string // pod UID -> assigned node name
	NodeAssignments     map[string][]*v1.Pod // node name -> assigned preemptor pods
	TotalCost           PreemptionCost
	SelectedVictims     []*v1.Pod
	SelectedVictimUIDs  sets.Set[types.UID]
	StatesExplored      int
	BranchesPruned      int
	EarlyExitTriggered  bool
	SearchDuration      time.Duration
	Status              *fwk.Status
}

// BranchAndBoundOptions configures branch and bound search execution.
type BranchAndBoundOptions struct {
	EnableEarlyExit          bool
	UseGreedyUpperBound      bool
	UseBestFirstBranchOrder  bool
}

// DefaultBranchAndBoundOptions provides standard optimal configuration.
func DefaultBranchAndBoundOptions() BranchAndBoundOptions {
	return BranchAndBoundOptions{
		EnableEarlyExit:         true,
		UseGreedyUpperBound:     true,
		UseBestFirstBranchOrder: true,
	}
}

// ExhaustiveGangSearch evaluates all candidate assignments by exhaustive combinatorial search.
// Used as the exact ground-truth baseline oracle for validating BranchAndBound equivalence.
func ExhaustiveGangSearch(ctx context.Context, prob *GangPlacementProblem) *GangPlacementResult {
	startTime := time.Now()
	numPods := len(prob.PreemptorPods)
	if numPods == 0 {
		return &GangPlacementResult{
			PodAssignments:  make(map[types.UID]string),
			NodeAssignments: make(map[string][]*v1.Pod),
			TotalCost:       ZeroPreemptionCost(),
			SearchDuration:  time.Since(startTime),
			Status:          fwk.NewStatus(fwk.Success),
		}
	}

	bestCost := MaxPreemptionCost()
	var bestAssignments []int
	statesExplored := 0

	// Current assignment vector: current[podIdx] = candidateIdx in prob.PerPodCandidates[podIdx]
	current := make([]int, numPods)

	var search func(podIdx int)
	search = func(podIdx int) {
		if podIdx == numPods {
			statesExplored++
			// Validate feasibility and compute total cost of the complete assignment
			valid, cost := evaluatePlacement(prob, current)
			if valid {
				if cost.Less(bestCost) {
					bestCost = cost
					bestAssignments = make([]int, numPods)
					copy(bestAssignments, current)
				}
			}
			return
		}

		numCandidates := len(prob.PerPodCandidates[podIdx])
		for candIdx := 0; candIdx < numCandidates; candIdx++ {
			if !prob.PerPodCandidates[podIdx][candIdx].Feasible {
				continue
			}
			current[podIdx] = candIdx
			search(podIdx + 1)
		}
	}

	search(0)

	if bestCost.Equal(MaxPreemptionCost()) {
		return &GangPlacementResult{
			StatesExplored: statesExplored,
			SearchDuration: time.Since(startTime),
			Status:         fwk.NewStatus(fwk.Unschedulable, "No feasible gang placement found"),
		}
	}

	res := buildPlacementResult(prob, bestAssignments, bestCost, statesExplored, 0, false, startTime)
	return res
}

// BranchAndBoundGangSearch computes the globally optimal gang placement using
// Admissible Lower Bound heuristic pruning with greedy initial upper bound.
func BranchAndBoundGangSearch(ctx context.Context, prob *GangPlacementProblem, opts BranchAndBoundOptions) *GangPlacementResult {
	startTime := time.Now()
	numPods := len(prob.PreemptorPods)
	if numPods == 0 {
		return &GangPlacementResult{
			PodAssignments:  make(map[types.UID]string),
			NodeAssignments: make(map[string][]*v1.Pod),
			TotalCost:       ZeroPreemptionCost(),
			SearchDuration:  time.Since(startTime),
			Status:          fwk.NewStatus(fwk.Success),
		}
	}

	// 1. Precompute per-pod minimum feasible lower-bound costs
	minPodCost := make([]PreemptionCost, numPods)
	for i := 0; i < numPods; i++ {
		minC := MaxPreemptionCost()
		for _, cand := range prob.PerPodCandidates[i] {
			if cand.Feasible && cand.Cost.Less(minC) {
				minC = cand.Cost
			}
		}
		if minC.Equal(MaxPreemptionCost()) {
			// A required pod has zero feasible candidate nodes
			return &GangPlacementResult{
				SearchDuration: time.Since(startTime),
				Status:         fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("pod %s has no feasible candidate nodes", prob.PreemptorPods[i].Name)),
			}
		}
		minPodCost[i] = minC
	}

	// 2. Precompute suffix lower bound sums for O(1) admissible bound calculations:
	// suffixLB[k] = optimistic lower bound of remaining pods [k .. numPods-1]
	suffixLB := make([]PreemptionCost, numPods+1)
	suffixLB[numPods] = ZeroPreemptionCost()
	for i := numPods - 1; i >= 0; i-- {
		suffixLB[i] = suffixLB[i+1].Add(minPodCost[i])
	}
	globalTheoreticalLowerBound := suffixLB[0]

	// 3. Compute Greedy Initial Upper Bound (Incumbent Best)
	bestCost := MaxPreemptionCost()
	var bestAssignments []int
	if opts.UseGreedyUpperBound {
		greedyAssignments, greedyCost, found := computeGreedyPlacement(prob)
		if found {
			bestCost = greedyCost
			bestAssignments = greedyAssignments
			// If greedy solution already matches the global theoretical minimum, we can exit early!
			if opts.EnableEarlyExit && bestCost.Equal(globalTheoreticalLowerBound) {
				return buildPlacementResult(prob, bestAssignments, bestCost, 1, 0, true, startTime)
			}
		}
	}

	statesExplored := 0
	branchesPruned := 0
	earlyExit := false

	// Candidate ordering: sort candidate options by ascending individual cost to maximize early pruning
	orderedCandidates := make([][]int, numPods)
	for i := 0; i < numPods; i++ {
		indices := make([]int, 0, len(prob.PerPodCandidates[i]))
		for idx, cand := range prob.PerPodCandidates[i] {
			if cand.Feasible {
				indices = append(indices, idx)
			}
		}
		if opts.UseBestFirstBranchOrder {
			sort.Slice(indices, func(a, b int) bool {
				return prob.PerPodCandidates[i][indices[a]].Cost.Less(prob.PerPodCandidates[i][indices[b]].Cost)
			})
		}
		orderedCandidates[i] = indices
	}

	current := make([]int, numPods)

	// Track state along search branch for O(1) constraint & partial cost updates
	nodeUsedCPU := make(map[string]int64)
	nodeUsedMem := make(map[string]int64)
	nodePodCount := make(map[string]int)
	nodeVictimSets := make(map[string]sets.Set[types.UID])
	nodeVictimPods := make(map[string][]*v1.Pod)
	nodePDBViolations := make(map[string]int)

	// Topology domain counts: [constraintIdx][domainName] -> count
	topoCounts := make([]map[string]int, len(prob.TopologyConstraints))
	for tIdx := range prob.TopologyConstraints {
		topoCounts[tIdx] = make(map[string]int)
	}

	var search func(podIdx int, currentCost PreemptionCost)
	search = func(podIdx int, currentCost PreemptionCost) {
		if earlyExit {
			return
		}

		if podIdx == numPods {
			statesExplored++
			if currentCost.Less(bestCost) {
				bestCost = currentCost
				bestAssignments = make([]int, numPods)
				copy(bestAssignments, current)
				if opts.EnableEarlyExit && bestCost.Equal(globalTheoreticalLowerBound) {
					earlyExit = true
				}
			}
			return
		}

		// Admissible Lower Bound Pruning:
		// Lower bound for complete assignment through this branch:
		// LB = currentCost + suffixLB[podIdx]
		admissibleLB := currentCost.Add(suffixLB[podIdx])
		if !bestCost.Equal(MaxPreemptionCost()) && !admissibleLB.Less(bestCost) {
			branchesPruned++
			return
		}

		for _, candIdx := range orderedCandidates[podIdx] {
			if earlyExit {
				return
			}
			cand := prob.PerPodCandidates[podIdx][candIdx]
			nodeName := cand.NodeName

			// 1. Check Node Capacity Constraints
			if prob.NodeAllocatableCPU != nil {
				if nodeUsedCPU[nodeName]+cand.RequiredMilliCPU > prob.NodeAllocatableCPU[nodeName] {
					continue
				}
			}
			if prob.NodeAllocatableMem != nil {
				if nodeUsedMem[nodeName]+cand.RequiredMemory > prob.NodeAllocatableMem[nodeName] {
					continue
				}
			}
			if prob.NodeMaxPods != nil {
				if nodePodCount[nodeName]+1 > prob.NodeMaxPods[nodeName] {
					continue
				}
			}

			// 2. Check Topology Spread Constraints
			topoFeasible := true
			for tIdx, tc := range prob.TopologyConstraints {
				domain := tc.NodeDomains[nodeName]
				if domain != "" {
					topoCounts[tIdx][domain]++
					if !tc.ValidateSkew(topoCounts[tIdx]) {
						topoCounts[tIdx][domain]--
						topoFeasible = false
						break
					}
					topoCounts[tIdx][domain]--
				}
			}
			if !topoFeasible {
				continue
			}

			// 3. Compute incremental preemption cost on this node
			prevVictims := nodeVictimPods[nodeName]
			prevPDB := nodePDBViolations[nodeName]
			prevCost := ComputeVictimsCost(prevVictims, prevPDB)

			// Combine victims on node
			existingSet, hasSet := nodeVictimSets[nodeName]
			var nextSet sets.Set[types.UID]
			if hasSet {
				nextSet = existingSet.Clone()
			} else {
				nextSet = sets.New[types.UID]()
			}

			var newlyAddedVictims []*v1.Pod
			for _, v := range cand.Victims {
				if !nextSet.Has(v.UID) {
					nextSet.Insert(v.UID)
					newlyAddedVictims = append(newlyAddedVictims, v)
				}
			}
			nextVictims := append(append([]*v1.Pod(nil), prevVictims...), newlyAddedVictims...)
			nextPDB := prevPDB + cand.PDBViolations
			nextCost := ComputeVictimsCost(nextVictims, nextPDB)

			// Delta cost = nextCost - prevCost (or recompute partial global cost)
			// For admissible bounding, calculate updated global partial cost
			newCurrentCost := currentCost
			// Adjust global cost by subtracting prev node cost and adding next node cost
			newCurrentCost.PDBViolations += (nextCost.PDBViolations - prevCost.PDBViolations)
			newCurrentCost.SumPriorities += (nextCost.SumPriorities - prevCost.SumPriorities)
			newCurrentCost.VictimCount += (nextCost.VictimCount - prevCost.VictimCount)
			if nextCost.HighestPriority > newCurrentCost.HighestPriority {
				newCurrentCost.HighestPriority = nextCost.HighestPriority
			}

			// Check Branch-and-Bound pruning after applying node assignment
			newAdmissibleLB := newCurrentCost.Add(suffixLB[podIdx+1])
			if !bestCost.Equal(MaxPreemptionCost()) && !newAdmissibleLB.Less(bestCost) {
				branchesPruned++
				continue
			}

			// Apply state mutation
			current[podIdx] = candIdx
			nodeUsedCPU[nodeName] += cand.RequiredMilliCPU
			nodeUsedMem[nodeName] += cand.RequiredMemory
			nodePodCount[nodeName]++
			nodeVictimSets[nodeName] = nextSet
			nodeVictimPods[nodeName] = nextVictims
			nodePDBViolations[nodeName] = nextPDB
			for tIdx, tc := range prob.TopologyConstraints {
				domain := tc.NodeDomains[nodeName]
				if domain != "" {
					topoCounts[tIdx][domain]++
				}
			}

			// Recurse to next pod
			search(podIdx+1, newCurrentCost)

			// Revert state mutation
			nodeUsedCPU[nodeName] -= cand.RequiredMilliCPU
			nodeUsedMem[nodeName] -= cand.RequiredMemory
			nodePodCount[nodeName]--
			if hasSet {
				nodeVictimSets[nodeName] = existingSet
			} else {
				delete(nodeVictimSets, nodeName)
			}
			nodeVictimPods[nodeName] = prevVictims
			nodePDBViolations[nodeName] = prevPDB
			for tIdx, tc := range prob.TopologyConstraints {
				domain := tc.NodeDomains[nodeName]
				if domain != "" {
					topoCounts[tIdx][domain]--
				}
			}
		}
	}

	search(0, ZeroPreemptionCost())

	if bestCost.Equal(MaxPreemptionCost()) {
		return &GangPlacementResult{
			StatesExplored: statesExplored,
			BranchesPruned: branchesPruned,
			SearchDuration: time.Since(startTime),
			Status:         fwk.NewStatus(fwk.Unschedulable, "No feasible gang placement found"),
		}
	}

	res := buildPlacementResult(prob, bestAssignments, bestCost, statesExplored, branchesPruned, earlyExit, startTime)
	return res
}

// computeGreedyPlacement finds an initial feasible placement using greedy best-candidate assignment.
func computeGreedyPlacement(prob *GangPlacementProblem) ([]int, PreemptionCost, bool) {
	numPods := len(prob.PreemptorPods)
	assignments := make([]int, numPods)

	nodeUsedCPU := make(map[string]int64)
	nodeUsedMem := make(map[string]int64)
	nodePodCount := make(map[string]int)
	topoCounts := make([]map[string]int, len(prob.TopologyConstraints))
	for tIdx := range prob.TopologyConstraints {
		topoCounts[tIdx] = make(map[string]int)
	}

	for podIdx := 0; podIdx < numPods; podIdx++ {
		bestCandIdx := -1
		bestIncrementalCost := MaxPreemptionCost()

		for candIdx, cand := range prob.PerPodCandidates[podIdx] {
			if !cand.Feasible {
				continue
			}
			nodeName := cand.NodeName

			// Capacity check
			if prob.NodeAllocatableCPU != nil && nodeUsedCPU[nodeName]+cand.RequiredMilliCPU > prob.NodeAllocatableCPU[nodeName] {
				continue
			}
			if prob.NodeAllocatableMem != nil && nodeUsedMem[nodeName]+cand.RequiredMemory > prob.NodeAllocatableMem[nodeName] {
				continue
			}
			if prob.NodeMaxPods != nil && nodePodCount[nodeName]+1 > prob.NodeMaxPods[nodeName] {
				continue
			}

			// Topology check
			topoFeasible := true
			for tIdx, tc := range prob.TopologyConstraints {
				domain := tc.NodeDomains[nodeName]
				if domain != "" {
					topoCounts[tIdx][domain]++
					if !tc.ValidateSkew(topoCounts[tIdx]) {
						topoCounts[tIdx][domain]--
						topoFeasible = false
						break
					}
					topoCounts[tIdx][domain]--
				}
			}
			if !topoFeasible {
				continue
			}

			if cand.Cost.Less(bestIncrementalCost) {
				bestIncrementalCost = cand.Cost
				bestCandIdx = candIdx
			}
		}

		if bestCandIdx == -1 {
			return nil, MaxPreemptionCost(), false
		}

		assignments[podIdx] = bestCandIdx
		cand := prob.PerPodCandidates[podIdx][bestCandIdx]
		nodeUsedCPU[cand.NodeName] += cand.RequiredMilliCPU
		nodeUsedMem[cand.NodeName] += cand.RequiredMemory
		nodePodCount[cand.NodeName]++
		for tIdx, tc := range prob.TopologyConstraints {
			domain := tc.NodeDomains[cand.NodeName]
			if domain != "" {
				topoCounts[tIdx][domain]++
			}
		}
	}

	valid, totalCost := evaluatePlacement(prob, assignments)
	if !valid {
		return nil, MaxPreemptionCost(), false
	}
	return assignments, totalCost, true
}

// evaluatePlacement computes validity and exact combined preemption cost for a candidate assignment vector.
func evaluatePlacement(prob *GangPlacementProblem, assignments []int) (bool, PreemptionCost) {
	nodeUsedCPU := make(map[string]int64)
	nodeUsedMem := make(map[string]int64)
	nodePodCount := make(map[string]int)
	topoCounts := make([]map[string]int, len(prob.TopologyConstraints))
	for tIdx := range prob.TopologyConstraints {
		topoCounts[tIdx] = make(map[string]int)
	}

	nodeVictimSets := make(map[string]sets.Set[types.UID])
	nodeVictimPods := make(map[string][]*v1.Pod)
	nodePDBViolations := make(map[string]int)

	for podIdx, candIdx := range assignments {
		cand := prob.PerPodCandidates[podIdx][candIdx]
		if !cand.Feasible {
			return false, MaxPreemptionCost()
		}
		nodeName := cand.NodeName

		// Capacity
		if prob.NodeAllocatableCPU != nil && nodeUsedCPU[nodeName]+cand.RequiredMilliCPU > prob.NodeAllocatableCPU[nodeName] {
			return false, MaxPreemptionCost()
		}
		if prob.NodeAllocatableMem != nil && nodeUsedMem[nodeName]+cand.RequiredMemory > prob.NodeAllocatableMem[nodeName] {
			return false, MaxPreemptionCost()
		}
		if prob.NodeMaxPods != nil && nodePodCount[nodeName]+1 > prob.NodeMaxPods[nodeName] {
			return false, MaxPreemptionCost()
		}

		nodeUsedCPU[nodeName] += cand.RequiredMilliCPU
		nodeUsedMem[nodeName] += cand.RequiredMemory
		nodePodCount[nodeName]++

		// Topology
		for tIdx, tc := range prob.TopologyConstraints {
			domain := tc.NodeDomains[nodeName]
			if domain != "" {
				topoCounts[tIdx][domain]++
			}
		}

		// Victims aggregation
		if _, ok := nodeVictimSets[nodeName]; !ok {
			nodeVictimSets[nodeName] = sets.New[types.UID]()
		}
		for _, v := range cand.Victims {
			if !nodeVictimSets[nodeName].Has(v.UID) {
				nodeVictimSets[nodeName].Insert(v.UID)
				nodeVictimPods[nodeName] = append(nodeVictimPods[nodeName], v)
			}
		}
		nodePDBViolations[nodeName] += cand.PDBViolations
	}

	// Validate all topology constraints
	for tIdx, tc := range prob.TopologyConstraints {
		if !tc.ValidateSkew(topoCounts[tIdx]) {
			return false, MaxPreemptionCost()
		}
	}

	// Calculate exact total cost
	totalCost := ZeroPreemptionCost()
	for nodeName, vPods := range nodeVictimPods {
		nodeCost := ComputeVictimsCost(vPods, nodePDBViolations[nodeName])
		totalCost = totalCost.Add(nodeCost)
	}

	return true, totalCost
}

// buildPlacementResult formats the final GangPlacementResult.
func buildPlacementResult(prob *GangPlacementProblem, assignments []int, cost PreemptionCost, explored, pruned int, earlyExit bool, startTime time.Time) *GangPlacementResult {
	podAssignments := make(map[types.UID]string)
	nodeAssignments := make(map[string][]*v1.Pod)
	victimSet := sets.New[types.UID]()
	var victimPods []*v1.Pod

	for podIdx, candIdx := range assignments {
		pod := prob.PreemptorPods[podIdx]
		cand := prob.PerPodCandidates[podIdx][candIdx]
		podAssignments[pod.UID] = cand.NodeName
		nodeAssignments[cand.NodeName] = append(nodeAssignments[cand.NodeName], pod)

		for _, v := range cand.Victims {
			if !victimSet.Has(v.UID) {
				victimSet.Insert(v.UID)
				victimPods = append(victimPods, v)
			}
		}
	}

	return &GangPlacementResult{
		PodAssignments:     podAssignments,
		NodeAssignments:    nodeAssignments,
		TotalCost:          cost,
		SelectedVictims:    victimPods,
		SelectedVictimUIDs: victimSet,
		StatesExplored:     explored,
		BranchesPruned:     pruned,
		EarlyExitTriggered: earlyExit,
		SearchDuration:     time.Since(startTime),
		Status:             fwk.NewStatus(fwk.Success),
	}
}
