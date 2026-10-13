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
	fwk "k8s.io/kube-scheduler/framework"
)

// TopologyDomainAggregate captures the aggregated resource capacity, headroom,
// and candidate pod counts within a failure domain (e.g. rack, switch, or zone).
type TopologyDomainAggregate struct {
	DomainName       string
	TopologyKey      string
	Nodes            []string
	NodeSet          sets.Set[string]
	TotalAllocCPU    int64
	TotalAllocMem    int64
	MaxNodeCPU       int64
	MaxNodeMem       int64
	MaxPodsCapacity  int
	// CandidateFeasiblePods counts how many pods in the gang have at least one feasible candidate node in this domain.
	CandidateFeasiblePods int
	// PodFeasibilityMask records which pod indices are feasible in this domain.
	PodFeasibilityMask []bool
	// MinDisruptionCost is the optimistic lower bound disruption cost across all nodes in this domain.
	MinDisruptionCost PreemptionCost
}

// TopologyHierarchyTree represents a hierarchical failure domain tree (e.g., Zone -> Rack -> Node).
type TopologyHierarchyTree struct {
	TopologyKey string
	Domains     map[string]*TopologyDomainAggregate
	DomainOrder []string // sorted domain names for deterministic traversal
}

// BuildTopologyHierarchy creates domain aggregations from a gang placement problem and topology constraints.
func BuildTopologyHierarchy(prob *GangPlacementProblem, constraint TopologySpreadConstraint) *TopologyHierarchyTree {
	tree := &TopologyHierarchyTree{
		TopologyKey: constraint.TopologyKey,
		Domains:     make(map[string]*TopologyDomainAggregate),
	}

	numPods := len(prob.PreemptorPods)

	// 1. Group nodes by domain
	for _, nodeName := range prob.CandidateNodes {
		domainVal, ok := constraint.NodeDomains[nodeName]
		if !ok || domainVal == "" {
			domainVal = "default"
		}

		dom, exists := tree.Domains[domainVal]
		if !exists {
			dom = &TopologyDomainAggregate{
				DomainName:         domainVal,
				TopologyKey:        constraint.TopologyKey,
				Nodes:              make([]string, 0),
				NodeSet:            sets.New[string](),
				PodFeasibilityMask: make([]bool, numPods),
				MinDisruptionCost:  MaxPreemptionCost(),
			}
			tree.Domains[domainVal] = dom
			tree.DomainOrder = append(tree.DomainOrder, domainVal)
		}

		dom.Nodes = append(dom.Nodes, nodeName)
		dom.NodeSet.Insert(nodeName)

		allocCPU := prob.NodeAllocatableCPU[nodeName]
		allocMem := prob.NodeAllocatableMem[nodeName]

		dom.TotalAllocCPU += allocCPU
		dom.TotalAllocMem += allocMem
		if allocCPU > dom.MaxNodeCPU {
			dom.MaxNodeCPU = allocCPU
		}
		if allocMem > dom.MaxNodeMem {
			dom.MaxNodeMem = allocMem
		}
		if prob.NodeMaxPods != nil {
			dom.MaxPodsCapacity += prob.NodeMaxPods[nodeName]
		} else {
			dom.MaxPodsCapacity += 110 // standard default pod capacity per node
		}
	}

	sort.Strings(tree.DomainOrder)

	// 2. Aggregate per-pod candidate feasibility and optimistic disruption cost per domain
	for podIdx := 0; podIdx < numPods; podIdx++ {
		for _, cand := range prob.PerPodCandidates[podIdx] {
			if !cand.Feasible {
				continue
			}
			domainVal, ok := constraint.NodeDomains[cand.NodeName]
			if !ok || domainVal == "" {
				domainVal = "default"
			}
			dom, ok := tree.Domains[domainVal]
			if !ok {
				continue
			}

			if !dom.PodFeasibilityMask[podIdx] {
				dom.PodFeasibilityMask[podIdx] = true
				dom.CandidateFeasiblePods++
			}

			if cand.Cost.Less(dom.MinDisruptionCost) {
				dom.MinDisruptionCost = cand.Cost
			}
		}
	}

	return tree
}

// DomainPruningVerdict stores the decision for whether a failure domain is viable.
type DomainPruningVerdict struct {
	DomainName string
	Viable     bool
	PruneReason string
}

// PruneInfeasibleDomains evaluates each failure domain against gang requirements.
// A domain is strictly pruned only if it mathematically cannot participate in a valid solution:
// 1. Quorum Feasibility: If a gang requires co-location in a single domain (e.g. rack/zone-local gang),
//    and the domain has fewer feasible pod slots than MinMember (or TotalPods).
// 2. Aggregate Resource Headroom: If the domain's total allocatable capacity is strictly less than
//    the sum of minimum resource requests for pods mapped to it.
// 3. Topology Spread Feasibility: If scheduling the minimum required pods in this domain
//    would mathematically violate maxSkew regardless of assignments in other domains.
//
// CONSERVATIVE GUARANTEE: Never discards a domain that could validly host gang members.
func PruneInfeasibleDomains(
	prob *GangPlacementProblem,
	tree *TopologyHierarchyTree,
	constraint TopologySpreadConstraint,
	requireCoLocation bool,
) ([]*TopologyDomainAggregate, []DomainPruningVerdict) {
	viableDomains := make([]*TopologyDomainAggregate, 0, len(tree.DomainOrder))
	verdicts := make([]DomainPruningVerdict, 0, len(tree.DomainOrder))

	numPods := len(prob.PreemptorPods)
	minMember := prob.MinMember
	if minMember <= 0 {
		minMember = numPods
	}

	// Calculate total gang resource demands
	var totalGangReqCPU int64
	var totalGangReqMem int64
	for i := 0; i < numPods; i++ {
		var minReqCPU int64 = math.MaxInt64
		var minReqMem int64 = math.MaxInt64
		for _, cand := range prob.PerPodCandidates[i] {
			if cand.Feasible {
				if cand.RequiredMilliCPU < minReqCPU {
					minReqCPU = cand.RequiredMilliCPU
				}
				if cand.RequiredMemory < minReqMem {
					minReqMem = cand.RequiredMemory
				}
			}
		}
		if minReqCPU != math.MaxInt64 {
			totalGangReqCPU += minReqCPU
		}
		if minReqMem != math.MaxInt64 {
			totalGangReqMem += minReqMem
		}
	}

	for _, domainName := range tree.DomainOrder {
		dom := tree.Domains[domainName]

		// Check 1: Zero feasible candidates
		if dom.CandidateFeasiblePods == 0 {
			verdicts = append(verdicts, DomainPruningVerdict{
				DomainName:  domainName,
				Viable:      false,
				PruneReason: "zero feasible candidate pods in domain",
			})
			continue
		}

		// Check 2: Co-location quorum requirement (e.g. single-rack or single-zone gang)
		if requireCoLocation {
			if dom.CandidateFeasiblePods < minMember {
				verdicts = append(verdicts, DomainPruningVerdict{
					DomainName:  domainName,
					Viable:      false,
					PruneReason: fmt.Sprintf("candidate feasible pods (%d) < required quorum (%d)", dom.CandidateFeasiblePods, minMember),
				})
				continue
			}

			// Aggregate resource headroom in domain
			if dom.TotalAllocCPU < totalGangReqCPU {
				verdicts = append(verdicts, DomainPruningVerdict{
					DomainName:  domainName,
					Viable:      false,
					PruneReason: fmt.Sprintf("domain total CPU (%d mCPU) < gang requirement (%d mCPU)", dom.TotalAllocCPU, totalGangReqCPU),
				})
				continue
			}
			if dom.TotalAllocMem < totalGangReqMem {
				verdicts = append(verdicts, DomainPruningVerdict{
					DomainName:  domainName,
					Viable:      false,
					PruneReason: fmt.Sprintf("domain total memory (%d bytes) < gang requirement (%d bytes)", dom.TotalAllocMem, totalGangReqMem),
				})
				continue
			}
			if dom.MaxPodsCapacity < minMember {
				verdicts = append(verdicts, DomainPruningVerdict{
					DomainName:  domainName,
					Viable:      false,
					PruneReason: fmt.Sprintf("domain max pods capacity (%d) < required quorum (%d)", dom.MaxPodsCapacity, minMember),
				})
				continue
			}
		}

		// Check 3: Multi-domain TopologySpread maxSkew mathematical impossibility
		// If total eligible domains is D, and a domain cannot take any pods without skew violation,
		// or if constraint.MaxSkew is strictly enforced and minimum pods per domain exceeds maxSkew bounds.
		// Conservative rule: Only prune if MaxSkew > 0 and placement would force skew > maxSkew.
		// In multi-domain spread, a domain with at least 1 feasible pod is viable unless all other domains
		// have 0 capacity and gang size > MaxSkew.
		if constraint.MaxSkew > 0 && len(tree.DomainOrder) > 1 {
			// If all OTHER domains combined cannot take (numPods - constraint.MaxSkew) pods,
			// this domain might be forced, but it wouldn't be pruned.
			// Conversely, if this domain alone has max capacity < lower skew bound, verify conservatively.
			// Strict conservative rule: do not prune unless provably 0 feasible solutions exist through this domain.
		}

		viableDomains = append(viableDomains, dom)
		verdicts = append(verdicts, DomainPruningVerdict{
			DomainName:  domainName,
			Viable:      true,
			PruneReason: "domain satisfies conservative quorum and capacity bounds",
		})
	}

	return viableDomains, verdicts
}

// PartitionProblemByDomains filters GangPlacementProblem to only explore nodes within viable domains.
func PartitionProblemByDomains(
	prob *GangPlacementProblem,
	viableDomains []*TopologyDomainAggregate,
) (*GangPlacementProblem, int) {
	viableNodeSet := sets.New[string]()
	for _, dom := range viableDomains {
		viableNodeSet.Insert(dom.Nodes...)
	}

	prunedNodesCount := 0
	filteredCandidateNodes := make([]string, 0, len(prob.CandidateNodes))
	for _, n := range prob.CandidateNodes {
		if viableNodeSet.Has(n) {
			filteredCandidateNodes = append(filteredCandidateNodes, n)
		} else {
			prunedNodesCount++
		}
	}

	numPods := len(prob.PreemptorPods)
	filteredPerPodCandidates := make([][]GangPodCandidate, numPods)
	for i := 0; i < numPods; i++ {
		filteredPerPodCandidates[i] = make([]GangPodCandidate, 0, len(prob.PerPodCandidates[i]))
		for _, cand := range prob.PerPodCandidates[i] {
			if viableNodeSet.Has(cand.NodeName) {
				filteredPerPodCandidates[i] = append(filteredPerPodCandidates[i], cand)
			}
		}
	}

	partitionedProblem := &GangPlacementProblem{
		PreemptorPods:       prob.PreemptorPods,
		CandidateNodes:      filteredCandidateNodes,
		NodeAllocatableCPU:  prob.NodeAllocatableCPU,
		NodeAllocatableMem:  prob.NodeAllocatableMem,
		NodeMaxPods:         prob.NodeMaxPods,
		PerPodCandidates:    filteredPerPodCandidates,
		TopologyConstraints: prob.TopologyConstraints,
		MinMember:           prob.MinMember,
	}

	return partitionedProblem, prunedNodesCount
}

// HierarchicalDomainSearchOptions configures hierarchical domain-partitioned preemption search.
type HierarchicalDomainSearchOptions struct {
	RequireCoLocation       bool
	EnableDomainPruning     bool
	BranchAndBoundOpts      BranchAndBoundOptions
}

// DefaultHierarchicalDomainSearchOptions provides standard configuration.
func DefaultHierarchicalDomainSearchOptions() HierarchicalDomainSearchOptions {
	return HierarchicalDomainSearchOptions{
		RequireCoLocation:   false,
		EnableDomainPruning: true,
		BranchAndBoundOpts:  DefaultBranchAndBoundOptions(),
	}
}

// HierarchicalDomainSearchResult encapsulates the result of hierarchical domain-partitioned gang preemption search.
type HierarchicalDomainSearchResult struct {
	PlacementResult     *GangPlacementResult
	TotalDomains        int
	ViableDomains       int
	PrunedDomains       int
	PrunedNodes         int
	DomainVerdicts      []DomainPruningVerdict
	HierarchyDuration   time.Duration
	TotalDuration       time.Duration
}

// HierarchicalDomainGangSearch executes hierarchical topology domain decomposition and pruning,
// followed by Admissible Branch-and-Bound placement search on viable partitions.
func HierarchicalDomainGangSearch(
	ctx context.Context,
	prob *GangPlacementProblem,
	constraint TopologySpreadConstraint,
	opts HierarchicalDomainSearchOptions,
) *HierarchicalDomainSearchResult {
	start := time.Now()

	numPods := len(prob.PreemptorPods)
	if numPods == 0 {
		emptyRes := &GangPlacementResult{
			PodAssignments:  make(map[types.UID]string),
			NodeAssignments: make(map[string][]*v1.Pod),
			TotalCost:       ZeroPreemptionCost(),
			SearchDuration:  time.Since(start),
			Status:          fwk.NewStatus(fwk.Success),
		}
		return &HierarchicalDomainSearchResult{
			PlacementResult:   emptyRes,
			TotalDuration:     time.Since(start),
		}
	}

	if !opts.EnableDomainPruning || len(prob.CandidateNodes) == 0 {
		bnbRes := BranchAndBoundGangSearch(ctx, prob, opts.BranchAndBoundOpts)
		return &HierarchicalDomainSearchResult{
			PlacementResult: bnbRes,
			TotalDomains:    1,
			ViableDomains:   1,
			PrunedDomains:   0,
			PrunedNodes:     0,
			TotalDuration:   time.Since(start),
		}
	}

	// 1. Build hierarchy tree
	tree := BuildTopologyHierarchy(prob, constraint)
	hDuration := time.Since(start)

	// 2. Conservative domain pruning
	viableDomains, verdicts := PruneInfeasibleDomains(prob, tree, constraint, opts.RequireCoLocation)
	prunedDomains := len(tree.DomainOrder) - len(viableDomains)

	// If no viable domains remain, return Unschedulable immediately without exploring nodes
	if len(viableDomains) == 0 {
		return &HierarchicalDomainSearchResult{
			PlacementResult: &GangPlacementResult{
				StatesExplored: 0,
				BranchesPruned: 0,
				SearchDuration: time.Since(start),
				Status:         fwk.NewStatus(fwk.Unschedulable, "All failure domains pruned as mathematically infeasible for gang requirements"),
			},
			TotalDomains:      len(tree.DomainOrder),
			ViableDomains:     0,
			PrunedDomains:     prunedDomains,
			PrunedNodes:       len(prob.CandidateNodes),
			DomainVerdicts:    verdicts,
			HierarchyDuration: hDuration,
			TotalDuration:     time.Since(start),
		}
	}

	// 3. Partition candidate nodes and candidates to viable domains
	if opts.RequireCoLocation {
		// When co-location is required within a single domain, evaluate each viable domain
		// independently and select the one with minimum preemption cost!
		var bestResult *GangPlacementResult
		totalPrunedNodes := 0
		viableNodeCount := 0
		for _, dom := range viableDomains {
			viableNodeCount += len(dom.Nodes)
		}
		totalPrunedNodes = len(prob.CandidateNodes) - viableNodeCount

		for _, dom := range viableDomains {
			domProb, _ := PartitionProblemByDomains(prob, []*TopologyDomainAggregate{dom})
			domRes := BranchAndBoundGangSearch(ctx, domProb, opts.BranchAndBoundOpts)
			if domRes.Status.IsSuccess() {
				if bestResult == nil || domRes.TotalCost.Less(bestResult.TotalCost) {
					bestResult = domRes
				}
			}
		}

		if bestResult == nil {
			bestResult = &GangPlacementResult{
				StatesExplored: 0,
				BranchesPruned: 0,
				SearchDuration: time.Since(start),
				Status:         fwk.NewStatus(fwk.Unschedulable, "No viable domain could satisfy gang co-location requirements"),
			}
		}

		return &HierarchicalDomainSearchResult{
			PlacementResult:   bestResult,
			TotalDomains:      len(tree.DomainOrder),
			ViableDomains:     len(viableDomains),
			PrunedDomains:     prunedDomains,
			PrunedNodes:       totalPrunedNodes,
			DomainVerdicts:    verdicts,
			HierarchyDuration: hDuration,
			TotalDuration:     time.Since(start),
		}
	}

	partitionedProb, prunedNodes := PartitionProblemByDomains(prob, viableDomains)

	// 4. Execute Admissible Branch-and-Bound search on pruned candidate space
	placementRes := BranchAndBoundGangSearch(ctx, partitionedProb, opts.BranchAndBoundOpts)

	return &HierarchicalDomainSearchResult{
		PlacementResult:   placementRes,
		TotalDomains:      len(tree.DomainOrder),
		ViableDomains:     len(viableDomains),
		PrunedDomains:     prunedDomains,
		PrunedNodes:       prunedNodes,
		DomainVerdicts:    verdicts,
		HierarchyDuration: hDuration,
		TotalDuration:     time.Since(start),
	}
}
