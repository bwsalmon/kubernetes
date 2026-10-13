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
	"sort"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/component-helpers/resource"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
)

// ResourceYield represents the aggregated compute and memory resources released on a single node.
type ResourceYield struct {
	MilliCPU int64
	Memory   int64
	PodCount int
}

// CollectiveVictimUnit represents an atomic cluster-wide victim group (single pod or entire CompositePodGroup tree)
// with precomputed multi-node resource yields and preemption disruption penalty.
type CollectiveVictimUnit struct {
	// ID uniquely identifies this collective unit (e.g., Pod UID or Root CompositePodGroup EntityKey).
	ID string
	// KeyType specifies if this unit is PodKeyType, PodGroupKeyType, or CompositePodGroupKeyType.
	KeyType fwk.EntityKeyType
	// Pods is the complete list of member PodInfos that would be evicted if this unit is disrupted.
	Pods []fwk.PodInfo
	// Priority is the resolved group priority.
	Priority int32
	// PDBViolations counts how many PDBs are violated by evicting this collective unit.
	PDBViolations int
	// PreemptionCost is the precalculated PreemptionCost under KEP-5710 / OrderedScoreFuncs metrics.
	Cost PreemptionCost
	// NodeYields maps nodeName -> ResourceYield released on that node upon eviction of this unit.
	NodeYields map[string]ResourceYield
	// AffectedNodes is the set of all node names hosting members of this collective unit.
	AffectedNodes sets.Set[string]
}

// CollectiveVictimGraph is an in-memory dependency graph compiled prior to preemption simulation.
// It maps member pods (by UID) to their enclosing CollectiveVictimUnit, precomputing the multi-node
// blast radius, cross-node resource yields, and PDB disruption penalties.
type CollectiveVictimGraph struct {
	mu sync.RWMutex
	// units stores all unique collective units keyed by unit ID.
	units map[string]*CollectiveVictimUnit
	// podToUnit maps individual Pod UID to its enclosing CollectiveVictimUnit.
	podToUnit map[types.UID]*CollectiveVictimUnit
	// nodeToUnits maps nodeName -> set of CollectiveVictimUnits that have members on that node.
	nodeToUnits map[string][]*CollectiveVictimUnit
}

// NewCollectiveVictimGraph instantiates an empty CollectiveVictimGraph.
func NewCollectiveVictimGraph() *CollectiveVictimGraph {
	return &CollectiveVictimGraph{
		units:       make(map[string]*CollectiveVictimUnit),
		podToUnit:   make(map[types.UID]*CollectiveVictimUnit),
		nodeToUnits: make(map[string][]*CollectiveVictimUnit),
	}
}

// BuildCollectiveVictimGraph compiles the collective victim dependency graph from the cluster snapshot,
// resolving CompositePodGroup and PodGroup disruption hierarchies (DisruptionModeAll vs Single) and PDB budgets.
func BuildCollectiveVictimGraph(
	ctx context.Context,
	snapshot fwk.SharedLister,
	podGroupSnapshot fwk.PodGroupLister,
	compositePodGroupSnapshot fwk.CompositePodGroupLister,
	pdbs []*policy.PodDisruptionBudget,
	preemptorPriority int32,
	enablePodLevelResources bool,
) (*CollectiveVictimGraph, error) {
	logger := klog.FromContext(ctx)
	graph := NewCollectiveVictimGraph()

	// 1. Gather all candidate victims across nodes
	rawVictims, err := getWorkloadPreemptionVictims(logger, snapshot, podGroupSnapshot, compositePodGroupSnapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to discover workload preemption victims: %w", err)
	}

	// 2. Filter victims by preemptor priority and evaluate PDB violations
	var eligibleVictims []Victim
	for _, v := range rawVictims {
		if v.Priority() < preemptorPriority {
			eligibleVictims = append(eligibleVictims, v)
		}
	}

	violatingVictims, nonViolatingVictims := FilterVictimsWithPDBViolation(eligibleVictims, pdbs)
	pdbViolationMap := make(map[Victim]int)
	for _, vv := range violatingVictims {
		pdbViolationMap[vv.Victim] = vv.ViolateCount
	}
	for _, nv := range nonViolatingVictims {
		pdbViolationMap[nv] = 0
	}

	// 3. Populate collective victim units
	for _, v := range eligibleVictims {
		podInfos := v.Pods()
		if len(podInfos) == 0 {
			continue
		}

		pdbViolations := pdbViolationMap[v]
		nodeYields := make(map[string]ResourceYield)
		affectedNodes := sets.New[string]()
		vPods := make([]*v1.Pod, len(podInfos))

		for i, pi := range podInfos {
			pod := pi.GetPod()
			vPods[i] = pod
			nodeName := pod.Spec.NodeName
			affectedNodes.Insert(nodeName)

			req := resource.PodRequests(pod, resource.PodResourcesOptions{
				SkipPodLevelResources: !enablePodLevelResources,
			})
			milliCPU := req.Cpu().MilliValue()
			mem := req.Memory().Value()

			yield := nodeYields[nodeName]
			yield.MilliCPU += milliCPU
			yield.Memory += mem
			yield.PodCount++
			nodeYields[nodeName] = yield
		}

		cost := ComputeVictimsCost(vPods, pdbViolations)

		// Formulate unit ID
		unitID := string(podInfos[0].GetPod().UID)
		if v.Type() == fwk.CompositePodGroupKeyType || v.Type() == fwk.PodGroupKeyType {
			// For group victims, use the highest ancestor key or group identifier
			if podInfos[0].GetPod().Spec.SchedulingGroup != nil {
				unitID = fmt.Sprintf("%s/%s", v.Type(), *podInfos[0].GetPod().Spec.SchedulingGroup.PodGroupName)
			}
		}

		unit := &CollectiveVictimUnit{
			ID:            unitID,
			KeyType:       v.Type(),
			Pods:          podInfos,
			Priority:      v.Priority(),
			PDBViolations: pdbViolations,
			Cost:          cost,
			NodeYields:    nodeYields,
			AffectedNodes: affectedNodes,
		}

		graph.units[unit.ID] = unit
		for _, pi := range podInfos {
			graph.podToUnit[pi.GetPod().UID] = unit
		}
		for nodeName := range affectedNodes {
			graph.nodeToUnits[nodeName] = append(graph.nodeToUnits[nodeName], unit)
		}
	}

	return graph, nil
}

// GetUnitForPod returns the collective victim unit containing the specified pod UID.
func (g *CollectiveVictimGraph) GetUnitForPod(podUID types.UID) (*CollectiveVictimUnit, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	u, ok := g.podToUnit[podUID]
	return u, ok
}

// GetUnitsOnNode returns all collective victim units that have member pods on the given node.
func (g *CollectiveVictimGraph) GetUnitsOnNode(nodeName string) []*CollectiveVictimUnit {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.nodeToUnits[nodeName]
}

// Units returns all collective victim units in the graph.
func (g *CollectiveVictimGraph) Units() []*CollectiveVictimUnit {
	g.mu.RLock()
	defer g.mu.RUnlock()
	res := make([]*CollectiveVictimUnit, 0, len(g.units))
	for _, u := range g.units {
		res = append(res, u)
	}
	return res
}

// CrossNodePlacementPlan tracks cumulative resource credits and active collective disruptions across nodes.
type CrossNodePlacementPlan struct {
	// activeUnits records the set of collective units that have been marked for eviction.
	activeUnits map[string]*CollectiveVictimUnit
	// cumulativeNodeCredits tracks total resource yields credited on each node from evicted units.
	cumulativeNodeCredits map[string]ResourceYield
	// totalCost is the combined preemption cost across all activated collective victim units.
	totalCost PreemptionCost
}

// NewCrossNodePlacementPlan creates an empty placement plan.
func NewCrossNodePlacementPlan() *CrossNodePlacementPlan {
	return &CrossNodePlacementPlan{
		activeUnits:           make(map[string]*CollectiveVictimUnit),
		cumulativeNodeCredits: make(map[string]ResourceYield),
		totalCost:             ZeroPreemptionCost(),
	}
}

// Clone creates a shallow-independent copy of the placement plan for branch exploration.
func (p *CrossNodePlacementPlan) Clone() *CrossNodePlacementPlan {
	cp := &CrossNodePlacementPlan{
		activeUnits:           make(map[string]*CollectiveVictimUnit, len(p.activeUnits)),
		cumulativeNodeCredits: make(map[string]ResourceYield, len(p.cumulativeNodeCredits)),
		totalCost:             p.totalCost,
	}
	for k, v := range p.activeUnits {
		cp.activeUnits[k] = v
	}
	for k, v := range p.cumulativeNodeCredits {
		cp.cumulativeNodeCredits[k] = v
	}
	return cp
}

// EvictUnit activates eviction of the given collective unit, crediting freed resources across all affected nodes.
// Returns false if the unit was already evicted.
func (p *CrossNodePlacementPlan) EvictUnit(unit *CollectiveVictimUnit) bool {
	if _, exists := p.activeUnits[unit.ID]; exists {
		return false
	}
	p.activeUnits[unit.ID] = unit
	for node, yield := range unit.NodeYields {
		credit := p.cumulativeNodeCredits[node]
		credit.MilliCPU += yield.MilliCPU
		credit.Memory += yield.Memory
		credit.PodCount += yield.PodCount
		p.cumulativeNodeCredits[node] = credit
	}
	p.totalCost = p.totalCost.Add(unit.Cost)
	return true
}

// RevertUnit rolls back eviction of the given collective unit, debiting freed resources across all affected nodes.
func (p *CrossNodePlacementPlan) RevertUnit(unit *CollectiveVictimUnit) {
	if _, exists := p.activeUnits[unit.ID]; !exists {
		return
	}
	delete(p.activeUnits, unit.ID)
	for node, yield := range unit.NodeYields {
		credit := p.cumulativeNodeCredits[node]
		credit.MilliCPU -= yield.MilliCPU
		credit.Memory -= yield.Memory
		credit.PodCount -= yield.PodCount
		p.cumulativeNodeCredits[node] = credit
	}
	// Recompute total cost
	p.totalCost = ZeroPreemptionCost()
	for _, u := range p.activeUnits {
		p.totalCost = p.totalCost.Add(u.Cost)
	}
}

// GetNodeCredit returns the cumulative resource credit currently available on the specified node.
func (p *CrossNodePlacementPlan) GetNodeCredit(node string) ResourceYield {
	return p.cumulativeNodeCredits[node]
}

// TotalCost returns the current combined preemption cost of the placement plan.
func (p *CrossNodePlacementPlan) TotalCost() PreemptionCost {
	return p.totalCost
}

// EvictedVictimPods returns the deduplicated list of all victim pods across all evicted units in the plan.
func (p *CrossNodePlacementPlan) EvictedVictimPods() []*v1.Pod {
	seen := sets.New[types.UID]()
	var pods []*v1.Pod
	for _, u := range p.activeUnits {
		for _, pi := range u.Pods {
			pod := pi.GetPod()
			if !seen.Has(pod.UID) {
				seen.Insert(pod.UID)
				pods = append(pods, pod)
			}
		}
	}
	return pods
}

// GraphAcceleratedGangSearch executes branch-and-bound gang placement search using the precomputed CollectiveVictimGraph.
// When an assignment requires evicting a collective unit on Node A, all other nodes (e.g. Node B and Node C) immediately
// observe and receive the credited resource headroom without dynamic reconciliation loops.
func GraphAcceleratedGangSearch(
	ctx context.Context,
	prob *GangPlacementProblem,
	graph *CollectiveVictimGraph,
	opts BranchAndBoundOptions,
) *GangPlacementResult {
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
	var bestPlan *CrossNodePlacementPlan
	statesExplored := 0
	branchesPruned := 0
	earlyExit := false

	// Candidate ordering: sort candidate options by ascending individual cost
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
	plan := NewCrossNodePlacementPlan()

	// Track per-node consumption by placed preemptor pods
	preemptorUsedCPU := make(map[string]int64)
	preemptorUsedMem := make(map[string]int64)
	preemptorPodCount := make(map[string]int)

	topoCounts := make([]map[string]int, len(prob.TopologyConstraints))
	for tIdx := range prob.TopologyConstraints {
		topoCounts[tIdx] = make(map[string]int)
	}

	var search func(podIdx int)
	search = func(podIdx int) {
		if earlyExit {
			return
		}

		if podIdx == numPods {
			statesExplored++
			cost := plan.TotalCost()
			if cost.Less(bestCost) {
				bestCost = cost
				bestAssignments = make([]int, numPods)
				copy(bestAssignments, current)
				bestPlan = plan.Clone()
				if opts.EnableEarlyExit && bestCost.Equal(ZeroPreemptionCost()) {
					earlyExit = true
				}
			}
			return
		}

		for _, candIdx := range orderedCandidates[podIdx] {
			if earlyExit {
				return
			}
			cand := prob.PerPodCandidates[podIdx][candIdx]
			nodeName := cand.NodeName

			// Determine which collective units must be evicted for this candidate placement
			var unitsToEvict []*CollectiveVictimUnit
			for _, v := range cand.Victims {
				if u, ok := graph.GetUnitForPod(v.UID); ok {
					if _, already := plan.activeUnits[u.ID]; !already {
						unitsToEvict = append(unitsToEvict, u)
					}
				}
			}

			// Speculatively evict required units and credit resources across all affected nodes
			for _, u := range unitsToEvict {
				plan.EvictUnit(u)
			}

			// Check Branch-and-Bound Cost Pruning: if current plan cost is already >= bestCost, prune immediately
			if !bestCost.Equal(MaxPreemptionCost()) && !plan.TotalCost().Less(bestCost) {
				branchesPruned++
				for i := len(unitsToEvict) - 1; i >= 0; i-- {
					plan.RevertUnit(unitsToEvict[i])
				}
				continue
			}

			// Check effective node capacity with collective resource yields credited
			credit := plan.GetNodeCredit(nodeName)
			effectiveAllocatableCPU := prob.NodeAllocatableCPU[nodeName] + credit.MilliCPU
			effectiveAllocatableMem := prob.NodeAllocatableMem[nodeName] + credit.Memory

			if prob.NodeAllocatableCPU != nil && preemptorUsedCPU[nodeName]+cand.RequiredMilliCPU > effectiveAllocatableCPU {
				for i := len(unitsToEvict) - 1; i >= 0; i-- {
					plan.RevertUnit(unitsToEvict[i])
				}
				continue
			}
			if prob.NodeAllocatableMem != nil && preemptorUsedMem[nodeName]+cand.RequiredMemory > effectiveAllocatableMem {
				for i := len(unitsToEvict) - 1; i >= 0; i-- {
					plan.RevertUnit(unitsToEvict[i])
				}
				continue
			}
			if prob.NodeMaxPods != nil && preemptorPodCount[nodeName]+1 > (prob.NodeMaxPods[nodeName]+credit.PodCount) {
				for i := len(unitsToEvict) - 1; i >= 0; i-- {
					plan.RevertUnit(unitsToEvict[i])
				}
				continue
			}

			// Check Topology Spread
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
				for i := len(unitsToEvict) - 1; i >= 0; i-- {
					plan.RevertUnit(unitsToEvict[i])
				}
				continue
			}

			// Apply state
			current[podIdx] = candIdx
			preemptorUsedCPU[nodeName] += cand.RequiredMilliCPU
			preemptorUsedMem[nodeName] += cand.RequiredMemory
			preemptorPodCount[nodeName]++
			for tIdx, tc := range prob.TopologyConstraints {
				domain := tc.NodeDomains[nodeName]
				if domain != "" {
					topoCounts[tIdx][domain]++
				}
			}

			// Recurse
			search(podIdx + 1)

			// Rollback state
			preemptorUsedCPU[nodeName] -= cand.RequiredMilliCPU
			preemptorUsedMem[nodeName] -= cand.RequiredMemory
			preemptorPodCount[nodeName]--
			for tIdx, tc := range prob.TopologyConstraints {
				domain := tc.NodeDomains[nodeName]
				if domain != "" {
					topoCounts[tIdx][domain]--
				}
			}
			for i := len(unitsToEvict) - 1; i >= 0; i-- {
				plan.RevertUnit(unitsToEvict[i])
			}
		}
	}

	search(0)

	if bestCost.Equal(MaxPreemptionCost()) || bestPlan == nil {
		return &GangPlacementResult{
			StatesExplored: statesExplored,
			BranchesPruned: branchesPruned,
			SearchDuration: time.Since(startTime),
			Status:         fwk.NewStatus(fwk.Unschedulable, "No feasible gang placement found"),
		}
	}

	podAssignments := make(map[types.UID]string)
	nodeAssignments := make(map[string][]*v1.Pod)
	for podIdx, candIdx := range bestAssignments {
		pod := prob.PreemptorPods[podIdx]
		cand := prob.PerPodCandidates[podIdx][candIdx]
		podAssignments[pod.UID] = cand.NodeName
		nodeAssignments[cand.NodeName] = append(nodeAssignments[cand.NodeName], pod)
	}

	evictedPods := bestPlan.EvictedVictimPods()
	victimUIDs := sets.New[types.UID]()
	for _, p := range evictedPods {
		victimUIDs.Insert(p.UID)
	}

	return &GangPlacementResult{
		PodAssignments:     podAssignments,
		NodeAssignments:    nodeAssignments,
		TotalCost:          bestCost,
		SelectedVictims:    evictedPods,
		SelectedVictimUIDs: victimUIDs,
		StatesExplored:     statesExplored,
		BranchesPruned:     branchesPruned,
		EarlyExitTriggered: earlyExit,
		SearchDuration:     time.Since(startTime),
		Status:             fwk.NewStatus(fwk.Success),
	}
}
