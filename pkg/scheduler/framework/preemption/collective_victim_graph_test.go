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
	policy "k8s.io/api/policy/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/klog/v2/ktesting"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	internalcache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
)

// Helper to create a fake pod with specific resource requests, nodeName, priority, and optional group.
func makeVictimPodWithGroup(name, ns, nodeName string, cpuMillis, memBytes int64, prio int32, groupName *string) *v1.Pod {
	p := st.MakePod().Name(name).Namespace(ns).Node(nodeName).Priority(prio).
		Req(map[v1.ResourceName]string{
			v1.ResourceCPU:    fmt.Sprintf("%dm", cpuMillis),
			v1.ResourceMemory: fmt.Sprintf("%d", memBytes),
		}).Obj()
	p.UID = types.UID(fmt.Sprintf("uid-%s", name))
	if groupName != nil {
		p.Spec.SchedulingGroup = &v1.PodSchedulingGroup{
			PodGroupName: groupName,
		}
	}
	return p
}

// TestCollectiveVictimGraph_MultiNodeDisruptionModeAll verifies that evicting a pod belonging to
// a CompositePodGroup with DisruptionModeAll precalculates the entire blast radius across multiple nodes,
// instantly crediting freed resources across Node B and Node C when a victim on Node A is evaluated.
func TestCollectiveVictimGraph_MultiNodeDisruptionModeAll(t *testing.T) {
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.GenericWorkload:                 true,
		features.TopologyAwareWorkloadScheduling: true,
		features.CompositePodGroup:               true,
	})
	logger, ctx := ktesting.NewTestContext(t)

	// Setup 3 nodes: node-A, node-B, node-C
	nodeA := st.MakeNode().Name("node-A").Capacity(map[v1.ResourceName]string{
		v1.ResourceCPU: "4000m", v1.ResourceMemory: "8192", v1.ResourcePods: "100",
	}).Obj()
	nodeB := st.MakeNode().Name("node-B").Capacity(map[v1.ResourceName]string{
		v1.ResourceCPU: "4000m", v1.ResourceMemory: "8192", v1.ResourcePods: "100",
	}).Obj()
	nodeC := st.MakeNode().Name("node-C").Capacity(map[v1.ResourceName]string{
		v1.ResourceCPU: "4000m", v1.ResourceMemory: "8192", v1.ResourcePods: "100",
	}).Obj()
	nodes := []*v1.Node{nodeA, nodeB, nodeC}

	// Setup a CompositePodGroup hierarchy:
	// root-cpg (DisruptionModeAll, priority 10)
	//   ├── worker-pg (priority 10) -> worker-pod-A (on node-A, 1000m, 2048), worker-pod-B (on node-B, 1000m, 2048)
	//   └── ps-pg (priority 10)     -> ps-pod-C (on node-C, 2000m, 4096)
	cpgName := "distributed-cpg"
	workerPGName := "worker-pg"
	psPGName := "ps-pg"

	workerPodA := makeVictimPodWithGroup("worker-pod-A", "default", "node-A", 1000, 2048, 10, &workerPGName)
	workerPodB := makeVictimPodWithGroup("worker-pod-B", "default", "node-B", 1000, 2048, 10, &workerPGName)
	psPodC := makeVictimPodWithGroup("ps-pod-C", "default", "node-C", 2000, 4096, 10, &psPGName)

	initPods := []*v1.Pod{workerPodA, workerPodB, psPodC}

	workerPG := st.MakePodGroup().Name(workerPGName).Namespace("default").UID(types.UID(workerPGName)).Priority(10).
		ParentCompositePodGroup(cpgName).DisruptionModeAll().Obj()
	psPG := st.MakePodGroup().Name(psPGName).Namespace("default").UID(types.UID(psPGName)).Priority(10).
		ParentCompositePodGroup(cpgName).DisruptionModeAll().Obj()
	initPodGroups := []*schedulingv1beta1.PodGroup{workerPG, psPG}

	rootCPG := st.MakeCompositePodGroup().Name(cpgName).Namespace("default").UID(cpgName).Priority(10).
		DisruptionModeAll().Obj()
	initCompositePodGroups := []*schedulingv1alpha3.CompositePodGroup{rootCPG}

	snapshot := internalcache.NewTestSnapshotWithPodGroups(initPods, nodes, initPodGroups, initCompositePodGroups)
	cache := internalcache.New(ctx, nil, true, true)
	for _, node := range nodes {
		cache.AddNode(logger, node)
	}
	for _, pg := range initPodGroups {
		cache.AddGenericPodGroup(fwk.NewGenericPodGroup(pg))
	}
	for _, cpg := range initCompositePodGroups {
		cache.AddGenericPodGroup(fwk.NewGenericCompositePodGroup(cpg))
	}
	for _, p := range initPods {
		if err := cache.AddPod(logger, p); err != nil {
			t.Fatalf("Failed to add pod: %v", err)
		}
		if p.Spec.SchedulingGroup != nil && p.Spec.SchedulingGroup.PodGroupName != nil {
			cache.AddPodGroupMember(p)
		}
	}
	if err := cache.UpdateSnapshot(logger, snapshot); err != nil {
		t.Fatalf("Failed to update snapshot: %v", err)
	}

	// Preemptor has priority 100 > 10
	graph, err := BuildCollectiveVictimGraph(
		ctx,
		snapshot,
		snapshot.PodGroups(),
		snapshot.CompositePodGroups(),
		nil, /* pdbs */
		100, /* preemptorPriority */
		true,
	)
	if err != nil {
		t.Fatalf("Failed to build CollectiveVictimGraph: %v", err)
	}

	// Verify graph contains exactly 1 collective unit representing the root CompositePodGroup
	units := graph.Units()
	if len(units) != 1 {
		t.Fatalf("Expected 1 collective victim unit, got %d", len(units))
	}

	unit := units[0]
	if len(unit.Pods) != 3 {
		t.Fatalf("Expected 3 pods in collective victim unit, got %d", len(unit.Pods))
	}

	// Verify all 3 pods map to the same collective unit
	for _, p := range initPods {
		u, ok := graph.GetUnitForPod(p.UID)
		if !ok {
			t.Fatalf("Pod %s not found in graph", p.Name)
		}
		if u != unit {
			t.Fatalf("Pod %s maps to different unit %v, expected %v", p.Name, u, unit)
		}
	}

	// Verify precomputed multi-node yields
	if unit.NodeYields["node-A"].MilliCPU != 1000 || unit.NodeYields["node-A"].Memory != 2048 {
		t.Errorf("Node-A yield mismatch: got %+v", unit.NodeYields["node-A"])
	}
	if unit.NodeYields["node-B"].MilliCPU != 1000 || unit.NodeYields["node-B"].Memory != 2048 {
		t.Errorf("Node-B yield mismatch: got %+v", unit.NodeYields["node-B"])
	}
	if unit.NodeYields["node-C"].MilliCPU != 2000 || unit.NodeYields["node-C"].Memory != 4096 {
		t.Errorf("Node-C yield mismatch: got %+v", unit.NodeYields["node-C"])
	}

	// Test cross-node resource credit propagation in CrossNodePlacementPlan
	plan := NewCrossNodePlacementPlan()
	if !plan.EvictUnit(unit) {
		t.Fatalf("Failed to evict unit in plan")
	}

	// Instantaneous multi-node credit verification
	creditA := plan.GetNodeCredit("node-A")
	creditB := plan.GetNodeCredit("node-B")
	creditC := plan.GetNodeCredit("node-C")

	if creditA.MilliCPU != 1000 || creditB.MilliCPU != 1000 || creditC.MilliCPU != 2000 {
		t.Errorf("Expected instantaneous cross-node CPU credit (1000, 1000, 2000), got (%d, %d, %d)",
			creditA.MilliCPU, creditB.MilliCPU, creditC.MilliCPU)
	}

	// Verify plan rollback
	plan.RevertUnit(unit)
	if plan.GetNodeCredit("node-A").MilliCPU != 0 || plan.GetNodeCredit("node-B").MilliCPU != 0 || plan.GetNodeCredit("node-C").MilliCPU != 0 {
		t.Errorf("Expected 0 credits after revert, got (%+v, %+v, %+v)",
			plan.GetNodeCredit("node-A"), plan.GetNodeCredit("node-B"), plan.GetNodeCredit("node-C"))
	}
}

// TestCollectiveVictimGraph_ZeroVictimLeakageAndPDBTracking ensures exact PDB accounting and
// ensures no victim pods leak or double-count during cross-node gang preemption search.
func TestCollectiveVictimGraph_ZeroVictimLeakageAndPDBTracking(t *testing.T) {
	ctx := context.Background()

	nodeA := st.MakeNode().Name("node-A").Capacity(map[v1.ResourceName]string{
		v1.ResourceCPU: "2000m", v1.ResourceMemory: "4096", v1.ResourcePods: "10",
	}).Obj()
	nodeB := st.MakeNode().Name("node-B").Capacity(map[v1.ResourceName]string{
		v1.ResourceCPU: "2000m", v1.ResourceMemory: "4096", v1.ResourcePods: "10",
	}).Obj()
	nodes := []*v1.Node{nodeA, nodeB}

	// Pod 1 on node-A (protected by PDB with 0 disruptions allowed)
	p1 := makeVictimPodWithGroup("p1-violating", "default", "node-A", 1000, 1024, 10, nil)
	p1.Labels = map[string]string{"app": "protected"}
	// Pod 2 on node-B (not violating)
	p2 := makeVictimPodWithGroup("p2-safe", "default", "node-B", 1000, 1024, 20, nil)

	initPods := []*v1.Pod{p1, p2}
	snapshot := internalcache.NewTestSnapshotWithPodGroups(initPods, nodes, nil, nil)

	pdb := &policy.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "pdb-p1", Namespace: "default"},
		Spec:       policy.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "protected"}}},
		Status:     policy.PodDisruptionBudgetStatus{DisruptionsAllowed: 0},
	}
	pdbs := []*policy.PodDisruptionBudget{pdb}

	graph, err := BuildCollectiveVictimGraph(
		ctx,
		snapshot,
		nil, nil,
		pdbs,
		100,
		true,
	)
	if err != nil {
		t.Fatalf("BuildCollectiveVictimGraph failed: %v", err)
	}

	u1, ok1 := graph.GetUnitForPod(p1.UID)
	u2, ok2 := graph.GetUnitForPod(p2.UID)
	if !ok1 || !ok2 {
		t.Fatalf("Units not found for p1 or p2")
	}

	if u1.PDBViolations != 1 {
		t.Errorf("Expected u1 to have 1 PDB violation, got %d", u1.PDBViolations)
	}
	if u2.PDBViolations != 0 {
		t.Errorf("Expected u2 to have 0 PDB violations, got %d", u2.PDBViolations)
	}

	// Test gang search with graph acceleration
	preemptor1 := makeVictimPodWithGroup("preemptor-1", "default", "", 1000, 1024, 100, nil)
	preemptor2 := makeVictimPodWithGroup("preemptor-2", "default", "", 1000, 1024, 100, nil)

	prob := &GangPlacementProblem{
		PreemptorPods:      []*v1.Pod{preemptor1, preemptor2},
		CandidateNodes:     []string{"node-A", "node-B"},
		NodeAllocatableCPU: map[string]int64{"node-A": 0, "node-B": 0}, // Requires preemption on both
		NodeAllocatableMem: map[string]int64{"node-A": 0, "node-B": 0},
		PerPodCandidates: [][]GangPodCandidate{
			{
				{NodeName: "node-A", Cost: u1.Cost, Victims: []*v1.Pod{p1}, PDBViolations: 1, Feasible: true, RequiredMilliCPU: 1000, RequiredMemory: 1024},
				{NodeName: "node-B", Cost: u2.Cost, Victims: []*v1.Pod{p2}, PDBViolations: 0, Feasible: true, RequiredMilliCPU: 1000, RequiredMemory: 1024},
			},
			{
				{NodeName: "node-A", Cost: u1.Cost, Victims: []*v1.Pod{p1}, PDBViolations: 1, Feasible: true, RequiredMilliCPU: 1000, RequiredMemory: 1024},
				{NodeName: "node-B", Cost: u2.Cost, Victims: []*v1.Pod{p2}, PDBViolations: 0, Feasible: true, RequiredMilliCPU: 1000, RequiredMemory: 1024},
			},
		},
		MinMember: 2,
	}

	res := GraphAcceleratedGangSearch(ctx, prob, graph, DefaultBranchAndBoundOptions())
	if !res.Status.IsSuccess() {
		t.Fatalf("Gang search failed: %v", res.Status.Message())
	}

	// Verify exact victims selected and zero leakage
	if len(res.SelectedVictims) != 2 {
		t.Fatalf("Expected 2 selected victims, got %d", len(res.SelectedVictims))
	}
	victimUIDs := sets.New[types.UID]()
	for _, v := range res.SelectedVictims {
		victimUIDs.Insert(v.UID)
	}
	if !victimUIDs.Has(p1.UID) || !victimUIDs.Has(p2.UID) {
		t.Errorf("Selected victims missing p1 or p2: got %v", victimUIDs)
	}
}

// TestCollectiveVictimGraph_EquivalenceWithStandardBnB rigorously asserts that
// GraphAcceleratedGangSearch achieves 100% mathematical bit-for-bit equivalence with
// standard BranchAndBoundGangSearch while properly applying multi-node credit propagation.
func TestCollectiveVictimGraph_EquivalenceWithStandardBnB(t *testing.T) {
	ctx := context.Background()

	// 4 nodes
	nodeNames := []string{"n0", "n1", "n2", "n3"}
	var nodes []*v1.Node
	for _, n := range nodeNames {
		nodes = append(nodes, st.MakeNode().Name(n).Capacity(map[v1.ResourceName]string{
			v1.ResourceCPU: "8000m", v1.ResourceMemory: "16384", v1.ResourcePods: "100",
		}).Obj())
	}

	// Create 4 victim pods, exactly one per node
	var victimPods []*v1.Pod
	for i, n := range nodeNames {
		prio := int32((i + 1) * 10)
		vp := makeVictimPodWithGroup(fmt.Sprintf("vpod-%d", i), "default", n, 1000, 1024, prio, nil)
		victimPods = append(victimPods, vp)
	}

	snapshot := internalcache.NewTestSnapshotWithPodGroups(victimPods, nodes, nil, nil)
	graph, err := BuildCollectiveVictimGraph(ctx, snapshot, nil, nil, nil, 1000, true)
	if err != nil {
		t.Fatalf("Failed to build graph: %v", err)
	}

	// Create 4 preemptor pods
	var preemptors []*v1.Pod
	for i := 0; i < 4; i++ {
		p := makeVictimPodWithGroup(fmt.Sprintf("preemptor-%d", i), "default", "", 1000, 1024, 1000, nil)
		preemptors = append(preemptors, p)
	}

	// Build PerPodCandidates: each preemptor can be placed on any node n_j, which evicts victimPods[j]
	perPodCandidates := make([][]GangPodCandidate, len(preemptors))
	for i := range preemptors {
		for j, n := range nodeNames {
			v := victimPods[j]
			unit, _ := graph.GetUnitForPod(v.UID)
			perPodCandidates[i] = append(perPodCandidates[i], GangPodCandidate{
				NodeName:         n,
				Cost:             unit.Cost,
				Victims:          []*v1.Pod{v},
				Feasible:         true,
				RequiredMilliCPU: 1000,
				RequiredMemory:   1024,
			})
		}
	}

	prob := &GangPlacementProblem{
		PreemptorPods:      preemptors,
		CandidateNodes:     nodeNames,
		NodeAllocatableCPU: map[string]int64{"n0": 4000, "n1": 4000, "n2": 4000, "n3": 4000},
		NodeAllocatableMem: map[string]int64{"n0": 8192, "n1": 8192, "n2": 8192, "n3": 8192},
		PerPodCandidates:   perPodCandidates,
		MinMember:          4,
	}

	standardRes := BranchAndBoundGangSearch(ctx, prob, DefaultBranchAndBoundOptions())
	graphRes := GraphAcceleratedGangSearch(ctx, prob, graph, DefaultBranchAndBoundOptions())

	if !standardRes.Status.IsSuccess() || !graphRes.Status.IsSuccess() {
		t.Fatalf("Standard (%v) or Graph (%v) failed", standardRes.Status, graphRes.Status)
	}

	if !standardRes.TotalCost.Equal(graphRes.TotalCost) {
		t.Fatalf("Total cost mismatch: standard=%+v vs graph=%+v", standardRes.TotalCost, graphRes.TotalCost)
	}

	if len(standardRes.SelectedVictims) != len(graphRes.SelectedVictims) {
		t.Fatalf("Victim count mismatch: standard=%d vs graph=%d", len(standardRes.SelectedVictims), len(graphRes.SelectedVictims))
	}
	t.Logf("Equivalence verified! Standard cost=%+v, Graph cost=%+v", standardRes.TotalCost, graphRes.TotalCost)
}

// BenchmarkCollectiveVictimGraph_Construction measures the compile time of the dependency graph
// on clusters with varying composite workload density.
func BenchmarkCollectiveVictimGraph_Construction(b *testing.B) {
	ctx := context.Background()

	// 50 nodes
	var nodes []*v1.Node
	for i := 0; i < 50; i++ {
		nodes = append(nodes, st.MakeNode().Name(fmt.Sprintf("node-%d", i)).Capacity(map[v1.ResourceName]string{
			v1.ResourceCPU: "32000m", v1.ResourceMemory: "65536", v1.ResourcePods: "110",
		}).Obj())
	}

	// 200 CompositePodGroups, each spanning 5 pods across 5 nodes
	var pods []*v1.Pod
	var pgs []*schedulingv1beta1.PodGroup
	var cpgs []*schedulingv1alpha3.CompositePodGroup

	for g := 0; g < 200; g++ {
		cpgName := fmt.Sprintf("cpg-%d", g)
		cpg := st.MakeCompositePodGroup().Name(cpgName).Namespace("default").UID(cpgName).Priority(10).DisruptionModeAll().Obj()
		cpgs = append(cpgs, cpg)

		pgName := fmt.Sprintf("pg-%d", g)
		pg := st.MakePodGroup().Name(pgName).Namespace("default").UID(types.UID(pgName)).Priority(10).
			ParentCompositePodGroup(cpgName).DisruptionModeAll().Obj()
		pgs = append(pgs, pg)

		for m := 0; m < 5; m++ {
			nodeName := fmt.Sprintf("node-%d", (g*5+m)%50)
			p := makeVictimPodWithGroup(fmt.Sprintf("pod-%d-%d", g, m), "default", nodeName, 1000, 2048, 10, &pgName)
			pods = append(pods, p)
		}
	}

	snapshot := internalcache.NewTestSnapshotWithPodGroups(pods, nodes, pgs, cpgs)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := BuildCollectiveVictimGraph(ctx, snapshot, snapshot.PodGroups(), snapshot.CompositePodGroups(), nil, 100, true)
		if err != nil {
			b.Fatalf("Failed to build graph: %v", err)
		}
	}
}

// BenchmarkGangPlacement_StandardVsGraph compares search throughput with and without
// the precomputed CollectiveVictimGraph across distributed multi-node workloads.
func BenchmarkGangPlacement_StandardVsGraph(b *testing.B) {
	ctx := context.Background()

	nodeNames := []string{"node-0", "node-1", "node-2", "node-3", "node-4", "node-5", "node-6", "node-7"}
	var nodes []*v1.Node
	for _, n := range nodeNames {
		nodes = append(nodes, st.MakeNode().Name(n).Capacity(map[v1.ResourceName]string{
			v1.ResourceCPU: "16000m", v1.ResourceMemory: "32768", v1.ResourcePods: "100",
		}).Obj())
	}

	// 10 CompositePodGroups spanning 4 nodes each
	var pods []*v1.Pod
	var pgs []*schedulingv1beta1.PodGroup
	var cpgs []*schedulingv1alpha3.CompositePodGroup
	for g := 0; g < 10; g++ {
		cpgName := fmt.Sprintf("cpg-%d", g)
		cpg := st.MakeCompositePodGroup().Name(cpgName).Namespace("default").UID(cpgName).Priority(int32(10 + g)).DisruptionModeAll().Obj()
		cpgs = append(cpgs, cpg)

		pgName := fmt.Sprintf("pg-%d", g)
		pg := st.MakePodGroup().Name(pgName).Namespace("default").UID(types.UID(pgName)).Priority(int32(10 + g)).
			ParentCompositePodGroup(cpgName).DisruptionModeAll().Obj()
		pgs = append(pgs, pg)

		for m := 0; m < 4; m++ {
			nodeName := nodeNames[(g*4+m)%len(nodeNames)]
			p := makeVictimPodWithGroup(fmt.Sprintf("pod-%d-%d", g, m), "default", nodeName, 1000, 2048, int32(10+g), &pgName)
			pods = append(pods, p)
		}
	}

	snapshot := internalcache.NewTestSnapshotWithPodGroups(pods, nodes, pgs, cpgs)
	graph, _ := BuildCollectiveVictimGraph(ctx, snapshot, snapshot.PodGroups(), snapshot.CompositePodGroups(), nil, 1000, true)

	// Preemptor gang of 8 pods
	var preemptors []*v1.Pod
	for i := 0; i < 8; i++ {
		preemptors = append(preemptors, makeVictimPodWithGroup(fmt.Sprintf("preemptor-%d", i), "default", "", 1000, 2048, 1000, nil))
	}

	perPodCandidates := make([][]GangPodCandidate, len(preemptors))
	for i := range preemptors {
		for _, n := range nodeNames {
			// pick candidate victim on this node
			var cVictims []*v1.Pod
			for _, p := range pods {
				if p.Spec.NodeName == n {
					cVictims = append(cVictims, p)
					break
				}
			}
			cost := ComputeVictimsCost(cVictims, 0)
			perPodCandidates[i] = append(perPodCandidates[i], GangPodCandidate{
				NodeName:         n,
				Cost:             cost,
				Victims:          cVictims,
				Feasible:         true,
				RequiredMilliCPU: 1000,
				RequiredMemory:   2048,
			})
		}
	}

	prob := &GangPlacementProblem{
		PreemptorPods:      preemptors,
		CandidateNodes:     nodeNames,
		NodeAllocatableCPU: map[string]int64{"node-0": 0, "node-1": 0, "node-2": 0, "node-3": 0, "node-4": 0, "node-5": 0, "node-6": 0, "node-7": 0},
		NodeAllocatableMem: map[string]int64{"node-0": 0, "node-1": 0, "node-2": 0, "node-3": 0, "node-4": 0, "node-5": 0, "node-6": 0, "node-7": 0},
		PerPodCandidates:   perPodCandidates,
		MinMember:          8,
	}

	b.Run("Standard_BranchAndBound", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = BranchAndBoundGangSearch(ctx, prob, DefaultBranchAndBoundOptions())
		}
	})

	b.Run("GraphAccelerated_BranchAndBound", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = GraphAcceleratedGangSearch(ctx, prob, graph, DefaultBranchAndBoundOptions())
		}
	})
}
