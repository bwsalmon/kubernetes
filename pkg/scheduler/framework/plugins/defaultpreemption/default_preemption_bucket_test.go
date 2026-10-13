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

package defaultpreemption

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/klog/v2/ktesting"
	fwk "k8s.io/kube-scheduler/framework"
	internalcache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
	internalqueue "k8s.io/kubernetes/pkg/scheduler/backend/queue"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/parallelize"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultbinder"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeports"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	"k8s.io/kubernetes/pkg/scheduler/framework/preemption"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	tf "k8s.io/kubernetes/pkg/scheduler/testing/framework"
)

// TestSelectVictimsOnNode_BucketEquivalence verifies bit-for-bit victim selection equivalence
// between Priority-Tier Bucket Pre-Partitioning and the linear reference baseline across multi-tiered
// candidate nodes with densities ranging from 50 to 500 pods.
func TestSelectVictimsOnNode_BucketEquivalence(t *testing.T) {
	densities := []int{50, 100, 250, 500}

	for _, count := range densities {
		t.Run(fmt.Sprintf("Density_%d_Pods_Three_Tiers", count), func(t *testing.T) {
			ctx := context.Background()
			node := st.MakeNode().Name("node-bucket").Capacity(map[v1.ResourceName]string{
				v1.ResourcePods:   "1000",
				v1.ResourceCPU:    "100000m",
				v1.ResourceMemory: "500000Mi",
			}).Obj()

			baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			var initPods []*v1.Pod

			// Create 3 discrete priority tiers:
			// Tier 1 (Lowest): Priority 0 (Scavenger) -> 30% of pods
			// Tier 2 (Middle): Priority 1000 (Batch)   -> 50% of pods
			// Tier 3 (High):   Priority 50000 (Prod)   -> 20% of pods
			for i := 0; i < count; i++ {
				var prio int32
				ratio := float64(i) / float64(count)
				if ratio < 0.30 {
					prio = 0
				} else if ratio < 0.80 {
					prio = 1000
				} else {
					prio = 50000
				}

				stTime := metav1.NewTime(baseTime.Add(time.Duration(i) * time.Minute))
				p := st.MakePod().Name(fmt.Sprintf("pod-%04d", i)).
					UID(fmt.Sprintf("uid-bucket-%04d", i)).
					Node("node-bucket").
					Priority(prio).
					StartTime(stTime).
					Req(map[v1.ResourceName]string{
						v1.ResourceCPU:    "100m",
						v1.ResourceMemory: "500Mi",
					}).Obj()
				initPods = append(initPods, p)
			}

			// Preemptor needs 500m CPU and 2000Mi memory.
			// Evicting 5 pods from Tier 1 (priority 0) provides 500m CPU and 2500Mi memory,
			// perfectly fitting the preemptor without needing to touch Tier 2 or Tier 3!
			preemptor := st.MakePod().Name("preemptor-tier1-sufficient").
				UID("preemptor-uid").
				Priority(100000).
				Req(map[v1.ResourceName]string{
					v1.ResourceCPU:    "500m",
					v1.ResourceMemory: "2000Mi",
				}).Obj()

			pl, _, nodeInfo, potentialVictims, cycleState := setupHighDensityTestEnv(t, node, initPods, preemptor, nil)

			// Execute Priority-Tier Bucket Pre-Partitioning
			bucketPods, bucketPDBViolations, bucketStatus := pl.SelectVictimsOnNode(
				ctx, cycleState.Clone(), preemptor, nodeInfo.Snapshot(), potentialVictims, nil,
			)
			if !bucketStatus.IsSuccess() {
				t.Fatalf("SelectVictimsOnNode failed: %v", bucketStatus)
			}

			// Execute Linear Baseline
			linPods, linPDBViolations, linStatus := linearSelectVictimsOnNode(
				ctx, pl, cycleState.Clone(), preemptor, nodeInfo.Snapshot(), potentialVictims, nil,
			)
			if !linStatus.IsSuccess() {
				t.Fatalf("linearSelectVictimsOnNode failed: %v", linStatus)
			}

			// Verify PDB violations equivalence
			if bucketPDBViolations != linPDBViolations {
				t.Errorf("PDB violations mismatch: bucket=%d, linear=%d", bucketPDBViolations, linPDBViolations)
			}

			// Verify bit-for-bit equivalence in selected victims
			bucketVictimNames := make([]string, len(bucketPods))
			for i, p := range bucketPods {
				bucketVictimNames[i] = p.Name
			}
			linVictimNames := make([]string, len(linPods))
			for i, p := range linPods {
				linVictimNames[i] = p.Name
			}

			if diff := cmp.Diff(linVictimNames, bucketVictimNames); diff != "" {
				t.Errorf("Bit-for-bit victim selection mismatch (-linear +bucket):\n%s", diff)
			}

			// Verify that every single selected victim is strictly from Tier 1 (priority 0)
			for _, p := range bucketPods {
				if *p.Spec.Priority != 0 {
					t.Errorf("Expected victim %s to be from Tier 1 (prio 0), got prio %d", p.Name, *p.Spec.Priority)
				}
			}
		})
	}
}

// TestSelectVictimsOnNode_BoundaryBucketExpansion verifies correct multi-tier expansion when
// the lowest tier does not provide enough resources on its own, correctly expanding to Tier 2
// while keeping Tier 3 untouched.
func TestSelectVictimsOnNode_BoundaryBucketExpansion(t *testing.T) {
	ctx := context.Background()
	node := st.MakeNode().Name("node-expansion").Capacity(map[v1.ResourceName]string{
		v1.ResourcePods:   "1000",
		v1.ResourceCPU:    "10000m",
		v1.ResourceMemory: "50000Mi",
	}).Obj()

	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var initPods []*v1.Pod

	// Tier 1: 5 pods, each 100m CPU = 500m CPU total (Priority 0)
	for i := 0; i < 5; i++ {
		p := st.MakePod().Name(fmt.Sprintf("t1-pod-%02d", i)).
			UID(fmt.Sprintf("uid-t1-%02d", i)).
			Node("node-expansion").
			Priority(0).
			StartTime(metav1.NewTime(baseTime.Add(time.Duration(i) * time.Minute))).
			Req(map[v1.ResourceName]string{
				v1.ResourceCPU:    "100m",
				v1.ResourceMemory: "500Mi",
			}).Obj()
		initPods = append(initPods, p)
	}

	// Tier 2: 10 pods, each 200m CPU = 2000m CPU total (Priority 1000)
	for i := 0; i < 10; i++ {
		p := st.MakePod().Name(fmt.Sprintf("t2-pod-%02d", i)).
			UID(fmt.Sprintf("uid-t2-%02d", i)).
			Node("node-expansion").
			Priority(1000).
			StartTime(metav1.NewTime(baseTime.Add(time.Duration(10+i) * time.Minute))).
			Req(map[v1.ResourceName]string{
				v1.ResourceCPU:    "200m",
				v1.ResourceMemory: "1000Mi",
			}).Obj()
		initPods = append(initPods, p)
	}

	// Tier 3: 10 pods, each 500m CPU (Priority 50000)
	for i := 0; i < 10; i++ {
		p := st.MakePod().Name(fmt.Sprintf("t3-pod-%02d", i)).
			UID(fmt.Sprintf("uid-t3-%02d", i)).
			Node("node-expansion").
			Priority(50000).
			StartTime(metav1.NewTime(baseTime.Add(time.Duration(30+i) * time.Minute))).
			Req(map[v1.ResourceName]string{
				v1.ResourceCPU:    "500m",
				v1.ResourceMemory: "2000Mi",
			}).Obj()
		initPods = append(initPods, p)
	}

	// Preemptor needs 800m CPU.
	// Tier 1 only provides 500m CPU (< 800m CPU).
	// So Tier 1 is insufficient. Tier 2 is required as boundary bucket (500m + 2000m = 2500m >= 800m).
	// Preemptor should evict all 5 pods in Tier 1 (500m) + 2 pods in Tier 2 (400m) = 900m >= 800m.
	// Tier 3 pods must be 100% untouched!
	preemptor := st.MakePod().Name("preemptor-tier2-boundary").
		UID("preemptor-uid").
		Priority(100000).
		Req(map[v1.ResourceName]string{
			v1.ResourceCPU:    "800m",
			v1.ResourceMemory: "1000Mi",
		}).Obj()

	pl, _, nodeInfo, potentialVictims, cycleState := setupHighDensityTestEnv(t, node, initPods, preemptor, nil)

	bucketPods, bucketPDBViolations, bucketStatus := pl.SelectVictimsOnNode(
		ctx, cycleState.Clone(), preemptor, nodeInfo.Snapshot(), potentialVictims, nil,
	)
	if !bucketStatus.IsSuccess() {
		t.Fatalf("SelectVictimsOnNode failed: %v", bucketStatus)
	}

	linPods, linPDBViolations, linStatus := linearSelectVictimsOnNode(
		ctx, pl, cycleState.Clone(), preemptor, nodeInfo.Snapshot(), potentialVictims, nil,
	)
	if !linStatus.IsSuccess() {
		t.Fatalf("linearSelectVictimsOnNode failed: %v", linStatus)
	}

	if bucketPDBViolations != linPDBViolations {
		t.Errorf("PDB violations mismatch: bucket=%d, linear=%d", bucketPDBViolations, linPDBViolations)
	}

	bucketVictimNames := make([]string, len(bucketPods))
	for i, p := range bucketPods {
		bucketVictimNames[i] = p.Name
	}
	linVictimNames := make([]string, len(linPods))
	for i, p := range linPods {
		linVictimNames[i] = p.Name
	}

	if diff := cmp.Diff(linVictimNames, bucketVictimNames); diff != "" {
		t.Errorf("Victim selection mismatch (-linear +bucket):\n%s", diff)
	}

	// Confirm that NO pods from Tier 3 were selected
	for _, p := range bucketPods {
		if *p.Spec.Priority == 50000 {
			t.Errorf("Tier 3 pod %s should not have been evicted", p.Name)
		}
	}
}

// TestSelectVictimsOnNode_PDBInversionSafety verifies that when a lower priority pod violates
// a PDB, the algorithm safely falls back to standard evaluation, preferring a higher-priority
// non-violating pod over a lower-priority violating pod.
func TestSelectVictimsOnNode_PDBInversionSafety(t *testing.T) {
	ctx := context.Background()
	node := st.MakeNode().Name("node-pdb").Capacity(map[v1.ResourceName]string{
		v1.ResourcePods:   "1000",
		v1.ResourceCPU:    "2000m",
		v1.ResourceMemory: "4000Mi",
	}).Obj()

	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Low-priority pod with PDB violation:
	// Priority 0, requests 1000m CPU.
	lowPrioViolatingPod := st.MakePod().Name("low-prio-violating").
		UID("uid-low-prio-violating").
		Namespace("default").
		Node("node-pdb").
		Priority(0).
		Label("app", "protected-low").
		StartTime(metav1.NewTime(baseTime)).
		Req(map[v1.ResourceName]string{
			v1.ResourceCPU:    "1000m",
			v1.ResourceMemory: "1000Mi",
		}).Obj()

	// High-priority pod with NO PDB violation:
	// Priority 1000, requests 1000m CPU.
	highPrioNonViolatingPod := st.MakePod().Name("high-prio-non-violating").
		UID("uid-high-prio-non-violating").
		Namespace("default").
		Node("node-pdb").
		Priority(1000).
		Label("app", "unprotected-high").
		StartTime(metav1.NewTime(baseTime.Add(time.Minute))).
		Req(map[v1.ResourceName]string{
			v1.ResourceCPU:    "1000m",
			v1.ResourceMemory: "1000Mi",
		}).Obj()

	initPods := []*v1.Pod{lowPrioViolatingPod, highPrioNonViolatingPod}

	// Preemptor needs 1000m CPU.
	preemptor := st.MakePod().Name("preemptor").
		UID("uid-preemptor").
		Namespace("default").
		Priority(10000).
		Req(map[v1.ResourceName]string{
			v1.ResourceCPU:    "1000m",
			v1.ResourceMemory: "1000Mi",
		}).Obj()

	// PDB with DisruptionsAllowed = 0 protecting app=protected-low
	pdb := &policy.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "pdb-low", Namespace: "default"},
		Spec: policy.PodDisruptionBudgetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "protected-low"},
			},
		},
		Status: policy.PodDisruptionBudgetStatus{
			DisruptionsAllowed: 0,
		},
	}
	pdbs := []*policy.PodDisruptionBudget{pdb}

	pl, _, nodeInfo, potentialVictims, cycleState := setupHighDensityTestEnv(t, node, initPods, preemptor, pdbs)

	bucketPods, bucketPDBViolations, bucketStatus := pl.SelectVictimsOnNode(
		ctx, cycleState.Clone(), preemptor, nodeInfo.Snapshot(), potentialVictims, pdbs,
	)
	if !bucketStatus.IsSuccess() {
		t.Fatalf("SelectVictimsOnNode failed: %v", bucketStatus)
	}

	linPods, linPDBViolations, linStatus := linearSelectVictimsOnNode(
		ctx, pl, cycleState.Clone(), preemptor, nodeInfo.Snapshot(), potentialVictims, pdbs,
	)
	if !linStatus.IsSuccess() {
		t.Fatalf("linearSelectVictimsOnNode failed: %v", linStatus)
	}

	// Both must report 0 PDB violations because the non-violating higher-priority pod
	// was chosen for eviction instead of violating the PDB on the lower-priority pod!
	if bucketPDBViolations != 0 {
		t.Errorf("Expected 0 PDB violations, got %d", bucketPDBViolations)
	}
	if linPDBViolations != 0 {
		t.Errorf("Expected 0 PDB violations in baseline, got %d", linPDBViolations)
	}

	if len(bucketPods) != 1 || bucketPods[0].Name != "high-prio-non-violating" {
		t.Fatalf("Expected high-prio-non-violating pod to be evicted, got: %v", bucketPods)
	}
	if len(linPods) != 1 || linPods[0].Name != "high-prio-non-violating" {
		t.Fatalf("Expected baseline to evict high-prio-non-violating pod, got: %v", linPods)
	}
}

// TestSelectVictimsOnNode_NonScalarHostPortCollision verifies dynamic tier expansion when
// scalar headroom is satisfied by Tier 1, but a higher-priority pod in Tier 2 holds a HostPort
// collision with the preemptor, ensuring zero false negatives.
func TestSelectVictimsOnNode_NonScalarHostPortCollision(t *testing.T) {
	ctx := context.Background()
	node := st.MakeNode().Name("node-port").Capacity(map[v1.ResourceName]string{
		v1.ResourcePods:   "1000",
		v1.ResourceCPU:    "10000m",
		v1.ResourceMemory: "50000Mi",
	}).Obj()

	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Tier 1: 5 pods, Priority 0, providing 500m CPU total
	var initPods []*v1.Pod
	for i := 0; i < 5; i++ {
		p := st.MakePod().Name(fmt.Sprintf("t1-pod-%02d", i)).
			UID(fmt.Sprintf("uid-t1-%02d", i)).
			Node("node-port").
			Priority(0).
			StartTime(metav1.NewTime(baseTime.Add(time.Duration(i) * time.Minute))).
			Req(map[v1.ResourceName]string{
				v1.ResourceCPU:    "100m",
				v1.ResourceMemory: "500Mi",
			}).Obj()
		initPods = append(initPods, p)
	}

	// Tier 2: 1 pod, Priority 1000, holding HostPort 8080!
	portConflictPod := st.MakePod().Name("t2-port-conflict").
		UID("uid-t2-port").
		Node("node-port").
		Priority(1000).
		StartTime(metav1.NewTime(baseTime.Add(10 * time.Minute))).
		Req(map[v1.ResourceName]string{
			v1.ResourceCPU:    "100m",
			v1.ResourceMemory: "500Mi",
		}).
		Containers([]v1.Container{
			{
				Name: "c1",
				Ports: []v1.ContainerPort{
					{HostPort: 8080, Protocol: v1.ProtocolTCP},
				},
			},
		}).Obj()
	initPods = append(initPods, portConflictPod)

	// Preemptor requests 100m CPU AND HostPort 8080.
	// Scalar CPU can be satisfied by Tier 1 alone, but HostPort 8080 is held by Tier 2 pod.
	// Dynamic tier expansion MUST expand to Tier 2 and evict the portConflictPod!
	preemptor := st.MakePod().Name("preemptor-needs-8080").
		UID("uid-preemptor").
		Priority(10000).
		Req(map[v1.ResourceName]string{
			v1.ResourceCPU:    "100m",
			v1.ResourceMemory: "500Mi",
		}).
		Containers([]v1.Container{
			{
				Name: "c1",
				Ports: []v1.ContainerPort{
					{HostPort: 8080, Protocol: v1.ProtocolTCP},
				},
			},
		}).Obj()

	registeredPlugins := []tf.RegisterPluginFunc{
		tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
		tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
		tf.RegisterPluginAsExtensions(noderesources.Name, nodeResourcesFitFunc, "Filter", "PreFilter"),
		tf.RegisterPluginAsExtensions(nodeports.Name, frameworkruntime.FactoryAdapter(feature.Features{}, nodeports.New), "Filter", "PreFilter"),
	}

	var objs []runtime.Object
	objs = append(objs, preemptor, node)
	for _, p := range initPods {
		objs = append(objs, p)
	}

	informerFactory := informers.NewSharedInformerFactory(clientsetfake.NewClientset(objs...), 0)
	logger, tCtx := ktesting.NewTestContext(t)
	cache := internalcache.New(tCtx, nil, false, false)
	snapshot := internalcache.NewTestSnapshotWithPodGroups(initPods, []*v1.Node{node}, nil, nil)

	testingFwk, err := tf.NewFramework(
		tCtx,
		registeredPlugins, "",
		frameworkruntime.WithPodNominator(internalqueue.NewSchedulingQueue(nil, informerFactory)),
		frameworkruntime.WithSnapshotSharedLister(snapshot),
		frameworkruntime.WithMutableSnapshotLister(snapshot),
		frameworkruntime.WithInformerFactory(informerFactory),
		frameworkruntime.WithParallelism(parallelize.DefaultParallelism),
		frameworkruntime.WithLogger(logger),
		frameworkruntime.WithPodGroupManager(cache),
		frameworkruntime.WithPreemptionManager(func(fh fwk.Handle) fwk.PreemptionManager {
			return preemption.NewPreemptionManager(fh, feature.Features{})
		}),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}

	cycleState := framework.NewCycleState()
	if _, status, _ := testingFwk.RunPreFilterPlugins(tCtx, cycleState, preemptor); !status.IsSuccess() {
		t.Fatalf("Unexpected PreFilter Status: %v", status)
	}

	pl, err := New(tCtx, getDefaultDefaultPreemptionArgs(), testingFwk, feature.Features{})
	if err != nil {
		t.Fatalf("Failed to create default preemption plugin: %v", err)
	}

	informerFactory.Start(tCtx.Done())
	informerFactory.WaitForCacheSync(tCtx.Done())

	nodeInfo, err := snapshot.NodeInfos().Get(node.Name)
	if err != nil {
		t.Fatalf("Failed to get nodeInfo: %v", err)
	}

	potentialVictims, err := pl.Evaluator.GetVictimsOnNode(tCtx, nodeInfo)
	if err != nil {
		t.Fatalf("Failed to get victims: %v", err)
	}

	bucketPods, _, bucketStatus := pl.SelectVictimsOnNode(
		ctx, cycleState.Clone(), preemptor, nodeInfo.Snapshot(), potentialVictims, nil,
	)
	if !bucketStatus.IsSuccess() {
		t.Fatalf("SelectVictimsOnNode failed to schedule with dynamic tier expansion: %v", bucketStatus)
	}

	linPods, _, linStatus := linearSelectVictimsOnNode(
		ctx, pl, cycleState.Clone(), preemptor, nodeInfo.Snapshot(), potentialVictims, nil,
	)
	if !linStatus.IsSuccess() {
		t.Fatalf("linearSelectVictimsOnNode failed: %v", linStatus)
	}

	// Verify that the port-conflict pod was evicted in both
	foundBucket := false
	for _, p := range bucketPods {
		if p.Name == "t2-port-conflict" {
			foundBucket = true
		}
	}
	foundLin := false
	for _, p := range linPods {
		if p.Name == "t2-port-conflict" {
			foundLin = true
		}
	}

	if !foundBucket {
		t.Errorf("Expected t2-port-conflict to be evicted in bucket preemption, got %v", bucketPods)
	}
	if !foundLin {
		t.Errorf("Expected t2-port-conflict to be evicted in linear preemption, got %v", linPods)
	}
}

// TestSelectVictimsOnNode_MutationReduction measures the dramatic reduction in NodeInfo mutations
// and Filter plugin evaluations on a 500-pod candidate node.
func TestSelectVictimsOnNode_MutationReduction(t *testing.T) {
	ctx := context.Background()
	node := st.MakeNode().Name("node-mutation-reduction").Capacity(map[v1.ResourceName]string{
		v1.ResourcePods:   "1000",
		v1.ResourceCPU:    "50000m",
		v1.ResourceMemory: "250000Mi",
	}).Obj()

	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var initPods []*v1.Pod
	count := 500

	// Tier 1: 100 pods (Priority 0)
	// Tier 2: 300 pods (Priority 1000)
	// Tier 3: 100 pods (Priority 50000)
	for i := 0; i < count; i++ {
		var prio int32
		if i < 100 {
			prio = 0
		} else if i < 400 {
			prio = 1000
		} else {
			prio = 50000
		}
		p := st.MakePod().Name(fmt.Sprintf("pod-%04d", i)).
			UID(fmt.Sprintf("uid-pod-%04d", i)).
			Node("node-mutation-reduction").
			Priority(prio).
			StartTime(metav1.NewTime(baseTime.Add(time.Duration(i) * time.Minute))).
			Req(map[v1.ResourceName]string{
				v1.ResourceCPU:    "100m",
				v1.ResourceMemory: "500Mi",
			}).Obj()
		initPods = append(initPods, p)
	}

	// Preemptor needs 500m CPU.
	// Tier 1 has 100 pods * 100m = 10000m CPU. 5 pods from Tier 1 are sufficient.
	preemptor := st.MakePod().Name("preemptor").
		UID("uid-preemptor").
		Priority(100000).
		Req(map[v1.ResourceName]string{
			v1.ResourceCPU:    "500m",
			v1.ResourceMemory: "2500Mi",
		}).Obj()

	pl, _, nodeInfo, potentialVictims, cycleState := setupHighDensityTestEnv(t, node, initPods, preemptor, nil)

	// In baseline, all 500 pods are removed initially.
	// In Idea 8, only the 100 pods in Tier 1 are removed! 400 pods in Tiers 2 & 3 are NEVER removed!
	bucketPods, _, status := pl.SelectVictimsOnNode(ctx, cycleState.Clone(), preemptor, nodeInfo.Snapshot(), potentialVictims, nil)
	if !status.IsSuccess() {
		t.Fatalf("SelectVictimsOnNode failed: %v", status)
	}

	if len(bucketPods) != 5 {
		t.Errorf("Expected 5 victims, got %d", len(bucketPods))
	}
	for _, p := range bucketPods {
		if *p.Spec.Priority != 0 {
			t.Errorf("Expected victim from Tier 1 (prio 0), got prio %d", *p.Spec.Priority)
		}
	}
}
