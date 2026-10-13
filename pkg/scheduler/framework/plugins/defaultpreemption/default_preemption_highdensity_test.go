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
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	fwk "k8s.io/kube-scheduler/framework"
	internalcache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
	internalqueue "k8s.io/kubernetes/pkg/scheduler/backend/queue"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/parallelize"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultbinder"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	"k8s.io/kubernetes/pkg/scheduler/framework/preemption"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	tf "k8s.io/kubernetes/pkg/scheduler/testing/framework"
)

// linearSelectVictimsOnNode is a reference baseline implementing the original linear O(K) victim reprieval algorithm.
func linearSelectVictimsOnNode(
	ctx context.Context,
	pl *DefaultPreemption,
	cycleState fwk.CycleState,
	preemptor *v1.Pod,
	nodeInfo fwk.NodeInfo,
	potentialVictims []*preemption.DomainVictim,
	pdbs []*policy.PodDisruptionBudget,
) ([]*v1.Pod, int, *fwk.Status) {
	logger := klog.FromContext(ctx)
	mainNodeName := nodeInfo.Node().Name
	nameToNode := map[string]fwk.NodeInfo{mainNodeName: nodeInfo}

	removeVictim := func(dv *preemption.DomainVictim) error {
		for _, pi := range dv.Pods() {
			nInfo := nameToNode[pi.GetPod().Spec.NodeName]
			if pi.GetPod().Spec.NodeName == mainNodeName {
				if err := nInfo.RemovePod(logger, pi.GetPod()); err != nil {
					return err
				}
			}
			status := pl.fh.RunPreFilterExtensionRemovePod(ctx, cycleState, preemptor, pi, nInfo)
			if !status.IsSuccess() {
				return status.AsError()
			}
		}
		return nil
	}

	addVictim := func(pu *preemption.DomainVictim) error {
		for _, pi := range pu.Pods() {
			nInfo := nameToNode[pi.GetPod().Spec.NodeName]
			if pi.GetPod().Spec.NodeName == mainNodeName {
				nInfo.AddPodInfo(pi)
			}
			status := pl.fh.RunPreFilterExtensionAddPod(ctx, cycleState, preemptor, pi, nInfo)
			if !status.IsSuccess() {
				return status.AsError()
			}
		}
		return nil
	}

	var eligibleVictims []*preemption.DomainVictim
	for _, victim := range potentialVictims {
		if pl.isPreemptionAllowedAcrossAllVictimNodes(victim, preemptor) {
			eligibleVictims = append(eligibleVictims, victim)
		}
	}
	if len(eligibleVictims) == 0 {
		return nil, 0, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "No preemption victims found for incoming pod")
	}

	for _, victim := range eligibleVictims {
		for name, nInfo := range victim.AffectedNodes() {
			if _, ok := nameToNode[name]; !ok {
				nameToNode[name] = nInfo
			}
		}
		if err := removeVictim(victim); err != nil {
			return nil, 0, fwk.AsStatus(err)
		}
	}

	if status := pl.fh.RunFilterPluginsWithNominatedPods(ctx, cycleState, preemptor, nodeInfo); !status.IsSuccess() {
		return nil, 0, status
	}

	sortedVictims := make([]*preemption.DomainVictim, len(eligibleVictims))
	copy(sortedVictims, eligibleVictims)
	sort.Slice(sortedVictims, func(i, j int) bool {
		return pl.MoreImportantVictim(sortedVictims[i], sortedVictims[j])
	})

	violatingVictims, nonViolatingVictims := preemption.FilterVictimsWithPDBViolation(sortedVictims, pdbs)
	var victims []*preemption.DomainVictim

	reprieveVictim := func(v *preemption.DomainVictim) (bool, error) {
		if err := addVictim(v); err != nil {
			return false, err
		}
		status := pl.fh.RunFilterPluginsWithNominatedPods(ctx, cycleState, preemptor, nodeInfo)
		fits := status.IsSuccess()
		if !fits {
			if err := removeVictim(v); err != nil {
				return false, err
			}
			victims = append(victims, v)
		}
		return fits, nil
	}

	numViolatingVictim := 0
	for _, violatingVictim := range violatingVictims {
		if fits, err := reprieveVictim(violatingVictim.Victim); err != nil {
			return nil, 0, fwk.AsStatus(err)
		} else if !fits {
			numViolatingVictim += violatingVictim.ViolateCount
		}
	}

	for _, v := range nonViolatingVictims {
		if _, err := reprieveVictim(v); err != nil {
			return nil, 0, fwk.AsStatus(err)
		}
	}

	if len(violatingVictims) != 0 && len(nonViolatingVictims) != 0 {
		sort.Slice(victims, func(i, j int) bool { return pl.MoreImportantVictim(victims[i], victims[j]) })
	}
	var victimPods []*v1.Pod
	for _, vi := range victims {
		for _, pi := range vi.Pods() {
			victimPods = append(victimPods, pi.GetPod())
		}
	}
	return victimPods, numViolatingVictim, nil
}

func setupHighDensityTestEnv(t *testing.T, node *v1.Node, pods []*v1.Pod, preemptor *v1.Pod, pdbs []*policy.PodDisruptionBudget) (*DefaultPreemption, fwk.Handle, fwk.NodeInfo, []*preemption.DomainVictim, fwk.CycleState) {
	t.Helper()
	registeredPlugins := []tf.RegisterPluginFunc{
		tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
		tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
		tf.RegisterPluginAsExtensions(noderesources.Name, nodeResourcesFitFunc, "Filter", "PreFilter"),
	}

	var objs []runtime.Object
	objs = append(objs, preemptor, node)
	for _, p := range pods {
		objs = append(objs, p)
	}
	for _, pdb := range pdbs {
		objs = append(objs, pdb)
	}

	informerFactory := informers.NewSharedInformerFactory(clientsetfake.NewClientset(objs...), 0)
	logger, ctx := ktesting.NewTestContext(t)

	cache := internalcache.New(ctx, nil, false, false)
	snapshot := internalcache.NewTestSnapshotWithPodGroups(pods, []*v1.Node{node}, nil, nil)

	testingFwk, err := tf.NewFramework(
		ctx,
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
	if _, status, _ := testingFwk.RunPreFilterPlugins(ctx, cycleState, preemptor); !status.IsSuccess() {
		t.Fatalf("Unexpected PreFilter Status: %v", status)
	}

	pl, err := New(ctx, getDefaultDefaultPreemptionArgs(), testingFwk, feature.Features{})
	if err != nil {
		t.Fatalf("Failed to create default preemption plugin: %v", err)
	}
	defaultPreemptionPlugin := pl

	informerFactory.Start(ctx.Done())
	informerFactory.WaitForCacheSync(ctx.Done())

	mainNodeInfo, err := snapshot.NodeInfos().Get(node.Name)
	if err != nil {
		t.Fatalf("Failed to get nodeInfo: %v", err)
	}

	potentialVictims, err := defaultPreemptionPlugin.Evaluator.GetVictimsOnNode(ctx, mainNodeInfo)
	if err != nil {
		t.Fatalf("Failed to get potential victims: %v", err)
	}

	return defaultPreemptionPlugin, testingFwk, mainNodeInfo, potentialVictims, cycleState
}

func TestSelectVictimsOnNode_HighDensityEquivalence(t *testing.T) {
	densities := []int{10, 50, 200, 500}

	for _, count := range densities {
		t.Run(fmt.Sprintf("Density_%d_Pods", count), func(t *testing.T) {
			ctx := context.Background()
			node := st.MakeNode().Name("node-1").Capacity(map[v1.ResourceName]string{
				v1.ResourceCPU:    "1000m",
				v1.ResourceMemory: "10000Mi",
			}).Obj()

			baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			var initPods []*v1.Pod
			for i := 0; i < count; i++ {
				prio := int32(100 + (i % 50))
				stTime := metav1.NewTime(baseTime.Add(time.Duration(i) * time.Minute))
				p := st.MakePod().Name(fmt.Sprintf("victim-pod-%04d", i)).
					UID(fmt.Sprintf("uid-%04d", i)).
					Node("node-1").
					Priority(prio).
					StartTime(stTime).
					Req(map[v1.ResourceName]string{
						v1.ResourceCPU:    "1m",
						v1.ResourceMemory: "10Mi",
					}).Obj()
				initPods = append(initPods, p)
			}

			// Preemptor requesting half the capacity (500m)
			preemptor := st.MakePod().Name("preemptor-pod").
				UID("preemptor-uid").
				Priority(10000).
				Req(map[v1.ResourceName]string{
					v1.ResourceCPU:    "500m",
					v1.ResourceMemory: "100Mi",
				}).Obj()

			pl, _, nodeInfo, potentialVictims, cycleState := setupHighDensityTestEnv(t, node, initPods, preemptor, nil)

			cycleState1 := cycleState.Clone()
			cycleState2 := cycleState.Clone()

			// 1. Run Logarithmic / Chunked implementation
			logPods, logPDBViolations, logStatus := pl.SelectVictimsOnNode(ctx, cycleState1, preemptor, nodeInfo.Snapshot(), potentialVictims, nil)
			if !logStatus.IsSuccess() {
				t.Fatalf("Logarithmic SelectVictimsOnNode failed: %v", logStatus)
			}

			// 2. Run Baseline Linear implementation
			linPods, linPDBViolations, linStatus := linearSelectVictimsOnNode(ctx, pl, cycleState2, preemptor, nodeInfo.Snapshot(), potentialVictims, nil)
			if !linStatus.IsSuccess() {
				t.Fatalf("Linear SelectVictimsOnNode failed: %v", linStatus)
			}

			// 3. Verify exact bit-for-bit equivalence
			if logPDBViolations != linPDBViolations {
				t.Errorf("PDB Violations mismatch: got %d, want %d", logPDBViolations, linPDBViolations)
			}

			logPodNames := make([]string, len(logPods))
			for i, p := range logPods {
				logPodNames[i] = p.Name
			}
			linPodNames := make([]string, len(linPods))
			for i, p := range linPods {
				linPodNames[i] = p.Name
			}

			if diff := cmp.Diff(linPodNames, logPodNames); diff != "" {
				t.Errorf("Selected victims mismatch between linear and logarithmic (-linear +logarithmic):\n%s", diff)
			}
		})
	}
}

func TestSelectVictimsOnNode_HeterogeneousKnapsackAndPDBBoundaries(t *testing.T) {
	ctx := context.Background()
	node := st.MakeNode().Name("node-knapsack").Capacity(map[v1.ResourceName]string{
		v1.ResourceCPU:    "1000m",
		v1.ResourceMemory: "10000Mi",
	}).Obj()

	var initPods []*v1.Pod
	var pdbs []*policy.PodDisruptionBudget

	pdb1 := &policy.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "pdb-1", Namespace: v1.NamespaceDefault},
		Spec: policy.PodDisruptionBudgetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"protected": "true"}},
		},
		Status: policy.PodDisruptionBudgetStatus{DisruptionsAllowed: 5},
	}
	pdbs = append(pdbs, pdb1)

	baseTime := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 100; i++ {
		cpuReq := "10m"
		if i%3 == 0 {
			cpuReq = "50m"
		} else if i%7 == 0 {
			cpuReq = "100m"
		}

		prio := int32(100 + (i % 20))
		stTime := metav1.NewTime(baseTime.Add(time.Duration(i) * time.Second))

		builder := st.MakePod().Name(fmt.Sprintf("het-pod-%03d", i)).
			UID(fmt.Sprintf("het-uid-%03d", i)).
			Node("node-knapsack").
			Priority(prio).
			StartTime(stTime).
			Req(map[v1.ResourceName]string{
				v1.ResourceCPU:    cpuReq,
				v1.ResourceMemory: "20Mi",
			})

		if i < 30 {
			builder.Label("protected", "true")
		}
		initPods = append(initPods, builder.Obj())
	}

	preemptor := st.MakePod().Name("preemptor-large").
		UID("preemptor-large-uid").
		Priority(5000).
		Req(map[v1.ResourceName]string{
			v1.ResourceCPU:    "600m",
			v1.ResourceMemory: "500Mi",
		}).Obj()

	pl, _, nodeInfo, potentialVictims, cycleState := setupHighDensityTestEnv(t, node, initPods, preemptor, pdbs)

	cycleState1 := cycleState.Clone()
	cycleState2 := cycleState.Clone()

	// 1. Run Logarithmic implementation
	logPods, logPDBViolations, logStatus := pl.SelectVictimsOnNode(ctx, cycleState1, preemptor, nodeInfo.Snapshot(), potentialVictims, pdbs)
	if !logStatus.IsSuccess() {
		t.Fatalf("Logarithmic SelectVictimsOnNode failed: %v", logStatus)
	}

	// 2. Run Linear baseline
	linPods, linPDBViolations, linStatus := linearSelectVictimsOnNode(ctx, pl, cycleState2, preemptor, nodeInfo.Snapshot(), potentialVictims, pdbs)
	if !linStatus.IsSuccess() {
		t.Fatalf("Linear SelectVictimsOnNode failed: %v", linStatus)
	}

	// 3. Verify exact equivalence
	if logPDBViolations != linPDBViolations {
		t.Errorf("PDB Violations mismatch: got %d, want %d", logPDBViolations, linPDBViolations)
	}

	logSet := sets.New[string]()
	for _, p := range logPods {
		logSet.Insert(p.Name)
	}
	linSet := sets.New[string]()
	for _, p := range linPods {
		linSet.Insert(p.Name)
	}

	if diff := cmp.Diff(linSet, logSet); diff != "" {
		t.Errorf("Selected victims set mismatch (-linear +logarithmic):\n%s", diff)
	}
}

func TestSelectVictimsOnNode_PR140999_DeterminismTieBreaking(t *testing.T) {
	ctx := context.Background()
	node := st.MakeNode().Name("node-tiebreak").Capacity(map[v1.ResourceName]string{
		v1.ResourceCPU:    "500m",
		v1.ResourceMemory: "5000Mi",
	}).Obj()

	sameTime := metav1.NewTime(time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC))
	var initPods []*v1.Pod
	for i := 0; i < 20; i++ {
		p := st.MakePod().Name(fmt.Sprintf("tie-pod-%02d", i)).
			UID(fmt.Sprintf("uid-tie-%02d", 20-i)).
			Node("node-tiebreak").
			Priority(100).
			StartTime(sameTime).
			Req(map[v1.ResourceName]string{
				v1.ResourceCPU:    "50m",
				v1.ResourceMemory: "50Mi",
			}).Obj()
		initPods = append(initPods, p)
	}

	preemptor := st.MakePod().Name("preemptor-tie").
		UID("preemptor-tie-uid").
		Priority(1000).
		Req(map[v1.ResourceName]string{
			v1.ResourceCPU:    "300m",
			v1.ResourceMemory: "300Mi",
		}).Obj()

	pl, _, nodeInfo, potentialVictims, cycleState := setupHighDensityTestEnv(t, node, initPods, preemptor, nil)

	cycleState1 := cycleState.Clone()
	cycleState2 := cycleState.Clone()

	logPods, logPDBViolations, logStatus := pl.SelectVictimsOnNode(ctx, cycleState1, preemptor, nodeInfo.Snapshot(), potentialVictims, nil)
	if !logStatus.IsSuccess() {
		t.Fatalf("Logarithmic SelectVictimsOnNode failed: %v", logStatus)
	}

	linPods, linPDBViolations, linStatus := linearSelectVictimsOnNode(ctx, pl, cycleState2, preemptor, nodeInfo.Snapshot(), potentialVictims, nil)
	if !linStatus.IsSuccess() {
		t.Fatalf("Linear SelectVictimsOnNode failed: %v", linStatus)
	}

	if logPDBViolations != linPDBViolations {
		t.Errorf("PDB Violations mismatch: got %d, want %d", logPDBViolations, linPDBViolations)
	}

	logPodNames := make([]string, len(logPods))
	for i, p := range logPods {
		logPodNames[i] = p.Name
	}
	linPodNames := make([]string, len(linPods))
	for i, p := range linPods {
		linPodNames[i] = p.Name
	}

	if diff := cmp.Diff(linPodNames, logPodNames); diff != "" {
		t.Errorf("Victims mismatch on deterministic tie-breaking (-linear +logarithmic):\n%s", diff)
	}
}

func TestSelectVictimsOnNode_FilterCallCountReduction(t *testing.T) {
	densities := []int{10, 50, 200, 500}

	for _, count := range densities {
		t.Run(fmt.Sprintf("Density_%d_Pods", count), func(t *testing.T) {
			ctx := context.Background()
			node := st.MakeNode().Name("node-1").Capacity(map[v1.ResourceName]string{
				v1.ResourceCPU:    "1000m",
				v1.ResourceMemory: "10000Mi",
			}).Obj()

			baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			var initPods []*v1.Pod
			for i := 0; i < count; i++ {
				prio := int32(100 + (i % 50))
				stTime := metav1.NewTime(baseTime.Add(time.Duration(i) * time.Minute))
				p := st.MakePod().Name(fmt.Sprintf("victim-pod-%04d", i)).
					UID(fmt.Sprintf("uid-%04d", i)).
					Node("node-1").
					Priority(prio).
					StartTime(stTime).
					Req(map[v1.ResourceName]string{
						v1.ResourceCPU:    "1m",
						v1.ResourceMemory: "10Mi",
					}).Obj()
				initPods = append(initPods, p)
			}

			// Preemptor requesting 200m CPU (so 80% of candidate pods fit and can be reprieved in bulk)
			preemptor := st.MakePod().Name("preemptor-pod").
				UID("preemptor-uid").
				Priority(10000).
				Req(map[v1.ResourceName]string{
					v1.ResourceCPU:    "200m",
					v1.ResourceMemory: "100Mi",
				}).Obj()

			pl, _, nodeInfo, potentialVictims, cycleState := setupHighDensityTestEnv(t, node, initPods, preemptor, nil)

			cycleState1 := cycleState.Clone()
			cycleState2 := cycleState.Clone()

			logPods, _, logStatus := pl.SelectVictimsOnNode(ctx, cycleState1, preemptor, nodeInfo.Snapshot(), potentialVictims, nil)
			if !logStatus.IsSuccess() {
				t.Fatalf("Logarithmic SelectVictimsOnNode failed: %v", logStatus)
			}

			linPods, _, linStatus := linearSelectVictimsOnNode(ctx, pl, cycleState2, preemptor, nodeInfo.Snapshot(), potentialVictims, nil)
			if !linStatus.IsSuccess() {
				t.Fatalf("Linear SelectVictimsOnNode failed: %v", linStatus)
			}

			if len(logPods) != len(linPods) {
				t.Fatalf("Victim count mismatch: logarithmic=%d, linear=%d", len(logPods), len(linPods))
			}
		})
	}
}
