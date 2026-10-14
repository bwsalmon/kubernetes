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
	"sync/atomic"
	"testing"

	v1 "k8s.io/api/core/v1"
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
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	"k8s.io/kubernetes/pkg/scheduler/framework/preemption"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	tf "k8s.io/kubernetes/pkg/scheduler/testing/framework"
)

// InstrumentedFilterPlugin wraps NodeResourcesFit to count Filter evaluations and Add/Remove pod calls.
type instrumentedPlugin struct {
	filterCount    atomic.Int64
	addPodCount    atomic.Int64
	removePodCount atomic.Int64
	fitPlugin      fwk.Plugin
}

func (p *instrumentedPlugin) Name() string {
	return noderesources.Name
}

func (p *instrumentedPlugin) Filter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	p.filterCount.Add(1)
	return p.fitPlugin.(fwk.FilterPlugin).Filter(ctx, state, pod, nodeInfo)
}

func (p *instrumentedPlugin) PreFilter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	return p.fitPlugin.(fwk.PreFilterPlugin).PreFilter(ctx, state, pod, nodes)
}

func (p *instrumentedPlugin) PreFilterExtensions() fwk.PreFilterExtensions {
	return p
}

func (p *instrumentedPlugin) AddPod(ctx context.Context, state fwk.CycleState, podToSchedule *v1.Pod, podInfoToAdd fwk.PodInfo, nodeInfo fwk.NodeInfo) *fwk.Status {
	p.addPodCount.Add(1)
	return nil
}

func (p *instrumentedPlugin) RemovePod(ctx context.Context, state fwk.CycleState, podToSchedule *v1.Pod, podInfoToRemove fwk.PodInfo, nodeInfo fwk.NodeInfo) *fwk.Status {
	p.removePodCount.Add(1)
	return nil
}

func setupInstrumentedEnv(t *testing.T, node *v1.Node, pods []*v1.Pod, preemptor *v1.Pod) (*DefaultPreemption, fwk.Handle, fwk.NodeInfo, []*preemption.DomainVictim, fwk.CycleState, *instrumentedPlugin) {
	t.Helper()

	ip := &instrumentedPlugin{}
	pluginFactory := func(ctx context.Context, configuration runtime.Object, f fwk.Handle) (fwk.Plugin, error) {
		fitFactory := frameworkruntime.FactoryAdapter(feature.Features{}, noderesources.NewFit)
		fit, err := fitFactory(ctx, configuration, f)
		if err != nil {
			return nil, err
		}
		ip.fitPlugin = fit
		return ip, nil
	}

	registeredPlugins := []tf.RegisterPluginFunc{
		tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
		tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
		tf.RegisterPluginAsExtensions(noderesources.Name, pluginFactory, "Filter", "PreFilter"),
	}

	var objs []runtime.Object
	objs = append(objs, preemptor, node)
	for _, p := range pods {
		objs = append(objs, p)
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

	informerFactory.Start(ctx.Done())
	informerFactory.WaitForCacheSync(ctx.Done())

	mainNodeInfo, err := snapshot.NodeInfos().Get(node.Name)
	if err != nil {
		t.Fatalf("Failed to get nodeInfo: %v", err)
	}

	potentialVictims, err := pl.Evaluator.GetVictimsOnNode(ctx, mainNodeInfo)
	if err != nil {
		t.Fatalf("Failed to get potential victims: %v", err)
	}

	return pl, testingFwk, mainNodeInfo, potentialVictims, cycleState, ip
}

// TestDownside_EmpiricalMeasurements collects exact filter call counts and mutations across scenarios.
func TestDownside_EmpiricalMeasurements(t *testing.T) {
	ctx := context.Background()

	type scenario struct {
		name         string
		podCount     int
		setupPods    func(count int) []*v1.Pod
		preemptorCPU string
		nodeCPU      string
	}

	scenarios := []scenario{
		{
			name:     "LowDensity_2Pods_AllEvict",
			podCount: 2,
			setupPods: func(count int) []*v1.Pod {
				var pods []*v1.Pod
				for i := 0; i < count; i++ {
					pods = append(pods, st.MakePod().Name(fmt.Sprintf("p-%d", i)).UID(fmt.Sprintf("uid-%d", i)).
						Node("test-node").Priority(int32(100 + i)).
						Req(map[v1.ResourceName]string{v1.ResourceCPU: "500m"}).Obj())
				}
				return pods
			},
			preemptorCPU: "1000m",
			nodeCPU:      "1000m",
		},
		{
			name:     "LowDensity_4Pods_AllEvict",
			podCount: 4,
			setupPods: func(count int) []*v1.Pod {
				var pods []*v1.Pod
				for i := 0; i < count; i++ {
					pods = append(pods, st.MakePod().Name(fmt.Sprintf("p-%d", i)).UID(fmt.Sprintf("uid-%d", i)).
						Node("test-node").Priority(int32(100 + i)).
						Req(map[v1.ResourceName]string{v1.ResourceCPU: "250m"}).Obj())
				}
				return pods
			},
			preemptorCPU: "1000m",
			nodeCPU:      "1000m",
		},
		{
			name:     "LowDensity_4Pods_HalfReprieve",
			podCount: 4,
			setupPods: func(count int) []*v1.Pod {
				var pods []*v1.Pod
				for i := 0; i < count; i++ {
					pods = append(pods, st.MakePod().Name(fmt.Sprintf("p-%d", i)).UID(fmt.Sprintf("uid-%d", i)).
						Node("test-node").Priority(int32(100 + i)).
						Req(map[v1.ResourceName]string{v1.ResourceCPU: "250m"}).Obj())
				}
				return pods
			},
			preemptorCPU: "500m",
			nodeCPU:      "1000m",
		},
		{
			name:     "LowDensity_6Pods_AlternatingFitEvict",
			podCount: 6,
			setupPods: func(count int) []*v1.Pod {
				var pods []*v1.Pod
				for i := 0; i < count; i++ {
					cpu := "50m"
					if i%2 == 1 {
						cpu = "800m" // will fail to reprieve
					}
					pods = append(pods, st.MakePod().Name(fmt.Sprintf("p-%d", i)).UID(fmt.Sprintf("uid-%d", i)).
						Node("test-node").Priority(int32(100 + i)).
						Req(map[v1.ResourceName]string{v1.ResourceCPU: cpu}).Obj())
				}
				return pods
			},
			preemptorCPU: "200m",
			nodeCPU:      "1000m",
		},
		{
			name:     "Adversarial_Alternating_50Pods",
			podCount: 50,
			setupPods: func(count int) []*v1.Pod {
				var pods []*v1.Pod
				for i := 0; i < count; i++ {
					cpu := "1m"
					if i%2 == 1 {
						cpu = "900m" // fails reprieve
					}
					pods = append(pods, st.MakePod().Name(fmt.Sprintf("p-%d", i)).UID(fmt.Sprintf("uid-%d", i)).
						Node("test-node").Priority(int32(100 + i)).
						Req(map[v1.ResourceName]string{v1.ResourceCPU: cpu}).Obj())
				}
				return pods
			},
			preemptorCPU: "50m",
			nodeCPU:      "1000m",
		},
		{
			name:     "Adversarial_Alternating_200Pods",
			podCount: 200,
			setupPods: func(count int) []*v1.Pod {
				var pods []*v1.Pod
				for i := 0; i < count; i++ {
					cpu := "1m"
					if i%2 == 1 {
						cpu = "900m" // fails reprieve
					}
					pods = append(pods, st.MakePod().Name(fmt.Sprintf("p-%d", i)).UID(fmt.Sprintf("uid-%d", i)).
						Node("test-node").Priority(int32(100 + i)).
						Req(map[v1.ResourceName]string{v1.ResourceCPU: cpu}).Obj())
				}
				return pods
			},
			preemptorCPU: "50m",
			nodeCPU:      "1000m",
		},
		{
			name:     "Capacity_0PercentReprieve_100Pods",
			podCount: 100,
			setupPods: func(count int) []*v1.Pod {
				var pods []*v1.Pod
				for i := 0; i < count; i++ {
					pods = append(pods, st.MakePod().Name(fmt.Sprintf("p-%d", i)).UID(fmt.Sprintf("uid-%d", i)).
						Node("test-node").Priority(int32(100 + i)).
						Req(map[v1.ResourceName]string{v1.ResourceCPU: "10m"}).Obj())
				}
				return pods
			},
			preemptorCPU: "1000m", // Preemptor consumes 100% capacity, 0 pods can be reprieved
			nodeCPU:      "1000m",
		},
		{
			name:     "Capacity_0PercentReprieve_500Pods",
			podCount: 500,
			setupPods: func(count int) []*v1.Pod {
				var pods []*v1.Pod
				for i := 0; i < count; i++ {
					pods = append(pods, st.MakePod().Name(fmt.Sprintf("p-%d", i)).UID(fmt.Sprintf("uid-%d", i)).
						Node("test-node").Priority(int32(100 + i)).
						Req(map[v1.ResourceName]string{v1.ResourceCPU: "2m"}).Obj())
				}
				return pods
			},
			preemptorCPU: "1000m",
			nodeCPU:      "1000m",
		},
		{
			name:     "CascadeFailure_GrowThenFail_100Pods",
			podCount: 100,
			setupPods: func(count int) []*v1.Pod {
				var pods []*v1.Pod
				// First 15 pods fit (step doubles: 1, 2, 4, 8 -> step becomes 16)
				// At pod 15, node capacity exhausted, subsequent pods fail.
				for i := 0; i < count; i++ {
					cpu := "10m"
					if i >= 15 {
						cpu = "500m" // fails immediately
					}
					pods = append(pods, st.MakePod().Name(fmt.Sprintf("p-%d", i)).UID(fmt.Sprintf("uid-%d", i)).
						Node("test-node").Priority(int32(100 + i)).
						Req(map[v1.ResourceName]string{v1.ResourceCPU: cpu}).Obj())
				}
				return pods
			},
			preemptorCPU: "700m",
			nodeCPU:      "1000m",
		},
	}

	fmt.Println("\n--- EMPIRICAL MEASUREMENT RESULTS ---")
	fmt.Printf("%-38s | %-12s | %-12s | %-12s | %-12s | %-10s\n",
		"Scenario", "Lin Filter", "Log Filter", "Lin Mut", "Log Mut", "Filter Ratio (Log/Lin)")
	fmt.Println("-------------------------------------------------------------------------------------------------------")

	for _, sc := range scenarios {
		node := st.MakeNode().Name("test-node").Capacity(map[v1.ResourceName]string{
			v1.ResourceCPU:    sc.nodeCPU,
			v1.ResourceMemory: "10000Mi",
		}).Obj()
		initPods := sc.setupPods(sc.podCount)
		preemptor := st.MakePod().Name("preemptor").UID("preemptor-uid").Priority(10000).
			Req(map[v1.ResourceName]string{v1.ResourceCPU: sc.preemptorCPU, v1.ResourceMemory: "100Mi"}).Obj()

		// 1. Measure Linear
		plLin, _, nInfoLin, victimsLin, csLin, ipLin := setupInstrumentedEnv(t, node, initPods, preemptor)
		ipLin.filterCount.Store(0)
		ipLin.addPodCount.Store(0)
		ipLin.removePodCount.Store(0)

		_, _, linStatus := linearSelectVictimsOnNode(ctx, plLin, csLin.Clone(), preemptor, nInfoLin.Snapshot(), victimsLin, nil)
		if !linStatus.IsSuccess() {
			t.Fatalf("Linear failed in %s: %v", sc.name, linStatus)
		}
		linFilters := ipLin.filterCount.Load()
		linMuts := ipLin.addPodCount.Load() + ipLin.removePodCount.Load()

		// 2. Measure Logarithmic
		plLog, _, nInfoLog, victimsLog, csLog, ipLog := setupInstrumentedEnv(t, node, initPods, preemptor)
		ipLog.filterCount.Store(0)
		ipLog.addPodCount.Store(0)
		ipLog.removePodCount.Store(0)

		_, _, logStatus := plLog.SelectVictimsOnNode(ctx, csLog.Clone(), preemptor, nInfoLog.Snapshot(), victimsLog, nil)
		if !logStatus.IsSuccess() {
			t.Fatalf("Logarithmic failed in %s: %v", sc.name, logStatus)
		}
		logFilters := ipLog.filterCount.Load()
		logMuts := ipLog.addPodCount.Load() + ipLog.removePodCount.Load()

		ratio := float64(logFilters) / float64(linFilters)
		fmt.Printf("%-38s | %-12d | %-12d | %-12d | %-12d | %-10.2fx\n",
			sc.name, linFilters, logFilters, linMuts, logMuts, ratio)
	}
	fmt.Println("-------------------------------------------------------------------------------------------------------")
}

// nonMonotonicPlugin implements a custom filter plugin where adding pod B clears a failure caused by pod A.
type nonMonotonicPlugin struct {
	fwk.Plugin
}

func (p *nonMonotonicPlugin) Name() string {
	return "NonMonotonic"
}

func (p *nonMonotonicPlugin) Filter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	// If victim-a is on node without victim-b, filter fails.
	// If both victim-a and victim-b are present (or neither), filter succeeds.
	hasPodA := false
	hasPodB := false
	for _, pi := range nodeInfo.GetPods() {
		if pi.GetPod().Name == "victim-a" {
			hasPodA = true
		}
		if pi.GetPod().Name == "victim-b" {
			hasPodB = true
		}
	}
	if hasPodA && !hasPodB {
		return fwk.NewStatus(fwk.Unschedulable, "victim-a requires victim-b to be co-located")
	}
	return nil
}

func TestDownside_NonMonotonicPluginBehavior(t *testing.T) {
	ctx := context.Background()

	// Setup node with 4 pods: victim-a, victim-b, victim-c, victim-d
	// Candidate ordering (by importance): [victim-a, victim-b, victim-c, victim-d]
	node := st.MakeNode().Name("nm-node").Capacity(map[v1.ResourceName]string{
		v1.ResourceCPU:    "1000m",
		v1.ResourceMemory: "10000Mi",
	}).Obj()

	pA := st.MakePod().Name("victim-a").UID("uid-a").Node("nm-node").Priority(400).
		Req(map[v1.ResourceName]string{v1.ResourceCPU: "100m"}).Obj()
	pB := st.MakePod().Name("victim-b").UID("uid-b").Node("nm-node").Priority(300).
		Req(map[v1.ResourceName]string{v1.ResourceCPU: "100m"}).Obj()
	pC := st.MakePod().Name("victim-c").UID("uid-c").Node("nm-node").Priority(200).
		Req(map[v1.ResourceName]string{v1.ResourceCPU: "100m"}).Obj()
	pD := st.MakePod().Name("victim-d").UID("uid-d").Node("nm-node").Priority(100).
		Req(map[v1.ResourceName]string{v1.ResourceCPU: "100m"}).Obj()

	preemptor := st.MakePod().Name("preemptor").UID("preemptor-uid").Priority(1000).
		Req(map[v1.ResourceName]string{v1.ResourceCPU: "100m"}).Obj()

	initPods := []*v1.Pod{pA, pB, pC, pD}

	nmp := &nonMonotonicPlugin{}
	pluginFactory := func(ctx context.Context, configuration runtime.Object, f fwk.Handle) (fwk.Plugin, error) {
		return nmp, nil
	}

	registeredPlugins := []tf.RegisterPluginFunc{
		tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
		tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
		tf.RegisterPluginAsExtensions("NonMonotonic", pluginFactory, "Filter"),
	}

	var objs []runtime.Object
	objs = append(objs, preemptor, node)
	for _, p := range initPods {
		objs = append(objs, p)
	}

	informerFactory := informers.NewSharedInformerFactory(clientsetfake.NewClientset(objs...), 0)
	logger, testCtx := ktesting.NewTestContext(t)

	cache := internalcache.New(testCtx, nil, false, false)
	snapshot := internalcache.NewTestSnapshotWithPodGroups(initPods, []*v1.Node{node}, nil, nil)

	testingFwk, err := tf.NewFramework(
		testCtx,
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
	pl, err := New(testCtx, getDefaultDefaultPreemptionArgs(), testingFwk, feature.Features{})
	if err != nil {
		t.Fatalf("Failed to create default preemption plugin: %v", err)
	}

	informerFactory.Start(testCtx.Done())
	informerFactory.WaitForCacheSync(testCtx.Done())

	mainNodeInfo, err := snapshot.NodeInfos().Get(node.Name)
	if err != nil {
		t.Fatalf("Failed to get nodeInfo: %v", err)
	}

	potentialVictims, err := pl.Evaluator.GetVictimsOnNode(testCtx, mainNodeInfo)
	if err != nil {
		t.Fatalf("Failed to get potential victims: %v", err)
	}

	// 1. Run Linear
	linPods, _, linStatus := linearSelectVictimsOnNode(ctx, pl, cycleState.Clone(), preemptor, mainNodeInfo.Snapshot(), potentialVictims, nil)
	if !linStatus.IsSuccess() {
		t.Fatalf("Linear failed: %v", linStatus)
	}

	// 2. Run Logarithmic
	logPods, _, logStatus := pl.SelectVictimsOnNode(ctx, cycleState.Clone(), preemptor, mainNodeInfo.Snapshot(), potentialVictims, nil)
	if !logStatus.IsSuccess() {
		t.Fatalf("Logarithmic failed: %v", logStatus)
	}

	var linNames, logNames []string
	for _, p := range linPods {
		linNames = append(linNames, p.Name)
	}
	for _, p := range logPods {
		logNames = append(logNames, p.Name)
	}

	fmt.Printf("\n--- NON-MONOTONIC PLUGIN COMPARISON ---\n")
	fmt.Printf("Linear selected victims (evicted):      %v\n", linNames)
	fmt.Printf("Logarithmic selected victims (evicted): %v\n", logNames)
	fmt.Printf("---------------------------------------\n")
}

func TestDownside_NonMonotonicDivergence(t *testing.T) {
	ctx := context.Background()

	// Setup node with 4 pods: p0, victim-a, victim-b, victim-c
	// Candidate ordering: [p0 (priority 500), victim-a (400), victim-b (300), victim-c (200)]
	// Under linear: p0 fits. victim-a evaluated alone: FAILS! victim-a evicted!
	// Under logarithmic: p0 fits (step=2). Batch of size 2 is [victim-a, victim-b].
	// Both victim-a and victim-b are added simultaneously! Non-monotonic plugin succeeds!
	// Logarithmic reprieves BOTH victim-a and victim-b, evicting 0 victims!
	node := st.MakeNode().Name("nm-node2").Capacity(map[v1.ResourceName]string{
		v1.ResourceCPU:    "1000m",
		v1.ResourceMemory: "10000Mi",
	}).Obj()

	p0 := st.MakePod().Name("p0").UID("uid-0").Node("nm-node2").Priority(500).
		Req(map[v1.ResourceName]string{v1.ResourceCPU: "100m"}).Obj()
	pA := st.MakePod().Name("victim-a").UID("uid-a").Node("nm-node2").Priority(400).
		Req(map[v1.ResourceName]string{v1.ResourceCPU: "100m"}).Obj()
	pB := st.MakePod().Name("victim-b").UID("uid-b").Node("nm-node2").Priority(300).
		Req(map[v1.ResourceName]string{v1.ResourceCPU: "100m"}).Obj()
	pC := st.MakePod().Name("victim-c").UID("uid-c").Node("nm-node2").Priority(200).
		Req(map[v1.ResourceName]string{v1.ResourceCPU: "100m"}).Obj()

	preemptor := st.MakePod().Name("preemptor").UID("preemptor-uid").Priority(1000).
		Req(map[v1.ResourceName]string{v1.ResourceCPU: "100m"}).Obj()

	initPods := []*v1.Pod{p0, pA, pB, pC}

	nmp := &nonMonotonicPlugin{}
	pluginFactory := func(ctx context.Context, configuration runtime.Object, f fwk.Handle) (fwk.Plugin, error) {
		return nmp, nil
	}

	registeredPlugins := []tf.RegisterPluginFunc{
		tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
		tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
		tf.RegisterPluginAsExtensions("NonMonotonic", pluginFactory, "Filter"),
	}

	var objs []runtime.Object
	objs = append(objs, preemptor, node)
	for _, p := range initPods {
		objs = append(objs, p)
	}

	informerFactory := informers.NewSharedInformerFactory(clientsetfake.NewClientset(objs...), 0)
	logger, testCtx := ktesting.NewTestContext(t)

	cache := internalcache.New(testCtx, nil, false, false)
	snapshot := internalcache.NewTestSnapshotWithPodGroups(initPods, []*v1.Node{node}, nil, nil)

	testingFwk, err := tf.NewFramework(
		testCtx,
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
	pl, err := New(testCtx, getDefaultDefaultPreemptionArgs(), testingFwk, feature.Features{})
	if err != nil {
		t.Fatalf("Failed to create default preemption plugin: %v", err)
	}

	informerFactory.Start(testCtx.Done())
	informerFactory.WaitForCacheSync(testCtx.Done())

	mainNodeInfo, err := snapshot.NodeInfos().Get(node.Name)
	if err != nil {
		t.Fatalf("Failed to get nodeInfo: %v", err)
	}

	potentialVictims, err := pl.Evaluator.GetVictimsOnNode(testCtx, mainNodeInfo)
	if err != nil {
		t.Fatalf("Failed to get potential victims: %v", err)
	}

	// 1. Run Linear
	linPods, _, linStatus := linearSelectVictimsOnNode(ctx, pl, cycleState.Clone(), preemptor, mainNodeInfo.Snapshot(), potentialVictims, nil)
	if !linStatus.IsSuccess() {
		t.Fatalf("Linear failed: %v", linStatus)
	}

	// 2. Run Logarithmic
	logPods, _, logStatus := pl.SelectVictimsOnNode(ctx, cycleState.Clone(), preemptor, mainNodeInfo.Snapshot(), potentialVictims, nil)
	if !logStatus.IsSuccess() {
		t.Fatalf("Logarithmic failed: %v", logStatus)
	}

	var linNames, logNames []string
	for _, p := range linPods {
		linNames = append(linNames, p.Name)
	}
	for _, p := range logPods {
		logNames = append(logNames, p.Name)
	}

	fmt.Printf("\n--- NON-MONOTONIC DIVERGENCE TEST ---\n")
	fmt.Printf("Candidates: [p0, victim-a, victim-b, victim-c]\n")
	fmt.Printf("Linear evicts:      %v\n", linNames)
	fmt.Printf("Logarithmic evicts: %v\n", logNames)
	fmt.Printf("-------------------------------------\n")

	// Verify that divergence indeed occurs
	if len(linNames) == len(logNames) {
		t.Errorf("Expected divergence between linear and logarithmic under non-monotonic plugin, but got identical length")
	}
}
