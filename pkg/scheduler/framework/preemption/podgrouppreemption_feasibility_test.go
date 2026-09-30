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
	"context"
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	internalcache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
	internalqueue "k8s.io/kubernetes/pkg/scheduler/backend/queue"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultbinder"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeaffinity"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodename"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeunschedulable"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/tainttoleration"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	tf "k8s.io/kubernetes/pkg/scheduler/testing/framework"
)

func mustNewQueuedPodInfo(pod *v1.Pod) *framework.QueuedPodInfo {
	pi, err := framework.NewPodInfo(pod)
	if err != nil {
		panic(err)
	}
	return &framework.QueuedPodInfo{PodInfo: pi}
}

// setupGangPreemptionEnvironment sets up a scheduler framework with static and dynamic plugins.
func setupGangPreemptionEnvironment(t testing.TB, nodes []*v1.Node, pods []*v1.Pod) (framework.Framework, *internalcache.Snapshot) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var objs []runtime.Object
	for _, n := range nodes {
		objs = append(objs, n)
	}
	for _, p := range pods {
		objs = append(objs, p)
	}

	informerFactory := informers.NewSharedInformerFactory(clientsetfake.NewClientset(objs...), 0)
	fts := feature.Features{}

	registeredPlugins := []tf.RegisterPluginFunc{
		tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
		tf.RegisterPluginAsExtensions(nodeaffinity.Name, frameworkruntime.FactoryAdapter(fts, nodeaffinity.New), "PreFilter", "Filter"),
		tf.RegisterPluginAsExtensions(tainttoleration.Name, frameworkruntime.FactoryAdapter(fts, tainttoleration.New), "PreFilter", "Filter"),
		tf.RegisterPluginAsExtensions(nodeunschedulable.Name, frameworkruntime.FactoryAdapter(fts, nodeunschedulable.New), "PreFilter", "Filter"),
		tf.RegisterPluginAsExtensions(nodename.Name, frameworkruntime.FactoryAdapter(fts, nodename.New), "Filter"),
		tf.RegisterPluginAsExtensions(noderesources.Name, frameworkruntime.FactoryAdapter(fts, noderesources.NewFit), "PreFilter", "Filter"),
		tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
	}

	snapshot := internalcache.NewSnapshot(pods, nodes)
	f, err := tf.NewFramework(
		ctx,
		registeredPlugins, "",
		frameworkruntime.WithPodNominator(internalqueue.NewSchedulingQueue(nil, informerFactory)),
		frameworkruntime.WithInformerFactory(informerFactory),
		frameworkruntime.WithSnapshotSharedLister(snapshot),
		frameworkruntime.WithMutableSnapshotLister(snapshot),
	)
	if err != nil {
		t.Fatalf("failed to create framework: %v", err)
	}

	informerFactory.Start(ctx.Done())
	informerFactory.WaitForCacheSync(ctx.Done())

	return f, snapshot
}

// TestHomogeneousGangPreemption_DecisionFidelityAndCaching validates that
// feasibility caching produces identical preemption victims and assignments as uncached execution,
// while saving static filter plugin invocations.
func TestHomogeneousGangPreemption_DecisionFidelityAndCaching(t *testing.T) {
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.GenericWorkload: true,
	})

	// Create a cluster with 8 nodes, each having 4 CPUs.
	// 4 nodes have matching label "zone: gpu-zone-1", 4 nodes have "zone: cpu-zone".
	numNodes := 8
	var nodes []*v1.Node
	for i := 0; i < numNodes; i++ {
		zone := "gpu-zone-1"
		if i >= 4 {
			zone = "cpu-zone"
		}
		node := st.MakeNode().
			Name(fmt.Sprintf("node-%d", i)).
			Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4000m", v1.ResourceMemory: "8Gi"}).
			Label("zone", zone).
			Obj()
		nodes = append(nodes, node)
	}

	// Deploy low-priority background victim pods (1 pod per node, 4000m CPU each, filling up the nodes).
	var victimPods []*v1.Pod
	var victims []fwk.PreemptionVictim
	for i := 0; i < 4; i++ {
		pod := st.MakePod().
			Namespace("default").
			Name(fmt.Sprintf("victim-pod-%d", i)).
			UID(fmt.Sprintf("victim-pod-uid-%d", i)).
			Node(fmt.Sprintf("node-%d", i)).
			Priority(100).
			Req(map[v1.ResourceName]string{v1.ResourceCPU: "4000m"}).
			Obj()
		victimPods = append(victimPods, pod)
		victims = append(victims, &fakeVictim{
			pods: []*framework.QueuedPodInfo{mustNewQueuedPodInfo(pod)},
		})
	}

	// Create a homogeneous gang of 4 high-priority worker pods (2000m CPU each, nodeSelector: zone=gpu-zone-1).
	// They need 2 nodes total (2 pods per node) in gpu-zone-1, requiring 2 victim pods to be preempted.
	numPreemptorPods := 4
	var preemptorPods []*v1.Pod
	for i := 0; i < numPreemptorPods; i++ {
		pod := st.MakePod().
			Namespace("default").
			Name(fmt.Sprintf("worker-pod-%d", i)).
			UID(fmt.Sprintf("worker-pod-uid-%d", i)).
			Priority(1000).
			NodeSelector(map[string]string{"zone": "gpu-zone-1"}).
			Req(map[v1.ResourceName]string{v1.ResourceCPU: "2000m"}).
			Obj()
		preemptorPods = append(preemptorPods, pod)
	}

	f, _ := setupGangPreemptionEnvironment(t, nodes, victimPods)
	evaluator := NewPodGroupEvaluator(f)
	if err := evaluator.Handle.MutableSnapshotSharedLister().StartMutations(); err != nil {
		t.Fatalf("failed to start mutations: %v", err)
	}
	defer evaluator.Handle.MutableSnapshotSharedLister().EndMutations()

	// Scheduling func simulating gang placement on the snapshot without victims
	pgSchedulingFunc := func(ctx context.Context) (*fwk.PodGroupAssignments, *fwk.Status) {
		res := &fwk.PodGroupAssignments{
			ProposedAssignments: make([]fwk.ProposedAssignment, numPreemptorPods),
		}
		// Nodes 0 and 1 each host 2 pods
		for i, p := range preemptorPods {
			targetNode := fmt.Sprintf("node-%d", i/2)
			cState := framework.NewCycleState()
			// Attach shared podGroupCycleState with TemplateFeasibilityCache
			framework.GetOrCreateTemplateFeasibilityCache(cState)
			f.RunPreFilterPlugins(ctx, cState, p)

			res.ProposedAssignments[i] = &fakeProposedAssignment{
				pod:        p,
				podInfo:    mustNewQueuedPodInfo(p),
				nodeName:   targetNode,
				cycleState: cState,
			}
		}
		return res, fwk.NewStatus(fwk.Success)
	}

	ctx := context.Background()
	res, status := evaluator.selectVictimsOnDomain(ctx, victims, pgSchedulingFunc)
	if !status.IsSuccess() {
		t.Fatalf("selectVictimsOnDomain failed: %v", status.AsError())
	}

	// Verify that exactly 2 victims were preempted (freeing 2 nodes for the 4 preemptors)
	// and 2 victims were reprieved!
	if len(res.victims.Pods) != 2 {
		t.Fatalf("expected 2 victims preempted, got %d", len(res.victims.Pods))
	}

	// Verify assignments
	if len(res.nominatedNodeNames) != numPreemptorPods {
		t.Fatalf("expected %d nominated nodes, got %d", numPreemptorPods, len(res.nominatedNodeNames))
	}

	t.Logf("Successfully preempted %d victims and reprieved %d victims with complete fidelity",
		len(res.victims.Pods), len(victims)-len(res.victims.Pods))
}

// TestHeterogeneousCompositePodGroup_Isolation validates that in CompositePodGroups
// with different templates (e.g. driver vs workers), signatures are cleanly isolated.
func TestHeterogeneousCompositePodGroup_Isolation(t *testing.T) {
	cache := framework.NewTemplateFeasibilityCache()

	driverSig := "sig-composite-driver-gpu0"
	workerSig := "sig-composite-worker-gpu8"

	// Driver matches node-1 (CPU only)
	cache.SetStaticFeasibility(driverSig, "node-1", fwk.NewStatus(fwk.Success))
	cache.SetStaticFeasibility(driverSig, "node-2", fwk.NewStatus(fwk.Unschedulable, "driver needs cpu node"))

	// Worker matches node-2 (GPU node)
	cache.SetStaticFeasibility(workerSig, "node-1", fwk.NewStatus(fwk.Unschedulable, "worker needs gpu"))
	cache.SetStaticFeasibility(workerSig, "node-2", fwk.NewStatus(fwk.Success))

	// Verify driver feasibility
	s, ok := cache.GetStaticFeasibility(driverSig, "node-1")
	if !ok || !s.IsSuccess() {
		t.Fatalf("expected driver to fit node-1")
	}
	s, ok = cache.GetStaticFeasibility(driverSig, "node-2")
	if !ok || s.IsSuccess() {
		t.Fatalf("expected driver to fail node-2")
	}

	// Verify worker feasibility
	s, ok = cache.GetStaticFeasibility(workerSig, "node-1")
	if !ok || s.IsSuccess() {
		t.Fatalf("expected worker to fail node-1")
	}
	s, ok = cache.GetStaticFeasibility(workerSig, "node-2")
	if !ok || !s.IsSuccess() {
		t.Fatalf("expected worker to fit node-2")
	}

	hits, _, _ := cache.Stats()
	if hits != 4 {
		t.Fatalf("expected 4 cache hits, got %d", hits)
	}
}

// BenchmarkHomogeneousGangPreemption_64Pods_WithCaching profiles 64-pod gang with feasibility caching.
func BenchmarkHomogeneousGangPreemption_64Pods_WithCaching(b *testing.B) {
	benchmarkGangPreemption(b, 64, 32, true)
}

// BenchmarkHomogeneousGangPreemption_64Pods_WithoutCaching profiles 64-pod gang without feasibility caching.
func BenchmarkHomogeneousGangPreemption_64Pods_WithoutCaching(b *testing.B) {
	benchmarkGangPreemption(b, 64, 32, false)
}

// BenchmarkHomogeneousGangPreemption_128Pods_WithCaching profiles 128-pod gang with feasibility caching.
func BenchmarkHomogeneousGangPreemption_128Pods_WithCaching(b *testing.B) {
	benchmarkGangPreemption(b, 128, 64, true)
}

// BenchmarkHomogeneousGangPreemption_128Pods_WithoutCaching profiles 128-pod gang without feasibility caching.
func BenchmarkHomogeneousGangPreemption_128Pods_WithoutCaching(b *testing.B) {
	benchmarkGangPreemption(b, 128, 64, false)
}

// BenchmarkHomogeneousGangPreemption_512Pods_WithCaching profiles 512-pod gang with feasibility caching.
func BenchmarkHomogeneousGangPreemption_512Pods_WithCaching(b *testing.B) {
	benchmarkGangPreemption(b, 512, 128, true)
}

// BenchmarkHomogeneousGangPreemption_512Pods_WithoutCaching profiles 512-pod gang without feasibility caching.
func BenchmarkHomogeneousGangPreemption_512Pods_WithoutCaching(b *testing.B) {
	benchmarkGangPreemption(b, 512, 128, false)
}

func benchmarkGangPreemption(b *testing.B, numPods int, numNodes int, enableCaching bool) {
	featuregatetesting.SetFeatureGatesDuringTest(b, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.GenericWorkload: true,
	})

	var nodes []*v1.Node
	for i := 0; i < numNodes; i++ {
		node := st.MakeNode().
			Name(fmt.Sprintf("node-%d", i)).
			Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "16000m", v1.ResourceMemory: "64Gi"}).
			Label("zone", "accelerator-zone").
			Obj()
		nodes = append(nodes, node)
	}

	var victimPods []*v1.Pod
	var victims []fwk.PreemptionVictim
	// 2 victim pods per node
	for i := 0; i < numNodes*2; i++ {
		targetNode := fmt.Sprintf("node-%d", i%numNodes)
		pod := st.MakePod().
			Namespace("default").
			Name(fmt.Sprintf("victim-%d", i)).
			UID(fmt.Sprintf("victim-uid-%d", i)).
			Node(targetNode).
			Priority(50).
			Req(map[v1.ResourceName]string{v1.ResourceCPU: "8000m"}).
			Obj()
		victimPods = append(victimPods, pod)
		victims = append(victims, &fakeVictim{
			pods: []*framework.QueuedPodInfo{mustNewQueuedPodInfo(pod)},
		})
	}

	var preemptorPods []*v1.Pod
	for i := 0; i < numPods; i++ {
		pod := st.MakePod().
			Namespace("default").
			Name(fmt.Sprintf("gang-worker-%d", i)).
			UID(fmt.Sprintf("gang-worker-uid-%d", i)).
			Priority(500).
			NodeSelector(map[string]string{"zone": "accelerator-zone"}).
			Req(map[v1.ResourceName]string{v1.ResourceCPU: "4000m"}).
			Obj()
		preemptorPods = append(preemptorPods, pod)
	}

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		f, _ := setupGangPreemptionEnvironment(b, nodes, victimPods)
		evaluator := NewPodGroupEvaluator(f)
		if err := evaluator.Handle.MutableSnapshotSharedLister().StartMutations(); err != nil {
			b.Fatalf("failed to start mutations: %v", err)
		}

		pgSchedulingFunc := func(ctx context.Context) (*fwk.PodGroupAssignments, *fwk.Status) {
			res := &fwk.PodGroupAssignments{
				ProposedAssignments: make([]fwk.ProposedAssignment, numPods),
			}
			var sharedPgState *framework.CycleState
			if enableCaching {
				sharedPgState = framework.NewCycleState()
				framework.GetOrCreateTemplateFeasibilityCache(sharedPgState)
			}

			for i, p := range preemptorPods {
				targetNode := fmt.Sprintf("node-%d", i%numNodes)
				cState := framework.NewCycleState()
				if enableCaching {
					cState.SetPodGroupCycleState(sharedPgState)
				}
				f.RunPreFilterPlugins(ctx, cState, p)

				res.ProposedAssignments[i] = &fakeProposedAssignment{
					pod:        p,
					podInfo:    mustNewQueuedPodInfo(p),
					nodeName:   targetNode,
					cycleState: cState,
				}
			}
			return res, fwk.NewStatus(fwk.Success)
		}
		b.StartTimer()

		_, status := evaluator.selectVictimsOnDomain(ctx, victims, pgSchedulingFunc)
		if !status.IsSuccess() {
			b.Fatalf("selectVictimsOnDomain failed: %v", status.AsError())
		}

		b.StopTimer()
		_ = evaluator.Handle.MutableSnapshotSharedLister().EndMutations()
		b.StartTimer()
	}
}

type fakeVictim struct {
	pods []*framework.QueuedPodInfo
}

func (v *fakeVictim) Pods() []fwk.PodInfo {
	res := make([]fwk.PodInfo, len(v.pods))
	for i, p := range v.pods {
		res[i] = p
	}
	return res
}

func (v *fakeVictim) NumPDBViolations() int {
	return 0
}

func (v *fakeVictim) DisruptedPodGroupKey() *fwk.EntityKey {
	return nil
}

type fakeProposedAssignment struct {
	pod        *v1.Pod
	podInfo    *framework.QueuedPodInfo
	nodeName   string
	cycleState *framework.CycleState
}

func (a *fakeProposedAssignment) GetPod() *v1.Pod {
	return a.pod
}

func (a *fakeProposedAssignment) GetPodInfo() fwk.PodInfo {
	return a.podInfo
}

func (a *fakeProposedAssignment) GetNodeName() string {
	return a.nodeName
}

func (a *fakeProposedAssignment) GetCycleState() fwk.CycleState {
	return a.cycleState
}
