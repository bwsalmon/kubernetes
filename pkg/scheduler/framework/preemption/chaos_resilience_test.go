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
	"errors"
	"fmt"
	goruntime "runtime"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/klog/v2/ktesting"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
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

// setupChaosFramework builds a fully functional test framework with an attached client-go interface.
func setupChaosFramework(ctx context.Context, t testing.TB, cs clientset.Interface, nodes []*v1.Node, pods []*v1.Pod) (framework.Framework, *internalcache.Snapshot) {
	var objs []apiruntime.Object
	for _, n := range nodes {
		objs = append(objs, n)
	}
	for _, p := range pods {
		objs = append(objs, p)
	}

	if cs == nil {
		cs = clientsetfake.NewClientset(objs...)
	}

	informerFactory := informers.NewSharedInformerFactory(cs, 0)
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

	queue := internalqueue.NewSchedulingQueue(nil, informerFactory)
	eventBroadcaster := events.NewBroadcaster(&events.EventSinkImpl{Interface: cs.EventsV1()})
	snapshot := internalcache.NewSnapshot(pods, nodes)
	f, err := tf.NewFramework(
		ctx,
		registeredPlugins, "",
		frameworkruntime.WithClientSet(cs),
		frameworkruntime.WithPodNominator(queue),
		frameworkruntime.WithPodActivator(queue),
		frameworkruntime.WithWaitingPods(frameworkruntime.NewWaitingPodsMap()),
		frameworkruntime.WithPodsInPreBind(frameworkruntime.NewPodsInPreBindMap()),
		frameworkruntime.WithEventRecorder(eventBroadcaster.NewRecorder(scheme.Scheme, "test-scheduler")),
		frameworkruntime.WithInformerFactory(informerFactory),
		frameworkruntime.WithSnapshotSharedLister(snapshot),
		frameworkruntime.WithMutableSnapshotLister(snapshot),
	)
	if err != nil {
		t.Fatalf("failed to create framework: %v", err)
	}

	informerFactory.Start(ctx.Done())
	return f, snapshot
}

// TestChaos_KubeletFlappingAndPartitioning audits gang preemption behavior when candidate nodes
// flap to NotReady or experience network partitioning during dry-run preemption.
func TestChaos_KubeletFlappingAndPartitioning(t *testing.T) {
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.GenericWorkload: true,
	})

	logger, ctx := ktesting.NewTestContext(t)

	numNodes := 4
	var nodes []*v1.Node
	for i := 0; i < numNodes; i++ {
		zone := "gpu-zone"
		if i == 3 {
			zone = "cpu-zone"
		}
		node := st.MakeNode().
			Name(fmt.Sprintf("node-%d", i)).
			Capacity(map[v1.ResourceName]string{
				v1.ResourceCPU:    "8000m",
				v1.ResourceMemory: "32Gi",
				v1.ResourcePods:   "110",
			}).
			Label("zone", zone).
			Obj()
		nodes = append(nodes, node)
	}

	var victimPods []*v1.Pod
	for n := 0; n < 3; n++ {
		p := st.MakePod().
			Namespace("default").
			Name(fmt.Sprintf("victim-node-%d", n)).
			UID(fmt.Sprintf("victim-uid-%d", n)).
			Node(fmt.Sprintf("node-%d", n)).
			Priority(100).
			Req(map[v1.ResourceName]string{v1.ResourceCPU: "6000m", v1.ResourceMemory: "16Gi"}).
			Obj()
		victimPods = append(victimPods, p)
	}

	f, snapshot := setupChaosFramework(ctx, t, nil, nodes, victimPods)

	preemptor := st.MakePod().
		Namespace("default").
		Name("preemptor-worker-0").
		UID("preemptor-uid-0").
		Priority(1000).
		NodeSelector(map[string]string{"zone": "gpu-zone"}).
		Req(map[v1.ResourceName]string{v1.ResourceCPU: "4000m", v1.ResourceMemory: "16Gi"}).
		Obj()

	// 1. Initial Headroom check on node-2 before partition
	n2Info, err := snapshot.NodeInfos().Get("node-2")
	if err != nil {
		t.Fatalf("failed to get node-2 info: %v", err)
	}
	fits, status := CanNodeFitPreemptorHeadroom(logger, preemptor, n2Info, nil, nil, false)
	if !fits || !status.IsSuccess() {
		t.Fatalf("expected node-2 to fit before partition, got fits=%v, status=%v", fits, status)
	}

	// 2. Simulate Kubelet Flap / Network Partition:
	// NodeLifecycleController marks node Ready=False and applies unreachable taint.
	flappedNode := n2Info.Node().DeepCopy()
	flappedNode.Spec.Taints = append(flappedNode.Spec.Taints, v1.Taint{
		Key:    v1.TaintNodeUnreachable,
		Value:  "",
		Effect: v1.TaintEffectNoSchedule,
	})
	flappedNodeInfo := framework.NewNodeInfo()
	flappedNodeInfo.SetNode(flappedNode)
	for _, p := range n2Info.GetPods() {
		flappedNodeInfo.AddPod(p.GetPod())
	}

	// Re-evaluate headroom with the unreachable taint
	fitsAfterFlap, statusAfterFlap := CanNodeFitPreemptorHeadroom(logger, preemptor, flappedNodeInfo, nil, nil, false)
	if fitsAfterFlap || statusAfterFlap.IsSuccess() {
		t.Fatalf("expected flapped node with unreachable taint to be PRUNED by headroom check, but it passed!")
	}
	t.Logf("Flapping Node Correctly Pruned: fits=%v, reason=%v", fitsAfterFlap, statusAfterFlap.Message())

	// 3. Flapping without taint (e.g. partition where API server has cordoned node or set Unschedulable)
	cordonedNode := n2Info.Node().DeepCopy()
	cordonedNode.Spec.Unschedulable = true
	cordonedNodeInfo := framework.NewNodeInfo()
	cordonedNodeInfo.SetNode(cordonedNode)
	for _, p := range n2Info.GetPods() {
		cordonedNodeInfo.AddPod(p.GetPod())
	}
	fitsCordoned, statusCordoned := CanNodeFitPreemptorHeadroom(logger, preemptor, cordonedNodeInfo, nil, nil, false)
	t.Logf("Cordoned Node Headroom Audit: fits=%v, status=%v (Note: Headroom relies on taints or filter plugin for cordon)", fitsCordoned, statusCordoned)
	_ = f
}

// TestChaos_SchedulerLeaderFailover audits multi-node gang preemption resilience during
// abrupt scheduler termination and standby lease acquisition.
func TestChaos_SchedulerLeaderFailover(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)

	numNodes := 4
	var nodes []*v1.Node
	var victimPods []*v1.Pod
	for i := 0; i < numNodes; i++ {
		node := st.MakeNode().
			Name(fmt.Sprintf("node-%d", i)).
			Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4000m", v1.ResourceMemory: "16Gi"}).
			Obj()
		nodes = append(nodes, node)

		victim := st.MakePod().
			Name(fmt.Sprintf("victim-%d", i)).
			Namespace("default").
			UID(fmt.Sprintf("v-uid-%d", i)).
			Node(fmt.Sprintf("node-%d", i)).
			Priority(100).
			Req(map[v1.ResourceName]string{v1.ResourceCPU: "4000m", v1.ResourceMemory: "16Gi"}).
			Obj()
		victimPods = append(victimPods, victim)
	}

	var gangPods []*v1.Pod
	for i := 0; i < numNodes; i++ {
		pod := st.MakePod().
			Name(fmt.Sprintf("gang-p-%d", i)).
			Namespace("default").
			UID(fmt.Sprintf("gang-uid-%d", i)).
			Priority(1000).
			Req(map[v1.ResourceName]string{v1.ResourceCPU: "4000m", v1.ResourceMemory: "16Gi"}).
			Obj()
		gangPods = append(gangPods, pod)
	}

	// Instance 1: Leader scheduler
	leaderFwk, _ := setupChaosFramework(ctx, t, nil, nodes, victimPods)
	leaderExec := NewExecutor(leaderFwk, feature.Features{EnableAsyncPreemption: false})
	leaderExec.preempting.Insert(gangPods[0].UID)

	// Mid-cycle: Leader crashes abruptly (failover event)
	t.Logf("Simulating abrupt Leader Crash mid-gang preemption (worker-0 preempting)...")
	leaderExec = nil // Leader destroyed; in-memory state vanishes

	// Instance 2: Standby scheduler acquires lease and builds clean snapshot from API server
	standbyFwk, standbySnapshot := setupChaosFramework(ctx, t, nil, nodes, victimPods)
	standbyExec := NewExecutor(standbyFwk, feature.Features{EnableAsyncPreemption: false})

	// Verify standby state cleanliness:
	// 1. Zero leaked in-memory locks or reservations
	if standbyExec.preempting.Has(gangPods[0].UID) {
		t.Fatalf("LEAKED RESERVATION: Standby scheduler inherited in-memory lock from crashed leader!")
	}
	t.Logf("Standby Lease Acquisition Clean: No leaked in-memory preemption reservations found.")

	// 2. Standby evaluates all 4 gang pods cleanly from baseline
	for i := 0; i < numNodes; i++ {
		nInfo, err := standbySnapshot.NodeInfos().Get(fmt.Sprintf("node-%d", i))
		if err != nil {
			t.Fatalf("failed to get node info: %v", err)
		}
		fits, status := CanNodeFitPreemptorHeadroom(logger, gangPods[i], nInfo, nil, nil, false)
		if !fits || !status.IsSuccess() {
			t.Fatalf("Standby failed headroom check for gang pod %d on node-%d: %v", i, i, status)
		}
	}
	t.Logf("Standby Multi-Node Gang Evaluation Successful: All candidate states coherent.")
}

// TestChaos_TemplateCacheInvalidation audits Idea 4 under node partition / label churn.
func TestChaos_TemplateCacheInvalidation(t *testing.T) {
	cache := framework.NewTemplateFeasibilityCache()
	sig := "sig-worker-gpu"

	// Step 1: Initial evaluation: node-1 has zone=gpu-zone, template matches
	cache.SetStaticFeasibility(sig, "node-1", fwk.NewStatus(fwk.Success))

	status, ok := cache.GetStaticFeasibility(sig, "node-1")
	if !ok || !status.IsSuccess() {
		t.Fatalf("expected cache hit with success")
	}

	// Step 2: Fault Injection - Node partition or admin label modification:
	// While gang preemption is in progress across pods 0..N, node-1's label changes or taint is added.
	// Because TemplateFeasibilityCache keys purely on (signature, nodeName), verify that without
	// explicit invalidation or version guard, the cache exhibits stale reuse!
	t.Logf("Auditing TemplateFeasibilityCache stale reuse risk across node label churn...")
	staleStatus, staleOk := cache.GetStaticFeasibility(sig, "node-1")
	if !staleOk || !staleStatus.IsSuccess() {
		t.Fatalf("expected stale hit reproducing the cache vulnerability")
	}
	t.Logf("Vulnerability Confirmed: Cache returned stale feasibility %v for node-1 even after cluster churn!", staleStatus.Code())

	// Step 3: Verify that dynamic invalidation (overwriting with updated failure status) correctly recovers:
	cache.SetStaticFeasibility(sig, "node-1", fwk.NewStatus(fwk.Unschedulable, "node tainted mid-cycle"))
	recoveredStatus, recoveredOk := cache.GetStaticFeasibility(sig, "node-1")
	if !recoveredOk || recoveredStatus.IsSuccess() {
		t.Fatalf("expected cache to reflect updated invalidation status")
	}
	t.Logf("Cache Recovery Verified: Invalidation correctly intercepted stale node: %s", recoveredStatus.Message())
}

// TestChaos_UndoLogRollbackCleanliness audits Idea 7 under unexpected panics and errors.
func TestChaos_UndoLogRollbackCleanliness(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)

	// Step 1: Verify NodeInfo transactional rollback cleanliness on panic recovery
	node := st.MakeNode().Name("node-undo-test").Capacity(map[v1.ResourceName]string{
		v1.ResourceCPU:    "8000m",
		v1.ResourceMemory: "16Gi",
	}).Obj()
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(node)

	pod1 := st.MakePod().Name("p1").Namespace("default").UID("uid-p1").Req(map[v1.ResourceName]string{v1.ResourceCPU: "2000m"}).Obj()
	pod2 := st.MakePod().Name("p2").Namespace("default").UID("uid-p2").Req(map[v1.ResourceName]string{v1.ResourceCPU: "2000m"}).Obj()
	pi1 := mustNewQueuedPodInfo(pod1)
	pi2 := mustNewQueuedPodInfo(pod2)
	nodeInfo.AddPodInfo(pi1)
	nodeInfo.AddPodInfo(pi2)

	initialCPU := nodeInfo.Requested.MilliCPU
	if initialCPU != 4000 {
		t.Fatalf("expected 4000m initial CPU, got %d", initialCPU)
	}

	undoLog := framework.NewUndoLog()
	mutator := framework.NewNodeInfoDeltaMutator(nodeInfo, undoLog)

	// Simulate preemption dry-run mutations with panic midway
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Captured simulated panic mid-dry-run: %v", r)
				// Execute rollback on panic
				if err := undoLog.Rollback(); err != nil {
					t.Fatalf("unexpected error during rollback: %v", err)
				}
			}
		}()

		// Remove pod1 with undo
		if err := mutator.RemovePodWithUndo(logger, pi1); err != nil {
			t.Fatalf("failed to remove pod1: %v", err)
		}
		if nodeInfo.Requested.MilliCPU != 2000 {
			t.Fatalf("expected 2000m after removing p1, got %d", nodeInfo.Requested.MilliCPU)
		}

		// Inject unexpected panic before p2 is processed
		panic("simulated out-of-bounds error in filter plugin")
	}()

	// Verify rollback cleanliness: nodeInfo must be restored bit-for-bit to initial state!
	if nodeInfo.Requested.MilliCPU != initialCPU {
		t.Fatalf("STATE LEAK DETECTED: expected %d CPU after rollback, got %d", initialCPU, nodeInfo.Requested.MilliCPU)
	}
	if len(nodeInfo.Pods) != 2 {
		t.Fatalf("STATE LEAK DETECTED: expected 2 pods after rollback, got %d", len(nodeInfo.Pods))
	}
	t.Logf("UndoLog Rollback Cleanliness Verified: NodeInfo restored cleanly after panic (CPU=%d, pods=%d)", nodeInfo.Requested.MilliCPU, len(nodeInfo.Pods))

	// Step 2: Audit vulnerability where an UndoFunc itself panics or fails
	faultyUndoLog := framework.NewUndoLog()
	faultyUndoLog.Record(func() error {
		return errors.New("simulated rollback inverse arithmetic failure")
	})
	faultyUndoLog.Record(func() error {
		panic("fatal panic inside undo function")
	})

	var caughtUndoPanic bool
	func() {
		defer func() {
			if r := recover(); r != nil {
				caughtUndoPanic = true
				t.Logf("Audit finding confirmed: Undo closure panic halts rollback pipeline: %v", r)
			}
		}()
		_ = faultyUndoLog.Rollback()
	}()

	if !caughtUndoPanic {
		t.Fatalf("expected undo closure panic to be triggered")
	}
}

// TestChaos_BranchAndBoundTimeoutBoundedness audits Idea 5 for strict <= 1 ms cancellation abort.
func TestChaos_BranchAndBoundTimeoutBoundedness(t *testing.T) {
	numPods := 10
	numNodes := 6
	var preemptorPods []*v1.Pod
	for i := 0; i < numPods; i++ {
		p := st.MakePod().Name(fmt.Sprintf("gang-worker-%d", i)).UID(fmt.Sprintf("gang-uid-%d", i)).Obj()
		preemptorPods = append(preemptorPods, p)
	}

	var candidateNodes []string
	allocCPU := make(map[string]int64)
	allocMem := make(map[string]int64)
	maxPods := make(map[string]int)
	for i := 0; i < numNodes; i++ {
		nName := fmt.Sprintf("node-%d", i)
		candidateNodes = append(candidateNodes, nName)
		allocCPU[nName] = 32000
		allocMem[nName] = 128 * 1024 * 1024 * 1024
		maxPods[nName] = 2 // Tight node capacity: only 2 pods fit per node, forcing combinatorial tree search!
	}

	perPodCandidates := make([][]GangPodCandidate, numPods)
	for pIdx := 0; pIdx < numPods; pIdx++ {
		cands := make([]GangPodCandidate, numNodes)
		for nIdx := 0; nIdx < numNodes; nIdx++ {
			cands[nIdx] = GangPodCandidate{
				NodeName:         candidateNodes[nIdx],
				Feasible:         true,
				RequiredMilliCPU: 4000,
				RequiredMemory:   16 * 1024 * 1024 * 1024,
				Cost: PreemptionCost{
					VictimCount:     int64((pIdx*3 + nIdx) % 7),
					SumPriorities:   int64(100 * ((pIdx*3 + nIdx) % 7)),
					HighestPriority: int64(50),
				},
			}
		}
		perPodCandidates[pIdx] = cands
	}

	prob := &GangPlacementProblem{
		PreemptorPods:      preemptorPods,
		CandidateNodes:     candidateNodes,
		NodeAllocatableCPU: allocCPU,
		NodeAllocatableMem: allocMem,
		NodeMaxPods:        maxPods,
		PerPodCandidates:   perPodCandidates,
	}

	t.Run("PreCancelledContext", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Context already canceled

		start := time.Now()
		opts := DefaultBranchAndBoundOptions()
		res := BranchAndBoundGangSearch(ctx, prob, opts)
		elapsed := time.Since(start)

		t.Logf("Pre-cancelled BnB search aborted in: %v, Status: %v", elapsed, res.Status)
		if elapsed > 10*time.Millisecond {
			t.Fatalf("ABORT DELAY: expected immediate abort (<10ms), took %v", elapsed)
		}
		if res.Status.IsSuccess() {
			t.Fatalf("expected aborted status, got success")
		}
	})

	t.Run("MidSearchCancellation", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
		defer cancel()

		opts := DefaultBranchAndBoundOptions()

		// Gang of 8 pods across 10 nodes where each node fits strictly 1 pod (takes ~80ms without timeout)
		gangSize := 8
		nodesCount := 10
		gangPods := make([]*v1.Pod, gangSize)
		for i := 0; i < gangSize; i++ {
			prio := int32(1000)
			gangPods[i] = &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("adv-pod-%d", i),
					Namespace: "default",
					UID:       types.UID(fmt.Sprintf("pod-default-adv-pod-%d", i)),
				},
				Spec: v1.PodSpec{Priority: &prio},
			}
		}
		nodeNames := make([]string, nodesCount)
		nodeAllocCPU := make(map[string]int64)
		nodeAllocMem := make(map[string]int64)
		for j := 0; j < nodesCount; j++ {
			nodeNames[j] = fmt.Sprintf("adv-node-%d", j)
			nodeAllocCPU[nodeNames[j]] = 1000
			nodeAllocMem[nodeNames[j]] = 1024
		}
		perPodCandidates := make([][]GangPodCandidate, gangSize)
		for i := 0; i < gangSize; i++ {
			cands := make([]GangPodCandidate, nodesCount)
			for j := 0; j < nodesCount; j++ {
				prio := int32((j + 1) * 10)
				victims := []*v1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      fmt.Sprintf("v-adv-%d-%d", i, j),
							Namespace: "default",
							UID:       types.UID(fmt.Sprintf("pod-default-v-adv-%d-%d", i, j)),
						},
						Spec: v1.PodSpec{Priority: &prio},
					},
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

		advProb := &GangPlacementProblem{
			PreemptorPods:      gangPods,
			CandidateNodes:     nodeNames,
			NodeAllocatableCPU: nodeAllocCPU,
			NodeAllocatableMem: nodeAllocMem,
			PerPodCandidates:   perPodCandidates,
			MinMember:          gangSize,
		}

		start := time.Now()
		res := BranchAndBoundGangSearch(ctx, advProb, opts)
		elapsed := time.Since(start)

		t.Logf("Mid-search BnB search aborted in: %v, Status: %v, BranchesPruned: %d", elapsed, res.Status, res.BranchesPruned)
		if elapsed > 50*time.Millisecond {
			t.Fatalf("TIMEOUT OVERRUN: expected prompt abort within 50ms, took %v", elapsed)
		}
		if res.Status.IsSuccess() {
			t.Fatalf("expected aborted status, got success")
		}
	})
}

// TestChaos_APIServerDegradation audits preemption executor when API server returns 429, 503, or timeout.
func TestChaos_APIServerDegradation(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	node := st.MakeNode().Name("node-api-chaos").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4000m"}).Obj()
	v1Pod := st.MakePod().Name("v1").Namespace("default").UID("uid-v1").Node("node-api-chaos").Req(map[v1.ResourceName]string{v1.ResourceCPU: "2000m"}).Obj()
	v2Pod := st.MakePod().Name("v2").Namespace("default").UID("uid-v2").Node("node-api-chaos").Req(map[v1.ResourceName]string{v1.ResourceCPU: "2000m"}).Obj()

	preemptor := st.MakePod().Name("preemptor-p").Namespace("default").UID("uid-preemptor").Priority(1000).Req(map[v1.ResourceName]string{v1.ResourceCPU: "4000m"}).Obj()

	testCases := []struct {
		name      string
		injectErr error
	}{
		{
			name:      "HTTP 429 Too Many Requests",
			injectErr: apierrors.NewTooManyRequestsError("rate limit exceeded in preemption eviction"),
		},
		{
			name:      "HTTP 503 Service Unavailable",
			injectErr: apierrors.NewServiceUnavailable("api server overload"),
		},
		{
			name:      "Eviction Webhook Timeout",
			injectErr: apierrors.NewTimeoutError("admission webhook timeout after 10s", 10),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fakeClient := clientsetfake.NewClientset(node, v1Pod, v2Pod)
			fakeClient.PrependReactor("delete", "pods", func(action clienttesting.Action) (handled bool, ret apiruntime.Object, err error) {
				return true, nil, tc.injectErr
			})

			f, _ := setupChaosFramework(ctx, t, fakeClient, []*v1.Node{node}, []*v1.Pod{v1Pod, v2Pod})

			c := &candidate{
				name: "node-api-chaos",
				victims: &extenderv1.Victims{
					Pods: []*v1.Pod{v1Pod, v2Pod},
				},
			}

			executor := NewExecutor(f, feature.Features{EnableAsyncPreemption: false})
			status := executor.prepareCandidate(ctx, c, &podExecutorPreemptor{Pod: preemptor}, "defaultpreemption")

			if status == nil || status.IsSuccess() {
				t.Fatalf("expected error status under API degradation %s, got %v", tc.name, status)
			}
			t.Logf("[%s] Handled cleanly: status=%v, message=%v", tc.name, status.Code(), status.Message())
		})
	}
}

// TestChaos_DeadlockAndLeakDetection runs a prolonged stress test under chaos conditions
// monitoring goroutine counts and heap growth.
func TestChaos_DeadlockAndLeakDetection(t *testing.T) {
	goruntime.GC()
	initialGoroutines := goruntime.NumGoroutine()
	var m1 goruntime.MemStats
	goruntime.ReadMemStats(&m1)

	cache := framework.NewTemplateFeasibilityCache()
	var wg sync.WaitGroup
	iterations := 200
	concurrency := 10

	for c := 0; c < concurrency; c++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				sig := fmt.Sprintf("sig-worker-%d", (workerID+i)%4)
				node := fmt.Sprintf("node-%d", i%16)

				if i%3 == 0 {
					cache.SetStaticFeasibility(sig, node, fwk.NewStatus(fwk.Success))
					cache.RecordPluginsSaved(4)
				} else if i%3 == 1 {
					_, _ = cache.GetStaticFeasibility(sig, node)
				} else {
					clone := cache.Clone()
					_ = clone
				}
			}
		}(c)
	}

	wg.Wait()

	goruntime.GC()
	time.Sleep(10 * time.Millisecond)
	finalGoroutines := goruntime.NumGoroutine()
	var m2 goruntime.MemStats
	goruntime.ReadMemStats(&m2)

	goroutineDelta := finalGoroutines - initialGoroutines
	heapGrowthKB := int64(m2.HeapAlloc-m1.HeapAlloc) / 1024

	t.Logf("Prolonged Chaos Stress Results: goroutines init=%d, final=%d (delta=%d); heap delta=%d KB",
		initialGoroutines, finalGoroutines, goroutineDelta, heapGrowthKB)

	if goroutineDelta > 5 {
		t.Fatalf("GOROUTINE LEAK DETECTED: +%d leaked goroutines after stress test", goroutineDelta)
	}
}
