/*
Copyright 2015 The Kubernetes Authors.

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

package extender

// This file tests scheduler extender.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	"k8s.io/kubernetes/pkg/scheduler"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	testutils "k8s.io/kubernetes/test/integration/util"
	imageutils "k8s.io/kubernetes/test/utils/image"
)

// imported from testutils
var (
	createNode = testutils.CreateNode
)

const (
	filter               = "filter"
	prioritize           = "prioritize"
	bind                 = "bind"
	preempt              = "preempt"
	extendedResourceName = "foo.com/bar"
)

type fitPredicate func(pod *v1.Pod, node *v1.Node) (bool, error)
type priorityFunc func(pod *v1.Pod, nodes *v1.NodeList) (*extenderv1.HostPriorityList, error)
type preemptFunc func(args *extenderv1.ExtenderPreemptionArgs) (*extenderv1.ExtenderPreemptionResult, error)

type priorityConfig struct {
	function priorityFunc
	weight   int64
}

type Extender struct {
	name             string
	predicates       []fitPredicate
	prioritizers     []priorityConfig
	preemptor        preemptFunc
	nodeCacheCapable bool
	Client           clientset.Interface
}

func (e *Extender) serveHTTP(t *testing.T, w http.ResponseWriter, req *http.Request) {
	decoder := json.NewDecoder(req.Body)
	defer req.Body.Close()

	encoder := json.NewEncoder(w)

	if strings.Contains(req.URL.Path, filter) || strings.Contains(req.URL.Path, prioritize) {
		var args extenderv1.ExtenderArgs

		if err := decoder.Decode(&args); err != nil {
			http.Error(w, "Decode error", http.StatusBadRequest)
			return
		}

		if strings.Contains(req.URL.Path, filter) {
			resp, err := e.Filter(&args)
			if err != nil {
				resp.Error = err.Error()
			}

			if err := encoder.Encode(resp); err != nil {
				t.Fatalf("Failed to encode %v", resp)
			}
		} else if strings.Contains(req.URL.Path, prioritize) {
			// Prioritize errors are ignored. Default k8s priorities or another extender's
			// priorities may be applied.
			priorities, _ := e.Prioritize(&args)

			if err := encoder.Encode(priorities); err != nil {
				t.Fatalf("Failed to encode %+v", priorities)
			}
		}
	} else if strings.Contains(req.URL.Path, bind) {
		var args extenderv1.ExtenderBindingArgs

		if err := decoder.Decode(&args); err != nil {
			http.Error(w, "Decode error", http.StatusBadRequest)
			return
		}

		resp := &extenderv1.ExtenderBindingResult{}

		if err := e.Bind(&args); err != nil {
			resp.Error = err.Error()
		}

		if err := encoder.Encode(resp); err != nil {
			t.Fatalf("Failed to encode %+v", resp)
		}
	} else if strings.Contains(req.URL.Path, preempt) {
		var args extenderv1.ExtenderPreemptionArgs

		if err := decoder.Decode(&args); err != nil {
			http.Error(w, "Decode error", http.StatusBadRequest)
			return
		}

		if e.preemptor != nil {
			resp, err := e.preemptor(&args)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if err := encoder.Encode(resp); err != nil {
				t.Fatalf("Failed to encode %+v", resp)
			}
		} else {
			resp := &extenderv1.ExtenderPreemptionResult{
				NodeNameToMetaVictims: map[string]*extenderv1.MetaVictims{},
			}
			if args.NodeNameToVictims != nil {
				for node := range args.NodeNameToVictims {
					resp.NodeNameToMetaVictims[node] = &extenderv1.MetaVictims{Pods: []*extenderv1.MetaPod{}}
				}
			}
			if args.NodeNameToMetaVictims != nil {
				for node := range args.NodeNameToMetaVictims {
					resp.NodeNameToMetaVictims[node] = &extenderv1.MetaVictims{Pods: []*extenderv1.MetaPod{}}
				}
			}
			if err := encoder.Encode(resp); err != nil {
				t.Fatalf("Failed to encode %+v", resp)
			}
		}
	} else {
		http.Error(w, "Unknown method", http.StatusNotFound)
	}
}

func (e *Extender) filterUsingNodeCache(args *extenderv1.ExtenderArgs) (*extenderv1.ExtenderFilterResult, error) {
	nodeSlice := make([]string, 0)
	failedNodesMap := extenderv1.FailedNodesMap{}
	for _, nodeName := range *args.NodeNames {
		fits := true
		for _, predicate := range e.predicates {
			fit, err := predicate(args.Pod,
				&v1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}})
			if err != nil {
				return &extenderv1.ExtenderFilterResult{
					Nodes:       nil,
					NodeNames:   nil,
					FailedNodes: extenderv1.FailedNodesMap{},
					Error:       err.Error(),
				}, err
			}
			if !fit {
				fits = false
				break
			}
		}
		if fits {
			nodeSlice = append(nodeSlice, nodeName)
		} else {
			failedNodesMap[nodeName] = fmt.Sprintf("extender failed: %s", e.name)
		}
	}

	return &extenderv1.ExtenderFilterResult{
		Nodes:       nil,
		NodeNames:   &nodeSlice,
		FailedNodes: failedNodesMap,
	}, nil
}

func (e *Extender) Filter(args *extenderv1.ExtenderArgs) (*extenderv1.ExtenderFilterResult, error) {
	filtered := []v1.Node{}
	failedNodesMap := extenderv1.FailedNodesMap{}

	if e.nodeCacheCapable {
		return e.filterUsingNodeCache(args)
	}

	for _, node := range args.Nodes.Items {
		fits := true
		for _, predicate := range e.predicates {
			fit, err := predicate(args.Pod, &node)
			if err != nil {
				return &extenderv1.ExtenderFilterResult{
					Nodes:       &v1.NodeList{},
					NodeNames:   nil,
					FailedNodes: extenderv1.FailedNodesMap{},
					Error:       err.Error(),
				}, err
			}
			if !fit {
				fits = false
				break
			}
		}
		if fits {
			filtered = append(filtered, node)
		} else {
			failedNodesMap[node.Name] = fmt.Sprintf("extender failed: %s", e.name)
		}
	}

	return &extenderv1.ExtenderFilterResult{
		Nodes:       &v1.NodeList{Items: filtered},
		NodeNames:   nil,
		FailedNodes: failedNodesMap,
	}, nil
}

func (e *Extender) Prioritize(args *extenderv1.ExtenderArgs) (*extenderv1.HostPriorityList, error) {
	result := extenderv1.HostPriorityList{}
	combinedScores := map[string]int64{}
	var nodes = &v1.NodeList{Items: []v1.Node{}}

	if e.nodeCacheCapable {
		for _, nodeName := range *args.NodeNames {
			nodes.Items = append(nodes.Items, v1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}})
		}
	} else {
		nodes = args.Nodes
	}

	for _, prioritizer := range e.prioritizers {
		weight := prioritizer.weight
		if weight == 0 {
			continue
		}
		priorityFunc := prioritizer.function
		prioritizedList, err := priorityFunc(args.Pod, nodes)
		if err != nil {
			return &extenderv1.HostPriorityList{}, err
		}
		for _, hostEntry := range *prioritizedList {
			combinedScores[hostEntry.Host] += hostEntry.Score * weight
		}
	}
	for host, score := range combinedScores {
		result = append(result, extenderv1.HostPriority{Host: host, Score: score})
	}
	return &result, nil
}

func (e *Extender) Bind(binding *extenderv1.ExtenderBindingArgs) error {
	b := &v1.Binding{
		ObjectMeta: metav1.ObjectMeta{Namespace: binding.PodNamespace, Name: binding.PodName, UID: binding.PodUID},
		Target: v1.ObjectReference{
			Kind: "Node",
			Name: binding.Node,
		},
	}

	return e.Client.CoreV1().Pods(b.Namespace).Bind(context.TODO(), b, metav1.CreateOptions{})
}

func machine1_2_3Predicate(pod *v1.Pod, node *v1.Node) (bool, error) {
	if node.Name == "machine1" || node.Name == "machine2" || node.Name == "machine3" {
		return true, nil
	}
	return false, nil
}

func machine2_3_5Predicate(pod *v1.Pod, node *v1.Node) (bool, error) {
	if node.Name == "machine2" || node.Name == "machine3" || node.Name == "machine5" {
		return true, nil
	}
	return false, nil
}

func machine2Prioritizer(pod *v1.Pod, nodes *v1.NodeList) (*extenderv1.HostPriorityList, error) {
	result := extenderv1.HostPriorityList{}
	for _, node := range nodes.Items {
		score := 1
		if node.Name == "machine2" {
			score = 10
		}
		result = append(result, extenderv1.HostPriority{
			Host:  node.Name,
			Score: int64(score),
		})
	}
	return &result, nil
}

func machine3Prioritizer(pod *v1.Pod, nodes *v1.NodeList) (*extenderv1.HostPriorityList, error) {
	result := extenderv1.HostPriorityList{}
	for _, node := range nodes.Items {
		score := 1
		if node.Name == "machine3" {
			score = 10
		}
		result = append(result, extenderv1.HostPriority{
			Host:  node.Name,
			Score: int64(score),
		})
	}
	return &result, nil
}

func createTestNodeWithResources(cs clientset.Interface, name string, cpuMillis int64, memoryMB int64) (*v1.Node, error) {
	node := st.MakeNode().Name(name).
		Capacity(map[v1.ResourceName]string{
			v1.ResourcePods:   "32",
			v1.ResourceCPU:    fmt.Sprintf("%dm", cpuMillis),
			v1.ResourceMemory: fmt.Sprintf("%dMi", memoryMB),
		}).
		Obj()
	return testutils.CreateNode(cs, node)
}

func makeTestPodWithResources(ns, name, nodeName string, priority int32, reqMap map[v1.ResourceName]string) *v1.Pod {
	pw := st.MakePod().Namespace(ns).Name(name).
		Priority(priority).
		Res(reqMap).
		ZeroTerminationGracePeriod()
	if nodeName != "" {
		pw.Node(nodeName)
	}
	return pw.Obj()
}

func simulateVictimDeletion(ctx context.Context, cs clientset.Interface, ns, name string) error {
	err := wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 10*time.Second, false, func(ctx context.Context) (bool, error) {
		pod, err := cs.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		return pod.DeletionTimestamp != nil, nil
	})
	if err != nil {
		return fmt.Errorf("timed out waiting for victim pod %s/%s eviction: %w", ns, name, err)
	}
	var zero int64
	err = cs.CoreV1().Pods(ns).Delete(ctx, name, metav1.DeleteOptions{GracePeriodSeconds: &zero})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func TestSchedulerExtender(t *testing.T) {
	testCtx := testutils.InitTestAPIServer(t, "scheduler-extender", nil)
	clientSet := testCtx.ClientSet

	extender1 := &Extender{
		name:         "extender1",
		predicates:   []fitPredicate{machine1_2_3Predicate},
		prioritizers: []priorityConfig{{machine2Prioritizer, 1}},
	}
	es1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		extender1.serveHTTP(t, w, req)
	}))
	defer es1.Close()

	extender2 := &Extender{
		name:         "extender2",
		predicates:   []fitPredicate{machine2_3_5Predicate},
		prioritizers: []priorityConfig{{machine3Prioritizer, 1}},
		Client:       clientSet,
	}
	es2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		extender2.serveHTTP(t, w, req)
	}))
	defer es2.Close()

	extender3 := &Extender{
		name:             "extender3",
		predicates:       []fitPredicate{machine1_2_3Predicate},
		prioritizers:     []priorityConfig{{machine2Prioritizer, 5}},
		nodeCacheCapable: true,
	}
	es3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		extender3.serveHTTP(t, w, req)
	}))
	defer es3.Close()

	extenders := []schedulerapi.Extender{
		{
			URLPrefix:      es1.URL,
			FilterVerb:     filter,
			PrioritizeVerb: prioritize,
			Weight:         3,
			EnableHTTPS:    false,
		},
		{
			URLPrefix:      es2.URL,
			FilterVerb:     filter,
			PrioritizeVerb: prioritize,
			BindVerb:       bind,
			Weight:         4,
			EnableHTTPS:    false,
			ManagedResources: []schedulerapi.ExtenderManagedResource{
				{
					Name:               extendedResourceName,
					IgnoredByScheduler: true,
				},
			},
		},
		{
			URLPrefix:        es3.URL,
			FilterVerb:       filter,
			PrioritizeVerb:   prioritize,
			Weight:           10,
			EnableHTTPS:      false,
			NodeCacheCapable: true,
		},
	}

	testCtx = testutils.InitTestSchedulerWithOptions(t, testCtx, 0, scheduler.WithExtenders(extenders...))
	testutils.SyncSchedulerInformerFactory(testCtx)
	go testCtx.Scheduler.Run(testCtx.Ctx)

	DoTestPodScheduling(testCtx.NS, t, clientSet)
}

func DoTestPodScheduling(ns *v1.Namespace, t *testing.T, cs clientset.Interface) {
	// NOTE: This test cannot run in parallel, because it is creating and deleting
	// non-namespaced objects (Nodes).
	defer cs.CoreV1().Nodes().DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{})

	goodCondition := v1.NodeCondition{
		Type:              v1.NodeReady,
		Status:            v1.ConditionTrue,
		Reason:            "schedulable condition",
		LastHeartbeatTime: metav1.Time{Time: time.Now()},
	}
	node := &v1.Node{
		Spec: v1.NodeSpec{Unschedulable: false},
		Status: v1.NodeStatus{
			Capacity: v1.ResourceList{
				v1.ResourcePods: *resource.NewQuantity(32, resource.DecimalSI),
			},
			Conditions: []v1.NodeCondition{goodCondition},
		},
	}

	for ii := range 5 {
		node.Name = fmt.Sprintf("machine%d", ii+1)
		if _, err := createNode(cs, node); err != nil {
			t.Fatalf("Failed to create nodes: %v", err)
		}
	}

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "extender-test-pod"},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "container",
					Image: imageutils.GetPauseImageName(),
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							extendedResourceName: *resource.NewQuantity(1, resource.DecimalSI),
						},
					},
				},
			},
		},
	}

	myPod, err := cs.CoreV1().Pods(ns.Name).Create(context.TODO(), pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create pod: %v", err)
	}

	err = wait.PollUntilContextTimeout(context.TODO(), time.Second, wait.ForeverTestTimeout, false,
		testutils.PodScheduled(cs, myPod.Namespace, myPod.Name))
	if err != nil {
		t.Fatalf("Failed to schedule pod: %v", err)
	}

	myPod, err = cs.CoreV1().Pods(ns.Name).Get(context.TODO(), myPod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get pod: %v", err)
	} else if myPod.Spec.NodeName != "machine2" {
		t.Fatalf("Failed to schedule using extender, expected machine2, got %v", myPod.Spec.NodeName)
	}
	var gracePeriod int64
	if err := cs.CoreV1().Pods(ns.Name).Delete(context.TODO(), myPod.Name, metav1.DeleteOptions{GracePeriodSeconds: &gracePeriod}); err != nil {
		t.Fatalf("Failed to delete pod: %v", err)
	}
	_, err = cs.CoreV1().Pods(ns.Name).Get(context.TODO(), myPod.Name, metav1.GetOptions{})
	if err == nil {
		t.Fatalf("Failed to delete pod: %v", err)
	}
	t.Logf("Scheduled pod using extenders")
}

// TestExtenderPreemption_EmptyInTreeVictimsPlaceholderChain tests extender preemption with placeholder
// candidate nodes across a multi-extender chain (KEP-562 / KEP-3838).
// When in-tree filter plugins fit without evicting in-tree pods (0 in-tree victims), but external extenders
// fail the node due to custom resources, the scheduler creates an empty placeholder candidate (&extenderv1.Victims{Pods: []})
// and passes it down the extender chain. An upstream extender can leave the placeholder unchanged, and a downstream
// extender can add victims to allow preemptor scheduling.
func TestExtenderPreemption_EmptyInTreeVictimsPlaceholderChain(t *testing.T) {
	const (
		lowPriority  int32 = 100
		highPriority int32 = 1000
		customGPU          = "example.com/gpu"
		customLicense      = "example.com/license"
	)

	t.Run("ChainedPassthroughAndPreempt", func(t *testing.T) {
		testCtx := testutils.InitTestAPIServer(t, "ext-placeholder-chain", nil)
		cs := testCtx.ClientSet
		ns := testCtx.NS.Name

		nodeName := "node-placeholder-chain"
		if _, err := createTestNodeWithResources(cs, nodeName, 8000, 8192); err != nil {
			t.Fatalf("Failed to create node: %v", err)
		}

		// Low-priority victim pod running on node, consuming 1 license and minor in-tree resources.
		victimPod := makeTestPodWithResources(ns, "victim-license-pod", nodeName, lowPriority, map[v1.ResourceName]string{
			v1.ResourceCPU:    "100m",
			v1.ResourceMemory: "100Mi",
			customLicense:     "1",
		})
		victimPod, err := testutils.RunPausePod(cs, victimPod)
		if err != nil {
			t.Fatalf("Failed to run victim pod: %v", err)
		}

		var extenderAObservedPlaceholder atomic.Bool
		var extenderBObservedPlaceholder atomic.Bool
		var extenderAPreemptCalls atomic.Int32
		var extenderBPreemptCalls atomic.Int32

		// Extender A: Manages customGPU. Preemptor requests GPU (which is available), so Extender A filter passes.
		// In preemption, Extender A receives the empty placeholder candidate and preserves it (returns empty victims).
		extenderA := &Extender{
			name: "extender-gpu",
			predicates: []fitPredicate{
				func(pod *v1.Pod, node *v1.Node) (bool, error) {
					return true, nil
				},
			},
			preemptor: func(args *extenderv1.ExtenderPreemptionArgs) (*extenderv1.ExtenderPreemptionResult, error) {
				extenderAPreemptCalls.Add(1)
				if args.NodeNameToVictims != nil && args.NodeNameToVictims[nodeName] != nil {
					if len(args.NodeNameToVictims[nodeName].Pods) == 0 {
						extenderAObservedPlaceholder.Store(true)
					}
				}
				// Return empty victims map entry to preserve placeholder for downstream extenders
				return &extenderv1.ExtenderPreemptionResult{
					NodeNameToMetaVictims: map[string]*extenderv1.MetaVictims{
						nodeName: {Pods: []*extenderv1.MetaPod{}},
					},
				}, nil
			},
		}
		esA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			extenderA.serveHTTP(t, w, req)
		}))
		defer esA.Close()

		// Extender B: Manages customLicense. Filters out node when victimPod is active.
		// In preemption, Extender B receives placeholder candidate passed through by Extender A,
		// and nominates victimPod.
		extenderB := &Extender{
			name: "extender-license",
			predicates: []fitPredicate{
				func(pod *v1.Pod, node *v1.Node) (bool, error) {
					_, err := cs.CoreV1().Pods(ns).Get(context.Background(), victimPod.Name, metav1.GetOptions{})
					if err == nil {
						// Victim holding license, fail filter
						return false, nil
					}
					// Victim deleted, pass filter
					return true, nil
				},
			},
			preemptor: func(args *extenderv1.ExtenderPreemptionArgs) (*extenderv1.ExtenderPreemptionResult, error) {
				extenderBPreemptCalls.Add(1)
				if args.NodeNameToVictims != nil && args.NodeNameToVictims[nodeName] != nil {
					if len(args.NodeNameToVictims[nodeName].Pods) == 0 {
						extenderBObservedPlaceholder.Store(true)
					}
				}
				return &extenderv1.ExtenderPreemptionResult{
					NodeNameToMetaVictims: map[string]*extenderv1.MetaVictims{
						nodeName: {
							Pods: []*extenderv1.MetaPod{{UID: string(victimPod.UID)}},
						},
					},
				}, nil
			},
		}
		esB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			extenderB.serveHTTP(t, w, req)
		}))
		defer esB.Close()

		extenders := []schedulerapi.Extender{
			{
				URLPrefix:   esA.URL,
				FilterVerb:  filter,
				PreemptVerb: preempt,
				EnableHTTPS: false,
				ManagedResources: []schedulerapi.ExtenderManagedResource{
					{Name: customGPU, IgnoredByScheduler: true},
				},
				Ignorable: false,
			},
			{
				URLPrefix:   esB.URL,
				FilterVerb:  filter,
				PreemptVerb: preempt,
				EnableHTTPS: false,
				ManagedResources: []schedulerapi.ExtenderManagedResource{
					{Name: customLicense, IgnoredByScheduler: true},
				},
				Ignorable: false,
			},
		}

		testCtx = testutils.InitTestSchedulerWithOptions(t, testCtx, 0, scheduler.WithExtenders(extenders...))
		testutils.SyncSchedulerInformerFactory(testCtx)
		go testCtx.Scheduler.Run(testCtx.Ctx)

		// Create high-priority preemptor requesting both GPU and license
		preemptorPod := makeTestPodWithResources(ns, "preemptor-license-pod", "", highPriority, map[v1.ResourceName]string{
			v1.ResourceCPU:    "100m",
			v1.ResourceMemory: "100Mi",
			customGPU:         "1",
			customLicense:     "1",
		})
		preemptorPod, err = testutils.CreatePausePod(cs, preemptorPod)
		if err != nil {
			t.Fatalf("Failed to create preemptor pod: %v", err)
		}

		// Simulate victim eviction and deletion
		if err := simulateVictimDeletion(testCtx.Ctx, cs, ns, victimPod.Name); err != nil {
			t.Fatalf("Error simulating victim deletion: %v", err)
		}

		// Verify preemptor pod is scheduled to nodeName
		err = wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, 10*time.Second, false,
			testutils.PodScheduled(cs, ns, preemptorPod.Name))
		if err != nil {
			t.Fatalf("Preemptor pod failed to schedule: %v", err)
		}

		scheduledPod, err := cs.CoreV1().Pods(ns).Get(testCtx.Ctx, preemptorPod.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Failed to get scheduled preemptor pod: %v", err)
		}
		if scheduledPod.Spec.NodeName != nodeName {
			t.Fatalf("Expected preemptor to be scheduled on %s, got %s", nodeName, scheduledPod.Spec.NodeName)
		}

		if !extenderAObservedPlaceholder.Load() {
			t.Fatalf("Expected Extender A to receive placeholder empty-victim candidate")
		}
		if !extenderBObservedPlaceholder.Load() {
			t.Fatalf("Expected Extender B to receive placeholder empty-victim candidate preserved across chain")
		}
		if extenderAPreemptCalls.Load() == 0 || extenderBPreemptCalls.Load() == 0 {
			t.Fatalf("Expected both extenders to have their preempt handlers invoked")
		}
	})

	t.Run("ChainedMultiExtenderMultipleVictims", func(t *testing.T) {
		testCtx := testutils.InitTestAPIServer(t, "ext-multi-victims-chain", nil)
		cs := testCtx.ClientSet
		ns := testCtx.NS.Name

		nodeName := "node-multi-victims-chain"
		if _, err := createTestNodeWithResources(cs, nodeName, 8000, 8192); err != nil {
			t.Fatalf("Failed to create node: %v", err)
		}

		// Victim pod 1 holds custom GPU
		victimGPU := makeTestPodWithResources(ns, "victim-gpu", nodeName, lowPriority, map[v1.ResourceName]string{
			v1.ResourceCPU:    "100m",
			v1.ResourceMemory: "100Mi",
			customGPU:         "1",
		})
		victimGPU, err := testutils.RunPausePod(cs, victimGPU)
		if err != nil {
			t.Fatalf("Failed to run victim GPU pod: %v", err)
		}

		// Victim pod 2 holds custom License
		victimLicense := makeTestPodWithResources(ns, "victim-license", nodeName, lowPriority, map[v1.ResourceName]string{
			v1.ResourceCPU:    "100m",
			v1.ResourceMemory: "100Mi",
			customLicense:     "1",
		})
		victimLicense, err = testutils.RunPausePod(cs, victimLicense)
		if err != nil {
			t.Fatalf("Failed to run victim license pod: %v", err)
		}

		// Extender A: Manages customGPU. Filters out node when victimGPU exists.
		// In preemption, Extender A adds victimGPU to the placeholder candidate.
		extenderA := &Extender{
			name: "extender-gpu",
			predicates: []fitPredicate{
				func(pod *v1.Pod, node *v1.Node) (bool, error) {
					_, err := cs.CoreV1().Pods(ns).Get(context.Background(), victimGPU.Name, metav1.GetOptions{})
					return err != nil, nil
				},
			},
			preemptor: func(args *extenderv1.ExtenderPreemptionArgs) (*extenderv1.ExtenderPreemptionResult, error) {
				return &extenderv1.ExtenderPreemptionResult{
					NodeNameToMetaVictims: map[string]*extenderv1.MetaVictims{
						nodeName: {
							Pods: []*extenderv1.MetaPod{{UID: string(victimGPU.UID)}},
						},
					},
				}, nil
			},
		}
		esA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			extenderA.serveHTTP(t, w, req)
		}))
		defer esA.Close()

		var extenderBObservedVictimGPU atomic.Bool

		// Extender B: Manages customLicense. Filters out node when victimLicense exists.
		// In preemption, Extender B receives victimGPU from Extender A and appends victimLicense.
		extenderB := &Extender{
			name: "extender-license",
			predicates: []fitPredicate{
				func(pod *v1.Pod, node *v1.Node) (bool, error) {
					_, err := cs.CoreV1().Pods(ns).Get(context.Background(), victimLicense.Name, metav1.GetOptions{})
					return err != nil, nil
				},
			},
			preemptor: func(args *extenderv1.ExtenderPreemptionArgs) (*extenderv1.ExtenderPreemptionResult, error) {
				if args.NodeNameToVictims != nil && args.NodeNameToVictims[nodeName] != nil {
					for _, p := range args.NodeNameToVictims[nodeName].Pods {
						if p.UID == victimGPU.UID {
							extenderBObservedVictimGPU.Store(true)
						}
					}
				}
				return &extenderv1.ExtenderPreemptionResult{
					NodeNameToMetaVictims: map[string]*extenderv1.MetaVictims{
						nodeName: {
							Pods: []*extenderv1.MetaPod{
								{UID: string(victimGPU.UID)},
								{UID: string(victimLicense.UID)},
							},
						},
					},
				}, nil
			},
		}
		esB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			extenderB.serveHTTP(t, w, req)
		}))
		defer esB.Close()

		extenders := []schedulerapi.Extender{
			{
				URLPrefix:   esA.URL,
				FilterVerb:  filter,
				PreemptVerb: preempt,
				EnableHTTPS: false,
				ManagedResources: []schedulerapi.ExtenderManagedResource{
					{Name: customGPU, IgnoredByScheduler: true},
				},
				Ignorable: false,
			},
			{
				URLPrefix:   esB.URL,
				FilterVerb:  filter,
				PreemptVerb: preempt,
				EnableHTTPS: false,
				ManagedResources: []schedulerapi.ExtenderManagedResource{
					{Name: customLicense, IgnoredByScheduler: true},
				},
				Ignorable: false,
			},
		}

		testCtx = testutils.InitTestSchedulerWithOptions(t, testCtx, 0, scheduler.WithExtenders(extenders...))
		testutils.SyncSchedulerInformerFactory(testCtx)
		go testCtx.Scheduler.Run(testCtx.Ctx)

		// Create high-priority preemptor requesting both GPU and License
		preemptorPod := makeTestPodWithResources(ns, "preemptor-dual", "", highPriority, map[v1.ResourceName]string{
			v1.ResourceCPU:    "100m",
			v1.ResourceMemory: "100Mi",
			customGPU:         "1",
			customLicense:     "1",
		})
		preemptorPod, err = testutils.CreatePausePod(cs, preemptorPod)
		if err != nil {
			t.Fatalf("Failed to create preemptor pod: %v", err)
		}

		// Evict and delete both victims
		if err := simulateVictimDeletion(testCtx.Ctx, cs, ns, victimGPU.Name); err != nil {
			t.Fatalf("Error simulating victim GPU deletion: %v", err)
		}
		if err := simulateVictimDeletion(testCtx.Ctx, cs, ns, victimLicense.Name); err != nil {
			t.Fatalf("Error simulating victim license deletion: %v", err)
		}

		err = wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, 10*time.Second, false,
			testutils.PodScheduled(cs, ns, preemptorPod.Name))
		if err != nil {
			t.Fatalf("Preemptor pod failed to schedule: %v", err)
		}

		scheduledPod, err := cs.CoreV1().Pods(ns).Get(testCtx.Ctx, preemptorPod.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Failed to get scheduled preemptor pod: %v", err)
		}
		if scheduledPod.Spec.NodeName != nodeName {
			t.Fatalf("Expected preemptor to be scheduled on %s, got %s", nodeName, scheduledPod.Spec.NodeName)
		}
		if !extenderBObservedVictimGPU.Load() {
			t.Fatalf("Expected Extender B to observe victim GPU nominated by Extender A")
		}
	})

	t.Run("OmittedPlaceholderDroppedAcrossChain", func(t *testing.T) {
		testCtx := testutils.InitTestAPIServer(t, "ext-omitted-chain", nil)
		cs := testCtx.ClientSet
		ns := testCtx.NS.Name

		nodeName := "node-omitted-chain"
		if _, err := createTestNodeWithResources(cs, nodeName, 8000, 8192); err != nil {
			t.Fatalf("Failed to create node: %v", err)
		}

		victimPod := makeTestPodWithResources(ns, "victim-license-pod", nodeName, lowPriority, map[v1.ResourceName]string{
			v1.ResourceCPU:    "100m",
			v1.ResourceMemory: "100Mi",
			customLicense:     "1",
		})
		victimPod, err := testutils.RunPausePod(cs, victimPod)
		if err != nil {
			t.Fatalf("Failed to run victim pod: %v", err)
		}

		var extenderBCalled atomic.Bool

		// Extender A: Omits the candidate node (returns empty map).
		extenderA := &Extender{
			name: "extender-gpu",
			predicates: []fitPredicate{
				func(pod *v1.Pod, node *v1.Node) (bool, error) {
					return true, nil
				},
			},
			preemptor: func(args *extenderv1.ExtenderPreemptionArgs) (*extenderv1.ExtenderPreemptionResult, error) {
				// Reject node by omitting it from result map
				return &extenderv1.ExtenderPreemptionResult{
					NodeNameToMetaVictims: map[string]*extenderv1.MetaVictims{},
				}, nil
			},
		}
		esA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			extenderA.serveHTTP(t, w, req)
		}))
		defer esA.Close()

		// Extender B should never be called for preemption because Extender A dropped the node.
		extenderB := &Extender{
			name: "extender-license",
			predicates: []fitPredicate{
				func(pod *v1.Pod, node *v1.Node) (bool, error) {
					return false, nil
				},
			},
			preemptor: func(args *extenderv1.ExtenderPreemptionArgs) (*extenderv1.ExtenderPreemptionResult, error) {
				extenderBCalled.Store(true)
				return &extenderv1.ExtenderPreemptionResult{
					NodeNameToMetaVictims: map[string]*extenderv1.MetaVictims{
						nodeName: {
							Pods: []*extenderv1.MetaPod{{UID: string(victimPod.UID)}},
						},
					},
				}, nil
			},
		}
		esB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			extenderB.serveHTTP(t, w, req)
		}))
		defer esB.Close()

		extenders := []schedulerapi.Extender{
			{
				URLPrefix:   esA.URL,
				FilterVerb:  filter,
				PreemptVerb: preempt,
				EnableHTTPS: false,
				ManagedResources: []schedulerapi.ExtenderManagedResource{
					{Name: customGPU, IgnoredByScheduler: true},
				},
				Ignorable: false,
			},
			{
				URLPrefix:   esB.URL,
				FilterVerb:  filter,
				PreemptVerb: preempt,
				EnableHTTPS: false,
				ManagedResources: []schedulerapi.ExtenderManagedResource{
					{Name: customLicense, IgnoredByScheduler: true},
				},
				Ignorable: false,
			},
		}

		testCtx = testutils.InitTestSchedulerWithOptions(t, testCtx, 0, scheduler.WithExtenders(extenders...))
		testutils.SyncSchedulerInformerFactory(testCtx)
		go testCtx.Scheduler.Run(testCtx.Ctx)

		preemptorPod := makeTestPodWithResources(ns, "preemptor-fail", "", highPriority, map[v1.ResourceName]string{
			v1.ResourceCPU:    "100m",
			v1.ResourceMemory: "100Mi",
			customGPU:         "1",
			customLicense:     "1",
		})
		preemptorPod, err = testutils.CreatePausePod(cs, preemptorPod)
		if err != nil {
			t.Fatalf("Failed to create preemptor pod: %v", err)
		}

		time.Sleep(1 * time.Second)

		currentVictim, err := cs.CoreV1().Pods(ns).Get(testCtx.Ctx, victimPod.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Failed to get victim pod: %v", err)
		}
		if currentVictim.DeletionTimestamp != nil {
			t.Fatalf("Victim pod was unexpectedly evicted")
		}

		currentPreemptor, err := cs.CoreV1().Pods(ns).Get(testCtx.Ctx, preemptorPod.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Failed to get preemptor pod: %v", err)
		}
		if currentPreemptor.Spec.NodeName != "" {
			t.Fatalf("Preemptor was unexpectedly scheduled to %s", currentPreemptor.Spec.NodeName)
		}
		if extenderBCalled.Load() {
			t.Fatalf("Extender B should not be called when upstream extender omitted the node")
		}
	})

	t.Run("NodeCacheCapableChainedPreemption", func(t *testing.T) {
		testCtx := testutils.InitTestAPIServer(t, "ext-cache-chain", nil)
		cs := testCtx.ClientSet
		ns := testCtx.NS.Name

		nodeName := "node-cache-chain"
		if _, err := createTestNodeWithResources(cs, nodeName, 8000, 8192); err != nil {
			t.Fatalf("Failed to create node: %v", err)
		}

		victimPod := makeTestPodWithResources(ns, "victim-license-pod", nodeName, lowPriority, map[v1.ResourceName]string{
			v1.ResourceCPU:    "100m",
			v1.ResourceMemory: "100Mi",
			customLicense:     "1",
		})
		victimPod, err := testutils.RunPausePod(cs, victimPod)
		if err != nil {
			t.Fatalf("Failed to run victim pod: %v", err)
		}

		var extenderAObservedMetaPlaceholder atomic.Bool

		// Extender A: NodeCacheCapable = true. Receives NodeNameToMetaVictims.
		extenderA := &Extender{
			name:             "extender-gpu-cache",
			nodeCacheCapable: true,
			predicates: []fitPredicate{
				func(pod *v1.Pod, node *v1.Node) (bool, error) {
					return true, nil
				},
			},
			preemptor: func(args *extenderv1.ExtenderPreemptionArgs) (*extenderv1.ExtenderPreemptionResult, error) {
				if args.NodeNameToMetaVictims != nil && args.NodeNameToMetaVictims[nodeName] != nil {
					if len(args.NodeNameToMetaVictims[nodeName].Pods) == 0 {
						extenderAObservedMetaPlaceholder.Store(true)
					}
				}
				return &extenderv1.ExtenderPreemptionResult{
					NodeNameToMetaVictims: map[string]*extenderv1.MetaVictims{
						nodeName: {Pods: []*extenderv1.MetaPod{}},
					},
				}, nil
			},
		}
		esA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			extenderA.serveHTTP(t, w, req)
		}))
		defer esA.Close()

		// Extender B: NodeCacheCapable = false. Receives NodeNameToVictims.
		extenderB := &Extender{
			name:             "extender-license-nocache",
			nodeCacheCapable: false,
			predicates: []fitPredicate{
				func(pod *v1.Pod, node *v1.Node) (bool, error) {
					_, err := cs.CoreV1().Pods(ns).Get(context.Background(), victimPod.Name, metav1.GetOptions{})
					return err != nil, nil
				},
			},
			preemptor: func(args *extenderv1.ExtenderPreemptionArgs) (*extenderv1.ExtenderPreemptionResult, error) {
				return &extenderv1.ExtenderPreemptionResult{
					NodeNameToMetaVictims: map[string]*extenderv1.MetaVictims{
						nodeName: {
							Pods: []*extenderv1.MetaPod{{UID: string(victimPod.UID)}},
						},
					},
				}, nil
			},
		}
		esB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			extenderB.serveHTTP(t, w, req)
		}))
		defer esB.Close()

		extenders := []schedulerapi.Extender{
			{
				URLPrefix:        esA.URL,
				FilterVerb:       filter,
				PreemptVerb:      preempt,
				EnableHTTPS:      false,
				NodeCacheCapable: true,
				ManagedResources: []schedulerapi.ExtenderManagedResource{
					{Name: customGPU, IgnoredByScheduler: true},
				},
				Ignorable: false,
			},
			{
				URLPrefix:        esB.URL,
				FilterVerb:       filter,
				PreemptVerb:      preempt,
				EnableHTTPS:      false,
				NodeCacheCapable: false,
				ManagedResources: []schedulerapi.ExtenderManagedResource{
					{Name: customLicense, IgnoredByScheduler: true},
				},
				Ignorable: false,
			},
		}

		testCtx = testutils.InitTestSchedulerWithOptions(t, testCtx, 0, scheduler.WithExtenders(extenders...))
		testutils.SyncSchedulerInformerFactory(testCtx)
		go testCtx.Scheduler.Run(testCtx.Ctx)

		preemptorPod := makeTestPodWithResources(ns, "preemptor-cache-chain", "", highPriority, map[v1.ResourceName]string{
			v1.ResourceCPU:    "100m",
			v1.ResourceMemory: "100Mi",
			customGPU:         "1",
			customLicense:     "1",
		})
		preemptorPod, err = testutils.CreatePausePod(cs, preemptorPod)
		if err != nil {
			t.Fatalf("Failed to create preemptor pod: %v", err)
		}

		if err := simulateVictimDeletion(testCtx.Ctx, cs, ns, victimPod.Name); err != nil {
			t.Fatalf("Error simulating victim deletion: %v", err)
		}

		err = wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, 10*time.Second, false,
			testutils.PodScheduled(cs, ns, preemptorPod.Name))
		if err != nil {
			t.Fatalf("Preemptor pod failed to schedule: %v", err)
		}

		scheduledPod, err := cs.CoreV1().Pods(ns).Get(testCtx.Ctx, preemptorPod.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Failed to get scheduled preemptor pod: %v", err)
		}
		if scheduledPod.Spec.NodeName != nodeName {
			t.Fatalf("Expected preemptor to be scheduled on %s, got %s", nodeName, scheduledPod.Spec.NodeName)
		}
		if !extenderAObservedMetaPlaceholder.Load() {
			t.Fatalf("Expected node-cache capable Extender A to observe empty meta placeholder")
		}
	})
}
