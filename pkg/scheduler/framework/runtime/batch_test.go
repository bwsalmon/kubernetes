/*
Copyright 2025 The Kubernetes Authors.

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

package runtime

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2/ktesting"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// NodeInfoLister declares a fwk.NodeInfo type for testing.
type NodeInfoLister []fwk.NodeInfo

// Get returns a fake node object in the fake nodes.
func (nodes NodeInfoLister) Get(nodeName string) (fwk.NodeInfo, error) {
	for _, node := range nodes {
		if node != nil && node.Node().Name == nodeName {
			return node, nil
		}
	}
	return nil, fmt.Errorf("unable to find node: %s", nodeName)
}

// List lists all nodes.
func (nodes NodeInfoLister) List() ([]fwk.NodeInfo, error) {
	return nodes, nil
}

// HavePodsWithAffinityList is supposed to list nodes with at least one pod with affinity. For the fake lister
// we just return everything.
func (nodes NodeInfoLister) HavePodsWithAffinityList() ([]fwk.NodeInfo, error) {
	return nodes, nil
}

// HavePodsWithRequiredAntiAffinityList is supposed to list nodes with at least one pod with
// required anti-affinity. For the fake lister we just return everything.
func (nodes NodeInfoLister) HavePodsWithRequiredAntiAffinityList() ([]fwk.NodeInfo, error) {
	return nodes, nil
}

var batchRegistry = func() Registry {
	r := make(Registry)
	err := r.Register("batchTest", newBatchTestPlugin)
	if err != nil {
		log.Fatal("Couldn't register test.")
	}
	err = r.Register(queueSortPlugin, newQueueSortPlugin)
	if err != nil {
		log.Fatal("Couldn't register test.")
	}
	err = r.Register(bindPlugin, newBindPlugin)
	if err != nil {
		log.Fatal("Couldn't register test.")
	}
	return r
}()

type BatchTestPlugin struct{}

func (pl *BatchTestPlugin) Name() string {
	return "batchTest"
}

// Test plugin assumes that each node can hold only one node whose id begins with "b". This allows
// us to construct pods that block future selves or not.
func (pl *BatchTestPlugin) Filter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	podID := pod.GetUID()
	for _, nodePod := range nodeInfo.GetPods() {
		npid := nodePod.GetPod().GetUID()
		if podID[0] == 'b' && npid[0] == 'b' {
			return fwk.NewStatus(fwk.Unschedulable, "unsched")
		}
	}
	return fwk.NewStatus(fwk.Success, "success")
}

func newBatchTestPlugin(_ context.Context, injArgs runtime.Object, f fwk.Handle) (fwk.Plugin, error) {
	return &BatchTestPlugin{}, nil
}

func newBatchTestFramework(ctx context.Context, r Registry) (framework.Framework, error) {
	plugins := &config.Plugins{}
	profile := config.KubeSchedulerProfile{Plugins: plugins}

	if _, ok := r[queueSortPlugin]; !ok {
		r[queueSortPlugin] = newQueueSortPlugin
	}
	if _, ok := r[bindPlugin]; !ok {
		r[bindPlugin] = newBindPlugin
	}

	if len(profile.Plugins.QueueSort.Enabled) == 0 {
		profile.Plugins.QueueSort.Enabled = append(profile.Plugins.QueueSort.Enabled, config.Plugin{Name: queueSortPlugin})
	}
	if len(profile.Plugins.Bind.Enabled) == 0 {
		profile.Plugins.Bind.Enabled = append(profile.Plugins.Bind.Enabled, config.Plugin{Name: bindPlugin})
	}
	profile.Plugins.Filter.Enabled = []config.Plugin{{Name: "batchTest"}}
	return NewFramework(ctx, r, &profile)
}

type testSortedScoredNodes struct {
	Nodes []string
}

var _ framework.SortedScoredNodes = &testSortedScoredNodes{}

func (t *testSortedScoredNodes) Pop() string {
	ret := t.Nodes[0]
	t.Nodes = t.Nodes[1:]
	return ret
}

func (t *testSortedScoredNodes) Len() int {
	return len(t.Nodes)
}

func newTestNodes(n []string) *testSortedScoredNodes {
	return &testSortedScoredNodes{Nodes: n}
}

func newTestSigFunc(sig *string) PodSignatureFunc {
	return func(p *v1.Pod) string {
		return *sig
	}
}

func TestBatchBasic(t *testing.T) {
	tests := []struct {
		name              string
		firstID           string
		firstSig          string
		firstChosenNode   string
		firstOtherNodes   framework.SortedScoredNodes
		firstPodCompleted bool
		skipPod           bool
		secondID          string
		secondSig         string
		secondChosenNode  string
		secondOtherNodes  framework.SortedScoredNodes
		expectedHint      string
		expectedState     *batchState
	}{
		{
			name:              "single match",
			firstID:           "b1",
			firstSig:          "sig",
			firstChosenNode:   "n3",
			firstOtherNodes:   newTestNodes([]string{"n1"}),
			firstPodCompleted: true,
			secondID:          "b2",
			secondSig:         "sig",
			secondChosenNode:  "n1",
			expectedHint:      "n1",
		},
		{
			name:              "diff sigs",
			firstID:           "a1",
			firstSig:          "sig",
			firstChosenNode:   "n3",
			firstOtherNodes:   newTestNodes([]string{"n1"}),
			firstPodCompleted: true,
			secondID:          "a2",
			secondSig:         "sig2",
			secondChosenNode:  "n1",
			expectedHint:      "",
		},
		{
			name:              "node not filtered",
			firstID:           "a1",
			firstSig:          "sig",
			firstChosenNode:   "n3",
			firstOtherNodes:   newTestNodes([]string{"n1"}),
			firstPodCompleted: true,
			secondID:          "a2",
			secondSig:         "sig",
			secondChosenNode:  "n1",
			expectedHint:      "",
		},
		{
			name:              "incomplete",
			firstID:           "b1",
			firstSig:          "sig",
			firstChosenNode:   "n3",
			firstOtherNodes:   newTestNodes([]string{"n1"}),
			firstPodCompleted: false,
			secondID:          "b2",
			secondSig:         "sig",
			secondChosenNode:  "n1",
			expectedHint:      "",
		},
		{
			name:              "empty list",
			firstID:           "b1",
			firstSig:          "sig",
			firstChosenNode:   "n3",
			firstOtherNodes:   newTestNodes([]string{}),
			firstPodCompleted: true,
			secondID:          "b2",
			secondSig:         "sig",
			secondChosenNode:  "n4",
			expectedHint:      "",
		},
		{
			name:              "nil list",
			firstID:           "b1",
			firstSig:          "sig",
			firstChosenNode:   "n3",
			firstOtherNodes:   nil,
			firstPodCompleted: true,
			secondID:          "b2",
			secondSig:         "sig",
			secondChosenNode:  "n4",
			expectedHint:      "",
		},
		{
			name:              "skip pod",
			firstID:           "b1",
			firstSig:          "sig",
			firstChosenNode:   "n3",
			firstOtherNodes:   newTestNodes([]string{"n1"}),
			firstPodCompleted: true,
			skipPod:           true,
			secondID:          "b2",
			secondSig:         "sig",
			secondChosenNode:  "n1",
			expectedHint:      "",
		},
		{
			name:              "match multi",
			firstID:           "b1",
			firstSig:          "sig",
			firstChosenNode:   "n3",
			firstOtherNodes:   newTestNodes([]string{"n1", "n2"}),
			firstPodCompleted: true,
			secondID:          "b2",
			secondSig:         "sig",
			secondChosenNode:  "n1",
			expectedHint:      "n1",
			expectedState: &batchState{
				signature:   "sig",
				sortedNodes: newTestNodes([]string{"n2"}),
			},
		},
	}

	for _, tt := range tests {
		_, ctx := ktesting.NewTestContext(t)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		testFwk, err := newBatchTestFramework(ctx, batchRegistry)
		if err != nil {
			t.Fatalf("Failed to create framework for testing: %v", err)
		}

		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod",
				UID:  types.UID(tt.firstID),
			},
		}

		signature := tt.firstSig
		batch := newOpportunisticBatch(testFwk, nil, newTestSigFunc(&signature))
		state := framework.NewCycleState()

		// Run the first "pod" through
		hint, _ := batch.GetNodeHint(ctx, pod, state, nil, 1)
		if hint != "" {
			t.Fatalf("Got unexpected hint %s", hint)
		}
		batch.StoreScheduleResults(ctx, tt.firstSig, hint, tt.firstChosenNode, tt.firstOtherNodes, 1)

		if tt.firstPodCompleted {
			batch.PodSucceeded(ctx)
		}

		var cycleCount int64 = 2
		if tt.skipPod {
			cycleCount = 3
		}

		// Run the second pod
		pod2 := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod2",
				UID:  types.UID(tt.secondID),
			},
		}

		lastChosenNode := framework.NewNodeInfo(pod)
		lastChosenNode.SetNode(&v1.Node{ObjectMeta: metav1.ObjectMeta{
			Name: tt.firstChosenNode,
			UID:  types.UID(tt.firstChosenNode),
		}})
		lister := NodeInfoLister{lastChosenNode}

		signature = tt.secondSig
		hint, _ = batch.GetNodeHint(ctx, pod2, state, lister, cycleCount)

		if hint != tt.expectedHint {
			t.Fatalf("Got hint '%s' expected '%s' for test '%s'", hint, tt.expectedHint, tt.name)
		}

		batch.StoreScheduleResults(ctx, tt.secondSig, hint, tt.secondChosenNode, tt.secondOtherNodes, cycleCount)
		batch.PodSucceeded(ctx)

		batchEmpty := batch.state == nil || batch.state.sortedNodes == nil || batch.state.sortedNodes.Len() == 0
		expectedEmpty := tt.expectedState == nil

		if batchEmpty != expectedEmpty {
			t.Fatalf("Expected empty %t, got empty %t for %s", expectedEmpty, batchEmpty, tt.name)
		}
		if !expectedEmpty {
			if batch.state.signature != tt.expectedState.signature {
				t.Fatalf("Got state signature '%s' expected '%s' for test '%s'", batch.state.signature, tt.expectedState.signature, tt.name)
			}
			nodesDiff := cmp.Diff(tt.expectedState.sortedNodes, batch.state.sortedNodes)
			if nodesDiff != "" {
				t.Fatalf("Diff between sortedNodes (-want,+got):\n%s", nodesDiff)
			}
		}
	}
}
