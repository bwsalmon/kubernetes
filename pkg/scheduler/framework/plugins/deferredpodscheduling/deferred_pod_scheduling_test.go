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

package deferredpodscheduling

import (
	"context"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2/ktesting"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
)

func TestDeferredPodScheduling_PreFilter(t *testing.T) {
	tests := []struct {
		name       string
		pod        *v1.Pod
		gateOn     bool
		wantStatus *fwk.Status
	}{
		{
			name: "feature gate off, deferred pod -> skip",
			pod: st.MakePod().Name("pod1").Node("node1").
				Condition(v1.PodResizePending, v1.ConditionTrue, v1.PodReasonDeferred).Obj(),
			gateOn:     false,
			wantStatus: fwk.NewStatus(fwk.Skip),
		},
		{
			name:       "feature gate on, non-deferred pod -> skip",
			pod:        st.MakePod().Name("pod1").Node("node1").Obj(),
			gateOn:     true,
			wantStatus: fwk.NewStatus(fwk.Skip),
		},
		{
			name: "feature gate on, deferred pod -> success/nil",
			pod: st.MakePod().Name("pod1").Node("node1").
				Condition(v1.PodResizePending, v1.ConditionTrue, v1.PodReasonDeferred).Obj(),
			gateOn:     true,
			wantStatus: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pl := &DeferredPodScheduling{
				enableInPlacePodVerticalScalingSchedulerPreemption: tt.gateOn,
			}
			_, status := pl.PreFilter(context.Background(), nil, tt.pod, nil)
			if !statusEqual(status, tt.wantStatus) {
				t.Errorf("PreFilter status = %v, want %v", status, tt.wantStatus)
			}
		})
	}
}

func TestDeferredPodScheduling_Filter(t *testing.T) {
	tests := []struct {
		name       string
		pod        *v1.Pod
		node       *v1.Node
		gateOn     bool
		wantStatus *fwk.Status
	}{
		{
			name: "feature gate off -> success (ignore disabled policy)",
			pod: st.MakePod().Name("pod1").Node("node1").
				Condition(v1.PodResizePending, v1.ConditionTrue, v1.PodReasonDeferred).Obj(),
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node1"},
				Spec: v1.NodeSpec{
					PodPreemptionPolicy: &v1.NodePodPreemptionPolicy{
						DisableResizePreemption: []string{"policy1"},
					},
				},
			},
			gateOn:     false,
			wantStatus: nil,
		},
		{
			name: "feature gate on, non-deferred pod -> success (ignore disabled policy)",
			pod:  st.MakePod().Name("pod1").Node("node1").Obj(),
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node1"},
				Spec: v1.NodeSpec{
					PodPreemptionPolicy: &v1.NodePodPreemptionPolicy{
						DisableResizePreemption: []string{"policy1"},
					},
				},
			},
			gateOn:     true,
			wantStatus: nil,
		},
		{
			name: "node has nil preemption policy -> success",
			pod: st.MakePod().Name("pod1").Node("node1").
				Condition(v1.PodResizePending, v1.ConditionTrue, v1.PodReasonDeferred).Obj(),
			node:       st.MakeNode().Name("node1").Obj(),
			gateOn:     true,
			wantStatus: nil,
		},
		{
			name: "node has empty disable preemption policy -> success",
			pod: st.MakePod().Name("pod1").Node("node1").
				Condition(v1.PodResizePending, v1.ConditionTrue, v1.PodReasonDeferred).Obj(),
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node1"},
				Spec: v1.NodeSpec{
					PodPreemptionPolicy: &v1.NodePodPreemptionPolicy{
						DisableResizePreemption: []string{},
					},
				},
			},
			gateOn:     true,
			wantStatus: nil,
		},
		{
			name: "node has disabled preemption policy -> unschedulable",
			pod: st.MakePod().Name("pod1").Node("node1").
				Condition(v1.PodResizePending, v1.ConditionTrue, v1.PodReasonDeferred).Obj(),
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node1"},
				Spec: v1.NodeSpec{
					PodPreemptionPolicy: &v1.NodePodPreemptionPolicy{
						DisableResizePreemption: []string{"policy1"},
					},
				},
			},
			gateOn:     true,
			wantStatus: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ErrReasonNodeDisablesResizePreemption),
		},
		{
			name: "node name does not match pod assigned node -> unschedulable",
			pod: st.MakePod().Name("pod1").Node("node1").
				Condition(v1.PodResizePending, v1.ConditionTrue, v1.PodReasonDeferred).Obj(),
			node:       st.MakeNode().Name("node2").Obj(),
			gateOn:     true,
			wantStatus: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "pod assigned to different node"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pl := &DeferredPodScheduling{
				enableInPlacePodVerticalScalingSchedulerPreemption: tt.gateOn,
			}
			nodeInfo := framework.NewNodeInfo()
			nodeInfo.SetNode(tt.node)
			status := pl.Filter(context.Background(), nil, tt.pod, nodeInfo)
			if !statusEqual(status, tt.wantStatus) {
				t.Errorf("Filter status = %v, want %v", status, tt.wantStatus)
			}
		})
	}
}

func TestDeferredPodScheduling_isSchedulableAfterNodeChange(t *testing.T) {
	nodeEnabled := st.MakeNode().Name("node1").Obj()
	nodeDisabled := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node1"},
		Spec: v1.NodeSpec{
			PodPreemptionPolicy: &v1.NodePodPreemptionPolicy{
				DisableResizePreemption: []string{"policy1"},
			},
		},
	}
	nodeOther := st.MakeNode().Name("node2").Obj()

	tests := []struct {
		name     string
		pod      *v1.Pod
		oldObj   interface{}
		newObj   interface{}
		gateOn   bool
		wantHint fwk.QueueingHint
	}{
		{
			name: "gate off, deferred pod, transition to enabled -> skip",
			pod: st.MakePod().Name("pod1").Node("node1").
				Condition(v1.PodResizePending, v1.ConditionTrue, v1.PodReasonDeferred).Obj(),
			oldObj:   nodeDisabled,
			newObj:   nodeEnabled,
			gateOn:   false,
			wantHint: fwk.QueueSkip,
		},
		{
			name:     "gate on, non-deferred pod, transition to enabled -> skip",
			pod:      st.MakePod().Name("pod1").Node("node1").Obj(),
			oldObj:   nodeDisabled,
			newObj:   nodeEnabled,
			gateOn:   true,
			wantHint: fwk.QueueSkip,
		},
		{
			name: "gate on, deferred pod, node is not pod's assigned node -> skip",
			pod: st.MakePod().Name("pod1").Node("node1").
				Condition(v1.PodResizePending, v1.ConditionTrue, v1.PodReasonDeferred).Obj(),
			oldObj:   nodeOther,
			newObj:   nodeOther,
			gateOn:   true,
			wantHint: fwk.QueueSkip,
		},
		{
			name: "gate on, deferred pod, policy transition disabled -> enabled -> queue",
			pod: st.MakePod().Name("pod1").Node("node1").
				Condition(v1.PodResizePending, v1.ConditionTrue, v1.PodReasonDeferred).Obj(),
			oldObj:   nodeDisabled,
			newObj:   nodeEnabled,
			gateOn:   true,
			wantHint: fwk.Queue,
		},
		{
			name: "gate on, deferred pod, policy transition enabled -> disabled -> skip",
			pod: st.MakePod().Name("pod1").Node("node1").
				Condition(v1.PodResizePending, v1.ConditionTrue, v1.PodReasonDeferred).Obj(),
			oldObj:   nodeEnabled,
			newObj:   nodeDisabled,
			gateOn:   true,
			wantHint: fwk.QueueSkip,
		},
		{
			name: "gate on, deferred pod, other node field updated -> skip",
			pod: st.MakePod().Name("pod1").Node("node1").
				Condition(v1.PodResizePending, v1.ConditionTrue, v1.PodReasonDeferred).Obj(),
			oldObj:   nodeEnabled,
			newObj:   nodeEnabled,
			gateOn:   true,
			wantHint: fwk.QueueSkip,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			pl := &DeferredPodScheduling{
				enableInPlacePodVerticalScalingSchedulerPreemption: tt.gateOn,
			}
			hint, err := pl.isSchedulableAfterNodeChange(logger, tt.pod, tt.oldObj, tt.newObj)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if hint != tt.wantHint {
				t.Errorf("isSchedulableAfterNodeChange hint = %v, want %v", hint, tt.wantHint)
			}
		})
	}
}

func TestDeferredPodScheduling_isSchedulableAfterNodeAdd(t *testing.T) {
	nodeEnabled := st.MakeNode().Name("node1").Obj()
	nodeDisabled := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node1"},
		Spec: v1.NodeSpec{
			PodPreemptionPolicy: &v1.NodePodPreemptionPolicy{
				DisableResizePreemption: []string{"policy1"},
			},
		},
	}
	nodeOther := st.MakeNode().Name("node2").Obj()

	tests := []struct {
		name     string
		pod      *v1.Pod
		newObj   interface{}
		gateOn   bool
		wantHint fwk.QueueingHint
	}{
		{
			name: "gate off, deferred pod, add enabled node -> skip",
			pod: st.MakePod().Name("pod1").Node("node1").
				Condition(v1.PodResizePending, v1.ConditionTrue, v1.PodReasonDeferred).Obj(),
			newObj:   nodeEnabled,
			gateOn:   false,
			wantHint: fwk.QueueSkip,
		},
		{
			name:     "gate on, non-deferred pod, add enabled node -> skip",
			pod:      st.MakePod().Name("pod1").Node("node1").Obj(),
			newObj:   nodeEnabled,
			gateOn:   true,
			wantHint: fwk.QueueSkip,
		},
		{
			name: "gate on, deferred pod, node is not pod's assigned node -> skip",
			pod: st.MakePod().Name("pod1").Node("node1").
				Condition(v1.PodResizePending, v1.ConditionTrue, v1.PodReasonDeferred).Obj(),
			newObj:   nodeOther,
			gateOn:   true,
			wantHint: fwk.QueueSkip,
		},
		{
			name: "gate on, deferred pod, add enabled node -> queue",
			pod: st.MakePod().Name("pod1").Node("node1").
				Condition(v1.PodResizePending, v1.ConditionTrue, v1.PodReasonDeferred).Obj(),
			newObj:   nodeEnabled,
			gateOn:   true,
			wantHint: fwk.Queue,
		},
		{
			name: "gate on, deferred pod, add disabled node -> skip",
			pod: st.MakePod().Name("pod1").Node("node1").
				Condition(v1.PodResizePending, v1.ConditionTrue, v1.PodReasonDeferred).Obj(),
			newObj:   nodeDisabled,
			gateOn:   true,
			wantHint: fwk.QueueSkip,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			pl := &DeferredPodScheduling{
				enableInPlacePodVerticalScalingSchedulerPreemption: tt.gateOn,
			}
			hint, err := pl.isSchedulableAfterNodeAdd(logger, tt.pod, nil, tt.newObj)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if hint != tt.wantHint {
				t.Errorf("isSchedulableAfterNodeAdd hint = %v, want %v", hint, tt.wantHint)
			}
		})
	}
}

func TestDeferredPodScheduling_Permit(t *testing.T) {
	tests := []struct {
		name       string
		pod        *v1.Pod
		gateOn     bool
		wantStatus *fwk.Status
	}{
		{
			name: "feature gate off, deferred pod -> success/nil",
			pod: st.MakePod().Name("pod1").Node("node1").
				Condition(v1.PodResizePending, v1.ConditionTrue, v1.PodReasonDeferred).Obj(),
			gateOn:     false,
			wantStatus: nil,
		},
		{
			name:       "feature gate on, non-deferred pod -> success/nil",
			pod:        st.MakePod().Name("pod1").Node("node1").Obj(),
			gateOn:     true,
			wantStatus: nil,
		},
		{
			name: "feature gate on, deferred pod -> reject with UnschedulableAndUnresolvable",
			pod: st.MakePod().Name("pod1").Node("node1").
				Condition(v1.PodResizePending, v1.ConditionTrue, v1.PodReasonDeferred).Obj(),
			gateOn:     true,
			wantStatus: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "pod resize fits, waiting for Kubelet actuation"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pl := &DeferredPodScheduling{
				enableInPlacePodVerticalScalingSchedulerPreemption: tt.gateOn,
			}
			status, _ := pl.Permit(context.Background(), nil, tt.pod, "node1")
			if !statusEqual(status, tt.wantStatus) {
				t.Errorf("Permit status = %v, want %v", status, tt.wantStatus)
			}
		})
	}
}

func statusEqual(s1, s2 *fwk.Status) bool {
	if s1 == nil && s2 == nil {
		return true
	}
	if s1 == nil || s2 == nil {
		return false
	}
	return s1.Code() == s2.Code() && s1.Message() == s2.Message()
}

func TestDeferredPodScheduling_MultiContainerHeterogeneousResize(t *testing.T) {
	// Pod with container c1 expanding CPU (100m -> 500m) and container c2 expanding Memory (100Mi -> 500Mi).
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "preemptor-multi",
			Namespace: "default",
			UID:       "preemptor-multi-uid",
		},
		Spec: v1.PodSpec{
			NodeName: "node1",
			Containers: []v1.Container{
				{
					Name: "c1",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("500m"),
							v1.ResourceMemory: resource.MustParse("100Mi"),
						},
					},
				},
				{
					Name: "c2",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("100m"),
							v1.ResourceMemory: resource.MustParse("500Mi"),
						},
					},
				},
			},
		},
		Status: v1.PodStatus{
			Conditions: []v1.PodCondition{
				{
					Type:   v1.PodResizePending,
					Status: v1.ConditionTrue,
					Reason: v1.PodReasonDeferred,
				},
			},
			ContainerStatuses: []v1.ContainerStatus{
				{
					Name: "c1",
					AllocatedResources: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("100m"),
						v1.ResourceMemory: resource.MustParse("100Mi"),
					},
				},
				{
					Name: "c2",
					AllocatedResources: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("100m"),
						v1.ResourceMemory: resource.MustParse("100Mi"),
					},
				},
			},
		},
	}

	// 1. Validate PreFilter behavior
	pl := &DeferredPodScheduling{
		enableInPlacePodVerticalScalingSchedulerPreemption: true,
	}
	_, status := pl.PreFilter(context.Background(), nil, pod, nil)
	if status != nil {
		t.Fatalf("PreFilter expected nil status for multi-container deferred pod, got: %v", status)
	}

	// 2. Validate Filter behavior on assigned node
	node := st.MakeNode().Name("node1").Obj()
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(node)
	status = pl.Filter(context.Background(), nil, pod, nodeInfo)
	if status != nil {
		t.Fatalf("Filter expected nil status for assigned node, got: %v", status)
	}

	// Filter on different node should fail
	otherNode := st.MakeNode().Name("node2").Obj()
	otherNodeInfo := framework.NewNodeInfo()
	otherNodeInfo.SetNode(otherNode)
	status = pl.Filter(context.Background(), nil, pod, otherNodeInfo)
	if status == nil || status.Code() != fwk.UnschedulableAndUnresolvable {
		t.Fatalf("Filter expected UnschedulableAndUnresolvable for different node, got: %v", status)
	}

	// 3. Validate Permit behavior (returns UnschedulableAndUnresolvable to wait for Kubelet actuation)
	status, _ = pl.Permit(context.Background(), nil, pod, "node1")
	if status == nil || status.Code() != fwk.UnschedulableAndUnresolvable {
		t.Fatalf("Permit expected UnschedulableAndUnresolvable, got: %v", status)
	}
	if status.Message() != "pod resize fits, waiting for Kubelet actuation" {
		t.Fatalf("Permit unexpected message: %v", status.Message())
	}

	// 4. Validate Delta Resource Calculations across multiple containers
	var totalAllocatedCPU, totalAllocatedMem int64
	for _, cStatus := range pod.Status.ContainerStatuses {
		totalAllocatedCPU += cStatus.AllocatedResources.Cpu().MilliValue()
		totalAllocatedMem += cStatus.AllocatedResources.Memory().Value()
	}

	var totalDesiredCPU, totalDesiredMem int64
	for _, c := range pod.Spec.Containers {
		totalDesiredCPU += c.Resources.Requests.Cpu().MilliValue()
		totalDesiredMem += c.Resources.Requests.Memory().Value()
	}

	expectedDesiredCPU := int64(600)
	expectedDesiredMem := int64(600 * 1024 * 1024)
	if totalDesiredCPU != expectedDesiredCPU {
		t.Errorf("Total desired CPU = %vm, want %vm", totalDesiredCPU, expectedDesiredCPU)
	}
	if totalDesiredMem != expectedDesiredMem {
		t.Errorf("Total desired Memory = %v, want %v", totalDesiredMem, expectedDesiredMem)
	}

	expectedAllocatedCPU := int64(200)
	expectedAllocatedMem := int64(200 * 1024 * 1024)
	if totalAllocatedCPU != expectedAllocatedCPU {
		t.Errorf("Total allocated CPU = %vm, want %vm", totalAllocatedCPU, expectedAllocatedCPU)
	}
	if totalAllocatedMem != expectedAllocatedMem {
		t.Errorf("Total allocated Memory = %v, want %v", totalAllocatedMem, expectedAllocatedMem)
	}

	deltaCPU := totalDesiredCPU - totalAllocatedCPU
	deltaMem := totalDesiredMem - totalAllocatedMem
	if deltaCPU != 400 {
		t.Errorf("Delta CPU = %v, want 400m", deltaCPU)
	}
	if deltaMem != 400*1024*1024 {
		t.Errorf("Delta Mem = %v, want 400Mi", deltaMem)
	}
}

func TestDeferredPodScheduling_ResizeCancellationReservationCleanup(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	// Initial pod with deferred resize: allocated 100m CPU, requesting 500m CPU (delta 400m CPU)
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "preemptor-cancel",
			Namespace: "default",
			UID:       "preemptor-cancel-uid",
		},
		Spec: v1.PodSpec{
			NodeName: "node1",
			Containers: []v1.Container{
				{
					Name: "c1",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("500m"),
							v1.ResourceMemory: resource.MustParse("100Mi"),
						},
					},
				},
			},
		},
		Status: v1.PodStatus{
			Conditions: []v1.PodCondition{
				{
					Type:   v1.PodResizePending,
					Status: v1.ConditionTrue,
					Reason: v1.PodReasonDeferred,
				},
			},
			ContainerStatuses: []v1.ContainerStatus{
				{
					Name: "c1",
					AllocatedResources: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("100m"),
						v1.ResourceMemory: resource.MustParse("100Mi"),
					},
				},
			},
		},
	}

	node := st.MakeNode().Name("node1").Capacity(map[v1.ResourceName]string{
		v1.ResourceCPU:    "1000m",
		v1.ResourceMemory: "1000Mi",
	}).Obj()

	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(node)
	nodeInfo.AddPod(pod)

	// In the snapshot / nodeInfo, pod requests are accounted for (500m CPU)
	if nodeInfo.Requested.MilliCPU != 500 {
		t.Fatalf("Expected nodeInfo.Requested.MilliCPU to be 500m before cancellation, got %v", nodeInfo.Requested.MilliCPU)
	}

	// Downsizing pod spec back to 100m CPU and clearing deferred condition
	cancelledPod := pod.DeepCopy()
	cancelledPod.Spec.Containers[0].Resources.Requests = v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("100m"),
		v1.ResourceMemory: resource.MustParse("100Mi"),
	}
	cancelledPod.Status.Conditions = []v1.PodCondition{
		{
			Type:   v1.PodScheduled,
			Status: v1.ConditionTrue,
		},
	}

	// Update the pod in NodeInfo (Remove old pod, Add updated pod)
	if err := nodeInfo.RemovePod(logger, pod); err != nil {
		t.Fatalf("Failed to remove pod from nodeInfo: %v", err)
	}
	nodeInfo.AddPod(cancelledPod)

	// Verify that delta reservation is cleared and nodeInfo.Requested immediately drops to 100m CPU
	if nodeInfo.Requested.MilliCPU != 100 {
		t.Fatalf("Expected nodeInfo.Requested.MilliCPU to be 100m after cancellation, got %v", nodeInfo.Requested.MilliCPU)
	}

	// Verify DeferredPodScheduling plugin ignores cancelled pod
	pl := &DeferredPodScheduling{
		enableInPlacePodVerticalScalingSchedulerPreemption: true,
	}
	_, status := pl.PreFilter(context.Background(), nil, cancelledPod, nil)
	if status == nil || status.Code() != fwk.Skip {
		t.Fatalf("Expected PreFilter to return Skip for cancelled/non-deferred pod, got: %v", status)
	}
}
