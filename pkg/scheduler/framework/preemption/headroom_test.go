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
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2/ktesting"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
)

type testPodGroupLister struct {
	podGroups map[string]*schedulingv1beta1.PodGroup
}

func (m *testPodGroupLister) Get(namespace, name string) (*schedulingv1beta1.PodGroup, error) {
	if pg, ok := m.podGroups[name]; ok {
		return pg, nil
	}
	return nil, fmt.Errorf("pod group %s not found", name)
}

func TestCanNodeFitPreemptorHeadroom(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)

	highPriorityVal := int32(1000)
	lowPriorityVal := int32(100)
	neverPreempt := v1.PreemptNever

	tests := []struct {
		name                 string
		pod                  *v1.Pod
		node                 *v1.Node
		existingPods         []*v1.Pod
		podGroups            map[string]*schedulingv1beta1.PodGroup
		expectedFit          bool
		expectedStatusReason string
	}{
		{
			name: "node selector mismatch - pruned immediately",
			pod: st.MakePod().Name("preemptor").Namespace("default").Priority(highPriorityVal).
				NodeSelector(map[string]string{"zone": "zone-a"}).
				Req(map[v1.ResourceName]string{v1.ResourceCPU: "1000m"}).Obj(),
			node: st.MakeNode().Name("node1").Label("zone", "zone-b").
				Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4000m", v1.ResourceMemory: "8000Mi"}).Obj(),
			existingPods: []*v1.Pod{
				st.MakePod().Name("v1").Namespace("default").Priority(lowPriorityVal).Obj(),
			},
			expectedFit:          false,
			expectedStatusReason: "node(s) didn't match Pod's node selector",
		},
		{
			name: "untolerated taint - pruned immediately",
			pod: st.MakePod().Name("preemptor").Namespace("default").Priority(highPriorityVal).
				Req(map[v1.ResourceName]string{v1.ResourceCPU: "1000m"}).Obj(),
			node: st.MakeNode().Name("node1").
				Taints([]v1.Taint{{Key: "dedicated", Value: "gpu", Effect: v1.TaintEffectNoSchedule}}).
				Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4000m", v1.ResourceMemory: "8000Mi"}).Obj(),
			existingPods: []*v1.Pod{
				st.MakePod().Name("v1").Namespace("default").Priority(lowPriorityVal).Obj(),
			},
			expectedFit:          false,
			expectedStatusReason: "node(s) had untolerated taint(s)",
		},
		{
			name: "tolerated taint - passes taint check and fits with victim preemption",
			pod: st.MakePod().Name("preemptor").Namespace("default").Priority(highPriorityVal).
				Toleration("dedicated").
				Req(map[v1.ResourceName]string{v1.ResourceCPU: "1000m"}).Obj(),
			node: st.MakeNode().Name("node1").
				Taints([]v1.Taint{{Key: "dedicated", Value: "gpu", Effect: v1.TaintEffectNoSchedule}}).
				Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4000m", v1.ResourceMemory: "8000Mi"}).Obj(),
			existingPods: []*v1.Pod{
				st.MakePod().Name("v1").Namespace("default").Priority(lowPriorityVal).
					Req(map[v1.ResourceName]string{v1.ResourceCPU: "3500m"}).Obj(),
			},
			expectedFit: true,
		},
		{
			name: "no lower priority victims - pruned immediately",
			pod: st.MakePod().Name("preemptor").Namespace("default").Priority(highPriorityVal).
				Req(map[v1.ResourceName]string{v1.ResourceCPU: "2000m"}).Obj(),
			node: st.MakeNode().Name("node1").
				Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4000m", v1.ResourceMemory: "8000Mi"}).Obj(),
			existingPods: []*v1.Pod{
				st.MakePod().Name("non-victim1").Namespace("default").Priority(highPriorityVal).
					Req(map[v1.ResourceName]string{v1.ResourceCPU: "3000m"}).Obj(),
			},
			expectedFit:          false,
			expectedStatusReason: "No preemption victims found for incoming pod",
		},
		{
			name: "raw allocatable CPU insufficient - pruned immediately",
			pod: st.MakePod().Name("preemptor").Namespace("default").Priority(highPriorityVal).
				Req(map[v1.ResourceName]string{v1.ResourceCPU: "8000m"}).Obj(),
			node: st.MakeNode().Name("node1").
				Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4000m", v1.ResourceMemory: "8000Mi"}).Obj(),
			existingPods: []*v1.Pod{
				st.MakePod().Name("v1").Namespace("default").Priority(lowPriorityVal).Obj(),
			},
			expectedFit:          false,
			expectedStatusReason: "Insufficient cpu",
		},
		{
			name: "raw allocatable Memory insufficient - pruned immediately",
			pod: st.MakePod().Name("preemptor").Namespace("default").Priority(highPriorityVal).
				Req(map[v1.ResourceName]string{v1.ResourceMemory: "16000Mi"}).Obj(),
			node: st.MakeNode().Name("node1").
				Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4000m", v1.ResourceMemory: "8000Mi"}).Obj(),
			existingPods: []*v1.Pod{
				st.MakePod().Name("v1").Namespace("default").Priority(lowPriorityVal).Obj(),
			},
			expectedFit:          false,
			expectedStatusReason: "Insufficient memory",
		},
		{
			name: "insufficient reclaimable headroom due to high priority non-preemptible pod",
			pod: st.MakePod().Name("preemptor").Namespace("default").Priority(highPriorityVal).
				Req(map[v1.ResourceName]string{v1.ResourceCPU: "3000m"}).Obj(),
			node: st.MakeNode().Name("node1").
				Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4000m", v1.ResourceMemory: "8000Mi"}).Obj(),
			existingPods: []*v1.Pod{
				// Non-preemptible high priority pod takes 2000m
				st.MakePod().Name("hp-pod").Namespace("default").Priority(highPriorityVal).
					Req(map[v1.ResourceName]string{v1.ResourceCPU: "2000m"}).Obj(),
				// Preemptible low priority pod takes 2000m
				st.MakePod().Name("lp-pod").Namespace("default").Priority(lowPriorityVal).
					Req(map[v1.ResourceName]string{v1.ResourceCPU: "2000m"}).Obj(),
			},
			// Total allocatable 4000m - non-preemptible 2000m = 2000m max available headroom < 3000m required
			expectedFit:          false,
			expectedStatusReason: "Insufficient cpu",
		},
		{
			name: "insufficient reclaimable headroom due to PreemptNever pod",
			pod: st.MakePod().Name("preemptor").Namespace("default").Priority(highPriorityVal).
				Req(map[v1.ResourceName]string{v1.ResourceCPU: "3000m"}).Obj(),
			node: st.MakeNode().Name("node1").
				Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4000m", v1.ResourceMemory: "8000Mi"}).Obj(),
			existingPods: []*v1.Pod{
				// PreemptNever pod takes 2500m
				st.MakePod().Name("never-pod").Namespace("default").Priority(lowPriorityVal).
					PreemptionPolicy(neverPreempt).
					Req(map[v1.ResourceName]string{v1.ResourceCPU: "2500m"}).Obj(),
				// Normal low-priority pod takes 1500m
				st.MakePod().Name("lp-pod").Namespace("default").Priority(lowPriorityVal).
					Req(map[v1.ResourceName]string{v1.ResourceCPU: "1500m"}).Obj(),
			},
			// Headroom is 4000m - 2500m = 1500m < 3000m
			expectedFit:          false,
			expectedStatusReason: "Insufficient cpu",
		},
		{
			name: "sufficient headroom after preempting lower priority pods",
			pod: st.MakePod().Name("preemptor").Namespace("default").Priority(highPriorityVal).
				Req(map[v1.ResourceName]string{v1.ResourceCPU: "3000m", v1.ResourceMemory: "4000Mi"}).Obj(),
			node: st.MakeNode().Name("node1").
				Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4000m", v1.ResourceMemory: "8000Mi"}).Obj(),
			existingPods: []*v1.Pod{
				// Non-preemptible pod takes 500m
				st.MakePod().Name("hp-pod").Namespace("default").Priority(highPriorityVal).
					Req(map[v1.ResourceName]string{v1.ResourceCPU: "500m", v1.ResourceMemory: "1000Mi"}).Obj(),
				// Preemptible low priority pod takes 3500m
				st.MakePod().Name("lp-pod").Namespace("default").Priority(lowPriorityVal).
					Req(map[v1.ResourceName]string{v1.ResourceCPU: "3500m", v1.ResourceMemory: "7000Mi"}).Obj(),
			},
			// Available headroom: 4000m - 500m = 3500m >= 3000m. Memory: 8000Mi - 1000Mi = 7000Mi >= 4000Mi.
			expectedFit: true,
		},
		{
			name: "podgroup priority resolution - pod inherits higher priority from podgroup",
			pod: st.MakePod().Name("preemptor").Namespace("default").Priority(int32(500)).
				Req(map[v1.ResourceName]string{v1.ResourceCPU: "3000m"}).Obj(),
			node: st.MakeNode().Name("node1").
				Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4000m", v1.ResourceMemory: "8000Mi"}).Obj(),
			existingPods: []*v1.Pod{
				// Pod has standalone priority 100, but belongs to pg-high with priority 1000!
				st.MakePod().Name("pg-pod").Namespace("default").Priority(lowPriorityVal).
					PodGroupName("pg-high").
					Req(map[v1.ResourceName]string{v1.ResourceCPU: "2500m"}).Obj(),
				// Normal low-priority pod takes 1500m
				st.MakePod().Name("lp-pod").Namespace("default").Priority(lowPriorityVal).
					Req(map[v1.ResourceName]string{v1.ResourceCPU: "1500m"}).Obj(),
			},
			podGroups: map[string]*schedulingv1beta1.PodGroup{
				"pg-high": {
					ObjectMeta: metav1.ObjectMeta{Name: "pg-high", Namespace: "default"},
					Spec:       schedulingv1beta1.PodGroupSpec{PriorityClassName: "high", Priority: &highPriorityVal},
				},
			},
			// Headroom is 4000m - 2500m = 1500m < 3000m -> Insufficient CPU
			expectedFit:          false,
			expectedStatusReason: "Insufficient cpu",
		},
		{
			name: "scalar resource headroom (e.g. nvidia.com/gpu)",
			pod: st.MakePod().Name("preemptor").Namespace("default").Priority(highPriorityVal).
				Req(map[v1.ResourceName]string{
					v1.ResourceCPU:                    "1000m",
					v1.ResourceName("nvidia.com/gpu"): "4",
				}).Obj(),
			node: st.MakeNode().Name("node1").
				Capacity(map[v1.ResourceName]string{
					v1.ResourceCPU:                    "16000m",
					v1.ResourceMemory:                 "64000Mi",
					v1.ResourceName("nvidia.com/gpu"): "8",
				}).Obj(),
			existingPods: []*v1.Pod{
				// Non-preemptible pod uses 6 GPUs
				st.MakePod().Name("hp-gpu-pod").Namespace("default").Priority(highPriorityVal).
					Req(map[v1.ResourceName]string{
						v1.ResourceCPU:                    "4000m",
						v1.ResourceName("nvidia.com/gpu"): "6",
					}).Obj(),
				// Preemptible pod uses 2 GPUs
				st.MakePod().Name("lp-gpu-pod").Namespace("default").Priority(lowPriorityVal).
					Req(map[v1.ResourceName]string{
						v1.ResourceCPU:                    "2000m",
						v1.ResourceName("nvidia.com/gpu"): "2",
					}).Obj(),
			},
			// Available GPU headroom: 8 - 6 = 2 < 4 requested -> Insufficient nvidia.com/gpu
			expectedFit:          false,
			expectedStatusReason: "Insufficient nvidia.com/gpu",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodeInfo := framework.NewNodeInfo(tt.existingPods...)
			nodeInfo.SetNode(tt.node)

			var pgLister fwk.PodGroupLister
			if tt.podGroups != nil {
				pgLister = &testPodGroupLister{podGroups: tt.podGroups}
			}

			fits, status := CanNodeFitPreemptorHeadroom(logger, tt.pod, nodeInfo, pgLister, nil, false)
			if fits != tt.expectedFit {
				t.Fatalf("expected fit=%v, got=%v (status=%v)", tt.expectedFit, fits, status)
			}
			if !tt.expectedFit && tt.expectedStatusReason != "" {
				if status == nil || status.Message() != tt.expectedStatusReason {
					t.Errorf("expected status reason %q, got %q", tt.expectedStatusReason, status.Message())
				}
			}
		})
	}
}

func BenchmarkCanNodeFitPreemptorHeadroom(b *testing.B) {
	logger, _ := ktesting.NewTestContext(b)
	highPriorityVal := int32(1000)
	lowPriorityVal := int32(100)

	node := st.MakeNode().Name("node1").
		Capacity(map[v1.ResourceName]string{
			v1.ResourceCPU:    "256000m",
			v1.ResourceMemory: "1024000Mi",
			v1.ResourcePods:   "1000",
		}).Obj()

	gangSizes := []int{16, 64, 256}

	for _, gangSize := range gangSizes {
		b.Run(fmt.Sprintf("GangSize_%d_PodsOnNode", gangSize), func(b *testing.B) {
			existingPods := make([]*v1.Pod, gangSize)
			for i := 0; i < gangSize; i++ {
				prio := lowPriorityVal
				if i%2 == 0 {
					prio = highPriorityVal
				}
				existingPods[i] = st.MakePod().Name(fmt.Sprintf("pod-%d", i)).Namespace("default").
					UID(fmt.Sprintf("pod-uid-%d", i)).
					Priority(prio).
					Req(map[v1.ResourceName]string{v1.ResourceCPU: "500m", v1.ResourceMemory: "2000Mi"}).Obj()
			}
			nodeInfo := framework.NewNodeInfo(existingPods...)
			nodeInfo.SetNode(node)

			preemptor := st.MakePod().Name("preemptor").Namespace("default").
				UID("preemptor-uid").
				Priority(highPriorityVal).
				Req(map[v1.ResourceName]string{v1.ResourceCPU: "4000m", v1.ResourceMemory: "16000Mi"}).Obj()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				fits, status := CanNodeFitPreemptorHeadroom(logger, preemptor, nodeInfo, nil, nil, false)
				if !fits {
					b.Fatalf("expected node to fit: %v", status)
				}
			}
		})
	}
}
