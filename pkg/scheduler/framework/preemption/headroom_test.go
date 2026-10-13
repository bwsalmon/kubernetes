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
	"math"
	"sync/atomic"
	"testing"

	v1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
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
		compositePodGroups   map[string]*schedulingv1alpha3.CompositePodGroup
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
		{
			name: "preemptor podgroup priority resolution - preemptor inherits higher priority from podgroup",
			pod: st.MakePod().Name("preemptor").Namespace("default").Priority(lowPriorityVal).
				PodGroupName("pg-preemptor-high").
				Req(map[v1.ResourceName]string{v1.ResourceCPU: "3000m"}).Obj(),
			node: st.MakeNode().Name("node1").
				Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4000m", v1.ResourceMemory: "8000Mi"}).Obj(),
			existingPods: []*v1.Pod{
				// Mid-priority pod (500) has higher priority than preemptor's pod priority (100),
				// but LOWER than preemptor's podgroup priority (1000). It must be preemptible!
				st.MakePod().Name("mid-pod").Namespace("default").Priority(int32(500)).
					Req(map[v1.ResourceName]string{v1.ResourceCPU: "2500m"}).Obj(),
				st.MakePod().Name("lp-pod").Namespace("default").Priority(lowPriorityVal).
					Req(map[v1.ResourceName]string{v1.ResourceCPU: "1500m"}).Obj(),
			},
			podGroups: map[string]*schedulingv1beta1.PodGroup{
				"pg-preemptor-high": {
					ObjectMeta: metav1.ObjectMeta{Name: "pg-preemptor-high", Namespace: "default"},
					Spec:       schedulingv1beta1.PodGroupSpec{PriorityClassName: "high", Priority: &highPriorityVal},
				},
			},
			expectedFit: true,
		},
		{
			name: "preemptor composite podgroup priority resolution - preemptor inherits priority from root composite podgroup",
			pod: st.MakePod().Name("preemptor").Namespace("default").Priority(lowPriorityVal).
				PodGroupName("child-pg").
				Req(map[v1.ResourceName]string{v1.ResourceCPU: "3000m"}).Obj(),
			node: st.MakeNode().Name("node1").
				Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4000m", v1.ResourceMemory: "8000Mi"}).Obj(),
			existingPods: []*v1.Pod{
				// Existing pod priority 1500 is higher than preemptor (100), but lower than root CPG (2000).
				st.MakePod().Name("mid-pod").Namespace("default").Priority(int32(1500)).
					Req(map[v1.ResourceName]string{v1.ResourceCPU: "2500m"}).Obj(),
				st.MakePod().Name("lp-pod").Namespace("default").Priority(lowPriorityVal).
					Req(map[v1.ResourceName]string{v1.ResourceCPU: "1500m"}).Obj(),
			},
			podGroups: map[string]*schedulingv1beta1.PodGroup{
				"child-pg": st.MakePodGroup().Name("child-pg").Priority(50).ParentCompositePodGroup("root-cpg").Obj(),
			},
			compositePodGroups: map[string]*schedulingv1alpha3.CompositePodGroup{
				"root-cpg": {
					ObjectMeta: metav1.ObjectMeta{Name: "root-cpg"},
					Spec: schedulingv1alpha3.CompositePodGroupSpec{
						Priority: new(int32(2000)),
					},
				},
			},
			expectedFit: true,
		},
		{
			name: "in-place vertical scaling deferred resize on assigned node with untolerated taint",
			pod: func() *v1.Pod {
				p := st.MakePod().Name("resizing-pod").Namespace("default").Node("node1").
					Priority(highPriorityVal).
					Req(map[v1.ResourceName]string{v1.ResourceCPU: "3000m"}).Obj()
				p.Status.Conditions = []v1.PodCondition{
					{
						Type:   v1.PodResizePending,
						Status: v1.ConditionTrue,
						Reason: v1.PodReasonDeferred,
					},
				}
				return p
			}(),
			node: st.MakeNode().Name("node1").
				Taints([]v1.Taint{{Key: "dedicated", Value: "gpu", Effect: v1.TaintEffectNoSchedule}}).
				Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4000m", v1.ResourceMemory: "8000Mi"}).Obj(),
			existingPods: []*v1.Pod{
				func() *v1.Pod {
					p := st.MakePod().Name("resizing-pod").Namespace("default").Node("node1").
						Priority(highPriorityVal).
						Req(map[v1.ResourceName]string{v1.ResourceCPU: "1000m"}).Obj()
					p.Status.Conditions = []v1.PodCondition{
						{
							Type:   v1.PodResizePending,
							Status: v1.ConditionTrue,
							Reason: v1.PodReasonDeferred,
						},
					}
					return p
				}(),
				st.MakePod().Name("lp-pod").Namespace("default").Priority(lowPriorityVal).
					Req(map[v1.ResourceName]string{v1.ResourceCPU: "3000m"}).Obj(),
			},
			expectedFit: true,
		},
		{
			name: "in-place vertical scaling deferred resize on different candidate node - pruned immediately",
			pod: func() *v1.Pod {
				p := st.MakePod().Name("resizing-pod").Namespace("default").Node("node1").
					Priority(highPriorityVal).
					Req(map[v1.ResourceName]string{v1.ResourceCPU: "3000m"}).Obj()
				p.Status.Conditions = []v1.PodCondition{
					{
						Type:   v1.PodResizePending,
						Status: v1.ConditionTrue,
						Reason: v1.PodReasonDeferred,
					},
				}
				return p
			}(),
			node: st.MakeNode().Name("node2").
				Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4000m", v1.ResourceMemory: "8000Mi"}).Obj(),
			existingPods: []*v1.Pod{
				st.MakePod().Name("lp-pod").Namespace("default").Priority(lowPriorityVal).
					Req(map[v1.ResourceName]string{v1.ResourceCPU: "3000m"}).Obj(),
			},
			expectedFit:          false,
			expectedStatusReason: "deferred resize pod cannot preempt on a different node",
		},
		{
			name: "in-place vertical scaling deferred resize on assigned node with mismatching NodeSelector",
			pod: func() *v1.Pod {
				p := st.MakePod().Name("resizing-pod").Namespace("default").Node("node1").
					NodeSelector(map[string]string{"zone": "zone-a"}).
					Priority(highPriorityVal).
					Req(map[v1.ResourceName]string{v1.ResourceCPU: "3000m"}).Obj()
				p.Status.Conditions = []v1.PodCondition{
					{
						Type:   v1.PodResizePending,
						Status: v1.ConditionTrue,
						Reason: v1.PodReasonDeferred,
					},
				}
				return p
			}(),
			node: st.MakeNode().Name("node1").Label("zone", "zone-b").
				Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4000m", v1.ResourceMemory: "8000Mi"}).Obj(),
			existingPods: []*v1.Pod{
				st.MakePod().Name("resizing-pod").Namespace("default").Node("node1").
					Priority(highPriorityVal).
					Req(map[v1.ResourceName]string{v1.ResourceCPU: "1000m"}).Obj(),
				st.MakePod().Name("lp-pod").Namespace("default").Priority(lowPriorityVal).
					Req(map[v1.ResourceName]string{v1.ResourceCPU: "3000m"}).Obj(),
			},
			expectedFit: true,
		},
		{
			name: "saturating addition prevents int64 overflow on extreme scalar resources",
			pod: st.MakePod().Name("preemptor").Namespace("default").Priority(highPriorityVal).
				Req(map[v1.ResourceName]string{
					v1.ResourceCPU:                   "1000m",
					v1.ResourceName("custom.io/gpu"): "10",
				}).Obj(),
			node: st.MakeNode().Name("node1").
				Capacity(map[v1.ResourceName]string{
					v1.ResourceCPU:                   "4000m",
					v1.ResourceMemory:                "8000Mi",
					v1.ResourceName("custom.io/gpu"): "100",
				}).Obj(),
			existingPods: []*v1.Pod{
				st.MakePod().Name("hp1").Namespace("default").Priority(highPriorityVal).
					Req(map[v1.ResourceName]string{
						v1.ResourceName("custom.io/gpu"): fmt.Sprintf("%d", int64(math.MaxInt64-50)),
					}).Obj(),
				st.MakePod().Name("hp2").Namespace("default").Priority(highPriorityVal).
					Req(map[v1.ResourceName]string{
						v1.ResourceName("custom.io/gpu"): "100",
					}).Obj(),
				st.MakePod().Name("lp").Namespace("default").Priority(lowPriorityVal).
					Req(map[v1.ResourceName]string{
						v1.ResourceCPU: "100m",
					}).Obj(),
			},
			expectedFit:          false,
			expectedStatusReason: "Insufficient custom.io/gpu",
		},
		{
			name: "adversarial pod spec with large number of tolerations",
			pod: func() *v1.Pod {
				p := st.MakePod().Name("preemptor").Namespace("default").Priority(highPriorityVal).
					Req(map[v1.ResourceName]string{v1.ResourceCPU: "1000m"}).Obj()
				for i := 0; i < 500; i++ {
					p.Spec.Tolerations = append(p.Spec.Tolerations, v1.Toleration{
						Key:      fmt.Sprintf("key-%d", i),
						Operator: v1.TolerationOpEqual,
						Value:    "val",
						Effect:   v1.TaintEffectNoSchedule,
					})
				}
				return p
			}(),
			node: st.MakeNode().Name("node1").
				Taints([]v1.Taint{
					{Key: "unmatched-key", Value: "val", Effect: v1.TaintEffectNoSchedule},
				}).
				Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "4000m", v1.ResourceMemory: "8000Mi"}).Obj(),
			existingPods: []*v1.Pod{
				st.MakePod().Name("lp").Namespace("default").Priority(lowPriorityVal).Obj(),
			},
			expectedFit:          false,
			expectedStatusReason: "node(s) had untolerated taint(s)",
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
			var cpgLister fwk.CompositePodGroupLister
			if tt.compositePodGroups != nil {
				cpgLister = &mockCompositePodGroupLister{compositePodGroups: tt.compositePodGroups}
			}

			fits, status := CanNodeFitPreemptorHeadroom(logger, tt.pod, nodeInfo, pgLister, cpgLister, false)
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
			v1.ResourceCPU:    "1024000m",
			v1.ResourceMemory: "4096000Mi",
			v1.ResourcePods:   "2000",
		}).Obj()

	gangSizes := []int{8, 64, 256, 1024}

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

// BenchmarkClusterScale evaluates Headroom Pruning performance across cluster scales (100 to 5,000 nodes)
// comparing the Coarse-Grained Headroom Pruning path against the baseline unpruned path.
func BenchmarkClusterScale(b *testing.B) {
	logger, _ := ktesting.NewTestContext(b)
	highPriorityVal := int32(1000)
	lowPriorityVal := int32(100)

	clusterSizes := []int{100, 500, 1000, 5000}

	for _, clusterSize := range clusterSizes {
		nodes := make([]fwk.NodeInfo, clusterSize)
		for i := 0; i < clusterSize; i++ {
			nodeObj := st.MakeNode().Name(fmt.Sprintf("node-%d", i)).
				Capacity(map[v1.ResourceName]string{
					v1.ResourceCPU:    "32000m",
					v1.ResourceMemory: "64000Mi",
					v1.ResourcePods:   "110",
				}).Obj()

			// 80% of nodes cannot fit the preemptor (insufficient headroom or untolerated taints)
			podsOnNode := 30
			existingPods := make([]*v1.Pod, podsOnNode)
			for p := 0; p < podsOnNode; p++ {
				prio := highPriorityVal
				if i%5 == 0 && p < 15 {
					// 20% of nodes have lower priority victims that can be preempted
					prio = lowPriorityVal
				}
				existingPods[p] = st.MakePod().Name(fmt.Sprintf("n%d-pod-%d", i, p)).Namespace("default").
					UID(fmt.Sprintf("uid-n%d-p%d", i, p)).
					Priority(prio).
					Req(map[v1.ResourceName]string{v1.ResourceCPU: "1000m", v1.ResourceMemory: "2000Mi"}).Obj()
			}
			ni := framework.NewNodeInfo(existingPods...)
			ni.SetNode(nodeObj)
			nodes[i] = ni
		}

		preemptor := st.MakePod().Name("preemptor").Namespace("default").
			UID("preemptor-uid").
			Priority(highPriorityVal).
			Req(map[v1.ResourceName]string{v1.ResourceCPU: "8000m", v1.ResourceMemory: "16000Mi"}).Obj()

		b.Run(fmt.Sprintf("HeadroomPruning_Nodes_%d", clusterSize), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				prunedCount := 0
				for _, ni := range nodes {
					fits, _ := CanNodeFitPreemptorHeadroom(logger, preemptor, ni, nil, nil, false)
					if !fits {
						prunedCount++
					}
				}
				if prunedCount == 0 {
					b.Fatalf("expected some nodes to be pruned")
				}
			}
		})
	}
}

// BenchmarkChurnStorm evaluates CPU and memory allocation under concurrent worker threads
// while candidate nodes undergo rapid capacity and pod state mutations.
func BenchmarkChurnStorm(b *testing.B) {
	logger, _ := ktesting.NewTestContext(b)
	highPriorityVal := int32(1000)
	lowPriorityVal := int32(100)

	const nodeCount = 200
	nodes := make([]fwk.NodeInfo, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodeObj := st.MakeNode().Name(fmt.Sprintf("churn-node-%d", i)).
			Capacity(map[v1.ResourceName]string{
				v1.ResourceCPU:    "64000m",
				v1.ResourceMemory: "128000Mi",
				v1.ResourcePods:   "110",
			}).Obj()

		existingPods := make([]*v1.Pod, 40)
		for p := 0; p < 40; p++ {
			prio := lowPriorityVal
			if p%2 == 0 {
				prio = highPriorityVal
			}
			existingPods[p] = st.MakePod().Name(fmt.Sprintf("c-n%d-p%d", i, p)).Namespace("default").
				UID(fmt.Sprintf("uid-c-n%d-p%d", i, p)).
				Priority(prio).
				Req(map[v1.ResourceName]string{v1.ResourceCPU: "1000m", v1.ResourceMemory: "2000Mi"}).Obj()
		}
		ni := framework.NewNodeInfo(existingPods...)
		ni.SetNode(nodeObj)
		nodes[i] = ni
	}

	preemptor := st.MakePod().Name("preemptor-churn").Namespace("default").
		UID("preemptor-churn-uid").
		Priority(highPriorityVal).
		Req(map[v1.ResourceName]string{v1.ResourceCPU: "8000m", v1.ResourceMemory: "16000Mi"}).Obj()

	var counter atomic.Int64
	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			idx := int(counter.Add(1)) % nodeCount
			ni := nodes[idx]
			_, _ = CanNodeFitPreemptorHeadroom(logger, preemptor, ni, nil, nil, false)
		}
	})
}

// BenchmarkPruneRatioEffectiveness measures prune ratio and speedup across
// Homogeneous clusters (uniform nodes) vs Fragmented Heterogeneous clusters (mixed hardware/taints/labels).
func BenchmarkPruneRatioEffectiveness(b *testing.B) {
	logger, _ := ktesting.NewTestContext(b)
	highPriorityVal := int32(1000)
	lowPriorityVal := int32(100)

	const totalNodes = 1000
	preemptor := st.MakePod().Name("preemptor").Namespace("default").
		UID("preemptor-uid").
		Priority(highPriorityVal).
		Req(map[v1.ResourceName]string{
			v1.ResourceCPU:                    "4000m",
			v1.ResourceMemory:                 "8000Mi",
			v1.ResourceName("nvidia.com/gpu"): "2",
		}).
		NodeSelector(map[string]string{"accelerator": "nvidia"}).Obj()

	// 1. Homogeneous cluster: all nodes have GPUs and matching labels
	homoNodes := make([]fwk.NodeInfo, totalNodes)
	for i := 0; i < totalNodes; i++ {
		nodeObj := st.MakeNode().Name(fmt.Sprintf("homo-node-%d", i)).
			Label("accelerator", "nvidia").
			Capacity(map[v1.ResourceName]string{
				v1.ResourceCPU:                    "32000m",
				v1.ResourceMemory:                 "64000Mi",
				v1.ResourceName("nvidia.com/gpu"): "4",
				v1.ResourcePods:                   "110",
			}).Obj()
		existingPods := []*v1.Pod{
			st.MakePod().Name(fmt.Sprintf("pod-%d", i)).Namespace("default").
				Priority(lowPriorityVal).
				Req(map[v1.ResourceName]string{
					v1.ResourceCPU:                    "10000m",
					v1.ResourceName("nvidia.com/gpu"): "2",
				}).Obj(),
		}
		ni := framework.NewNodeInfo(existingPods...)
		ni.SetNode(nodeObj)
		homoNodes[i] = ni
	}

	// 2. Heterogeneous cluster: only 10% nodes have nvidia GPU and matching label, 90% are CPU/TPU nodes or tainted
	heteroNodes := make([]fwk.NodeInfo, totalNodes)
	for i := 0; i < totalNodes; i++ {
		var nodeObj *v1.Node
		var existingPods []*v1.Pod
		if i%10 == 0 {
			// Schedulable GPU node
			nodeObj = st.MakeNode().Name(fmt.Sprintf("hetero-node-%d", i)).
				Label("accelerator", "nvidia").
				Capacity(map[v1.ResourceName]string{
					v1.ResourceCPU:                    "32000m",
					v1.ResourceMemory:                 "64000Mi",
					v1.ResourceName("nvidia.com/gpu"): "4",
					v1.ResourcePods:                   "110",
				}).Obj()
			existingPods = []*v1.Pod{
				st.MakePod().Name(fmt.Sprintf("pod-%d", i)).Namespace("default").
					Priority(lowPriorityVal).
					Req(map[v1.ResourceName]string{v1.ResourceName("nvidia.com/gpu"): "2"}).Obj(),
			}
		} else if i%3 == 0 {
			// CPU node with untolerated taint
			nodeObj = st.MakeNode().Name(fmt.Sprintf("hetero-node-%d", i)).
				Label("accelerator", "cpu").
				Taints([]v1.Taint{{Key: "dedicated", Value: "cpu-only", Effect: v1.TaintEffectNoSchedule}}).
				Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "32000m", v1.ResourceMemory: "64000Mi", v1.ResourcePods: "110"}).Obj()
		} else {
			// Standard CPU node lacking GPU
			nodeObj = st.MakeNode().Name(fmt.Sprintf("hetero-node-%d", i)).
				Label("accelerator", "cpu").
				Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "32000m", v1.ResourceMemory: "64000Mi", v1.ResourcePods: "110"}).Obj()
		}
		ni := framework.NewNodeInfo(existingPods...)
		ni.SetNode(nodeObj)
		heteroNodes[i] = ni
	}

	b.Run("Homogeneous_Cluster_PruneRatio", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			pruned := 0
			for _, ni := range homoNodes {
				fits, _ := CanNodeFitPreemptorHeadroom(logger, preemptor, ni, nil, nil, false)
				if !fits {
					pruned++
				}
			}
		}
	})

	b.Run("Heterogeneous_Cluster_PruneRatio", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			pruned := 0
			for _, ni := range heteroNodes {
				fits, _ := CanNodeFitPreemptorHeadroom(logger, preemptor, ni, nil, nil, false)
				if !fits {
					pruned++
				}
			}
			if pruned < 850 {
				b.Fatalf("expected >= 85%% prune ratio in heterogeneous cluster, got %d", pruned)
			}
		}
	})
}
