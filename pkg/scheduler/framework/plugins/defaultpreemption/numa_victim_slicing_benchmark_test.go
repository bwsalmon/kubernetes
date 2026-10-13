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

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
)

// BenchmarkNUMATopologyAligned_8GPU_DualSocket benchmarks Idea 12 (NUMA-Topology-Aligned Hardware
// Victim Slicing) against the unaligned baseline on an 8-GPU dual-socket multi-accelerator node
// across varied pod densities (8, 16, 32, and 64 accelerator workloads).
func BenchmarkNUMATopologyAligned_8GPU_DualSocket(b *testing.B) {
	podCounts := []int{8, 16, 32, 64}

	for _, count := range podCounts {
		node := st.MakeNode().Name(fmt.Sprintf("bench-node-8gpu-%d", count)).Capacity(map[v1.ResourceName]string{
			v1.ResourcePods:   "200",
			v1.ResourceCPU:    "128000m",
			v1.ResourceMemory: "512000Mi",
			ResourceNVIDIAGPU: "8",
		}).Obj()

		baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		var initPods []*v1.Pod

		// Distribute pods evenly across Socket 0 and Socket 1.
		for i := 0; i < count; i++ {
			socketID := i % 2
			podName := fmt.Sprintf("bench-pod-%03d", i)
			stTime := metav1.NewTime(baseTime.Add(time.Duration(i) * time.Minute))
			p := st.MakePod().Name(podName).
				UID(fmt.Sprintf("bench-uid-%03d", i)).
				Node(node.Name).
				Priority(100).
				StartTime(stTime).
				Annotation(AnnotationNUMANode, fmt.Sprintf("%d", socketID)).
				Req(map[v1.ResourceName]string{
					v1.ResourceCPU:    "1000m",
					v1.ResourceMemory: "4000Mi",
					ResourceNVIDIAGPU: "1",
				}).Obj()
			initPods = append(initPods, p)
		}

		// Preemptor requires 4 GPUs under SingleNUMANode policy.
		preemptor := st.MakePod().Name("bench-preemptor").
			UID("bench-preemptor-uid").
			Priority(1000).
			Annotation(AnnotationTopologyPolicy, PolicySingleNumaNode).
			Req(map[v1.ResourceName]string{
				v1.ResourceCPU:    "4000m",
				v1.ResourceMemory: "16000Mi",
				ResourceNVIDIAGPU: "4",
			}).Obj()

		pl, _, nodeInfo, potentialVictims, cycleState := setupHighDensityTestEnv(&testing.T{}, node, initPods, preemptor, nil)

		// Set cached node hardware topology
		topo := &NodeHardwareTopology{
			NodeName:   node.Name,
			NumSockets: 2,
			Domains: []HardwareLocalityDomain{
				{SocketID: 0, DomainID: "socket-0"},
				{SocketID: 1, DomainID: "socket-1"},
			},
			DomainAllocatable: map[string]map[v1.ResourceName]int64{
				"socket-0": {
					v1.ResourceCPU:    64000,
					v1.ResourceMemory: 256000 * 1024 * 1024,
					ResourceNVIDIAGPU: 4,
				},
				"socket-1": {
					v1.ResourceCPU:    64000,
					v1.ResourceMemory: 256000 * 1024 * 1024,
					ResourceNVIDIAGPU: 4,
				},
			},
		}
		pl.TopologyCache.SetNodeTopology(node.Name, topo)

		ctx := context.Background()

		b.Run(fmt.Sprintf("Idea12_AlignedSlicing_Pods_%d", count), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				cState := cycleState.Clone()
				nInfo := nodeInfo.Snapshot()
				victims, _, status := pl.SelectVictimsOnNode(ctx, cState, preemptor, nInfo, potentialVictims, nil)
				if !status.IsSuccess() || len(victims) == 0 {
					b.Fatalf("Aligned selection failed: status=%v, victims=%d", status, len(victims))
				}
			}
		})

		b.Run(fmt.Sprintf("Baseline_Unaligned_Pods_%d", count), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				cState := cycleState.Clone()
				nInfo := nodeInfo.Snapshot()
				victims, _, status := unbucketedSelectVictimsOnNode(ctx, pl, cState, preemptor, nInfo, potentialVictims, nil)
				if !status.IsSuccess() || len(victims) == 0 {
					b.Fatalf("Baseline selection failed: status=%v, victims=%d", status, len(victims))
				}
			}
		})
	}
}

// BenchmarkNUMATopologyAligned_PCIeSwitch benchmarks PCIe switch-aligned victim slicing
// on an 8-GPU node partitioned into 4 PCIe switches (2 switches per socket, 2 GPUs per switch).
func BenchmarkNUMATopologyAligned_PCIeSwitch(b *testing.B) {
	node := st.MakeNode().Name("bench-node-pcie-8gpu").Capacity(map[v1.ResourceName]string{
		v1.ResourcePods:   "100",
		v1.ResourceCPU:    "64000m",
		v1.ResourceMemory: "256000Mi",
		ResourceNVIDIAGPU: "8",
	}).Obj()

	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var initPods []*v1.Pod

	// 8 pods on node across 4 switches: socket-0/switch-0, socket-0/switch-1, socket-1/switch-2, socket-1/switch-3.
	for i := 0; i < 8; i++ {
		socketID := i / 4
		switchID := fmt.Sprintf("sw-%d", i/2)
		podName := fmt.Sprintf("pcie-bench-pod-%d", i)
		stTime := metav1.NewTime(baseTime.Add(time.Duration(i) * time.Minute))
		p := st.MakePod().Name(podName).
			UID(fmt.Sprintf("pcie-uid-%d", i)).
			Node(node.Name).
			Priority(100).
			StartTime(stTime).
			Annotation(AnnotationNUMANode, fmt.Sprintf("%d", socketID)).
			Annotation(AnnotationPCIeSwitch, switchID).
			Req(map[v1.ResourceName]string{
				v1.ResourceCPU:    "2000m",
				v1.ResourceMemory: "8000Mi",
				ResourceNVIDIAGPU: "1",
			}).Obj()
		initPods = append(initPods, p)
	}

	preemptor := st.MakePod().Name("pcie-preemptor").
		UID("pcie-preemptor-uid").
		Priority(1000).
		Annotation(AnnotationTopologyPolicy, PolicySingleNumaNode).
		Annotation(AnnotationPCIeSwitchAlignment, "true").
		Req(map[v1.ResourceName]string{
			v1.ResourceCPU:    "4000m",
			v1.ResourceMemory: "16000Mi",
			ResourceNVIDIAGPU: "2",
		}).Obj()

	pl, _, nodeInfo, potentialVictims, cycleState := setupHighDensityTestEnv(&testing.T{}, node, initPods, preemptor, nil)
	ctx := context.Background()

	b.Run("Idea12_PCIeSwitchAligned", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cState := cycleState.Clone()
			nInfo := nodeInfo.Snapshot()
			victims, _, status := pl.SelectVictimsOnNode(ctx, cState, preemptor, nInfo, potentialVictims, nil)
			if !status.IsSuccess() || len(victims) != 2 {
				b.Fatalf("PCIe aligned selection failed: status=%v, victims=%d", status, len(victims))
			}
		}
	})

	b.Run("Baseline_Unaligned", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cState := cycleState.Clone()
			nInfo := nodeInfo.Snapshot()
			victims, _, status := unbucketedSelectVictimsOnNode(ctx, pl, cState, preemptor, nInfo, potentialVictims, nil)
			if !status.IsSuccess() || len(victims) != 2 {
				b.Fatalf("Baseline selection failed: status=%v, victims=%d", status, len(victims))
			}
		}
	})
}
