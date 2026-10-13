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
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/dynamicresources"
	schedulertesting "k8s.io/kubernetes/pkg/scheduler/testing"
)

func setupHeterogeneousCluster(nodeCount int) ([]fwk.NodeInfo, *testSharedDRAManager, *v1.Pod) {
	classH100 := &resourceapi.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{Name: "nvidia-h100"},
		Spec: resourceapi.DeviceClassSpec{
			Config: []resourceapi.DeviceClassConfiguration{
				{
					DeviceConfiguration: resourceapi.DeviceConfiguration{
						Opaque: &resourceapi.OpaqueDeviceConfiguration{Driver: "gpu.nvidia.com"},
					},
				},
			},
		},
	}
	classMI300 := &resourceapi.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{Name: "amd-mi300"},
		Spec: resourceapi.DeviceClassSpec{
			Config: []resourceapi.DeviceClassConfiguration{
				{
					DeviceConfiguration: resourceapi.DeviceConfiguration{
						Opaque: &resourceapi.OpaqueDeviceConfiguration{Driver: "amd.com/gpu"},
					},
				},
			},
		},
	}
	classGaudi := &resourceapi.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{Name: "intel-gaudi"},
		Spec: resourceapi.DeviceClassSpec{
			Config: []resourceapi.DeviceClassConfiguration{
				{
					DeviceConfiguration: resourceapi.DeviceConfiguration{
						Opaque: &resourceapi.OpaqueDeviceConfiguration{Driver: "gaudi.intel.com"},
					},
				},
			},
		},
	}

	preemptor := makePodWithClaim("preemptor-h100", "default", 1000, "claim-h100")
	claimH100 := makeDRAClaimWithCount("claim-h100", "default", "nvidia-h100", 8)

	claims := map[string]*resourceapi.ResourceClaim{
		"default/claim-h100": claimH100,
	}
	classes := map[string]*resourceapi.DeviceClass{
		"nvidia-h100": classH100,
		"amd-mi300":   classMI300,
		"intel-gaudi": classGaudi,
	}

	var slices []*resourceapi.ResourceSlice
	nodeInfos := make([]fwk.NodeInfo, nodeCount)

	for i := 0; i < nodeCount; i++ {
		nodeName := fmt.Sprintf("node-%d", i)
		node := schedulertesting.MakeNode().Name(nodeName).Capacity(map[v1.ResourceName]string{
			v1.ResourceCPU:    "64",
			v1.ResourceMemory: "256Gi",
		}).Obj()
		ni := framework.NewNodeInfo()
		ni.SetNode(node)
		nodeInfos[i] = ni

		// Heterogeneous distribution:
		// 0: NVIDIA H100 (8 GPUs) - Feasible
		// 1: AMD MI300 (8 GPUs) - Incompatible driver
		// 2: Intel Gaudi (8 Gaudi) - Incompatible driver
		// 3: CPU-only (0 devices) - Incompatible
		// 4: NVIDIA H100 (4 GPUs) - Insufficient devices
		switch i % 5 {
		case 0:
			s := makeDeviceSlice("gpu.nvidia.com", "h100", ptrString(nodeName), nil, 8)
			slices = append(slices, s)
		case 1:
			s := makeDeviceSlice("amd.com/gpu", "mi300", ptrString(nodeName), nil, 8)
			slices = append(slices, s)
		case 2:
			s := makeDeviceSlice("gaudi.intel.com", "gaudi3", ptrString(nodeName), nil, 8)
			slices = append(slices, s)
		case 3:
			// CPU-only: no slice
		case 4:
			s := makeDeviceSlice("gpu.nvidia.com", "h100", ptrString(nodeName), nil, 4)
			slices = append(slices, s)
		}
	}

	mgr := &testSharedDRAManager{
		claimsTracker: &testDRAResourceClaimsTracker{claims: claims},
		classLister:   &testDRADeviceClassLister{classes: classes},
		sliceLister:   &testDRAResourceSliceLister{slices: slices},
	}

	return nodeInfos, mgr, preemptor
}

func BenchmarkDRAPreFilter_CacheHit(b *testing.B) {
	logger := klog.Background()
	nodeInfos, mgr, preemptor := setupHeterogeneousCluster(1)
	nodeInfo := nodeInfos[0]

	state := framework.NewCycleState()
	pgState := framework.NewCycleState()
	state.SetPodGroupCycleState(pgState)

	// Prime the cache
	_, _ = DRADevicePoolPreFilter(logger, state, preemptor, nodeInfo, mgr, nil, nil)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		fit, _ := DRADevicePoolPreFilter(logger, state, preemptor, nodeInfo, mgr, nil, nil)
		if !fit {
			b.Fatalf("expected fit=true on cached node")
		}
	}
}

func BenchmarkDRAPreFilter_ColdEvaluation(b *testing.B) {
	logger := klog.Background()
	nodeInfos, mgr, preemptor := setupHeterogeneousCluster(1)
	nodeInfo := nodeInfos[0]

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// New unprimed state each iteration
		state := framework.NewCycleState()
		fit, _ := DRADevicePoolPreFilter(logger, state, preemptor, nodeInfo, mgr, nil, nil)
		if !fit {
			b.Fatalf("expected fit=true")
		}
	}
}

func BenchmarkDRAPreFilter_HeterogeneousCluster(b *testing.B) {
	clusterSizes := []int{100, 500, 1000}
	for _, size := range clusterSizes {
		b.Run(fmt.Sprintf("Nodes_%d", size), func(b *testing.B) {
			logger := klog.Background()
			nodeInfos, mgr, preemptor := setupHeterogeneousCluster(size)

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				state := framework.NewCycleState()
				pgState := framework.NewCycleState()
				state.SetPodGroupCycleState(pgState)

				prunedCount := 0
				for _, ni := range nodeInfos {
					fit, _ := DRADevicePoolPreFilter(logger, state, preemptor, ni, mgr, nil, nil)
					if !fit {
						prunedCount++
					}
				}
				// 4 out of 5 nodes should be pruned (80% pruning rate)
				expectedPruned := size * 4 / 5
				if prunedCount != expectedPruned {
					b.Fatalf("expected %d pruned nodes, got %d", expectedPruned, prunedCount)
				}
			}
		})
	}
}

func BenchmarkDRAPreFilter_GangTemplateReuseAcrossPods(b *testing.B) {
	gangSizes := []int{8, 32, 64}
	for _, gangSize := range gangSizes {
		b.Run(fmt.Sprintf("Gang_%d_Pods_100_Nodes", gangSize), func(b *testing.B) {
			logger := klog.Background()
			nodeInfos, mgr, _ := setupHeterogeneousCluster(100)

			// Pre-construct gang pods
			gangPods := make([]*v1.Pod, gangSize)
			for p := 0; p < gangSize; p++ {
				gangPods[p] = makePodWithClaim(fmt.Sprintf("gang-pod-%d", p), "default", 1000, "claim-h100")
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				pgState := framework.NewCycleState()

				for _, pod := range gangPods {
					podCycleState := framework.NewCycleState()
					podCycleState.SetPodGroupCycleState(pgState)

					for _, ni := range nodeInfos {
						_, _ = DRADevicePoolPreFilter(logger, podCycleState, pod, ni, mgr, nil, nil)
					}
				}

				cache := dynamicresources.GetOrCreateClaimTemplateDriverCache(pgState)
				hits, misses, _ := cache.Stats()
				expectedMisses := int64(100) // only the first pod incurs misses across 100 nodes
				expectedHits := int64((gangSize - 1) * 100)
				if misses != expectedMisses || hits != expectedHits {
					b.Fatalf("expected %d misses and %d hits, got misses=%d hits=%d", expectedMisses, expectedHits, misses, hits)
				}
			}
		})
	}
}
