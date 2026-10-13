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
	policy "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
)

// TestNUMATopologyAligned_MultiGPUFragmentationPrevention verifies that on an 8-GPU dual-socket node,
// when a preemptor requests 4 GPUs under SingleNUMANode policy, candidate victims are sliced by
// NUMA socket so that all evicted victims originate from a single NUMA domain, preventing multi-GPU fragmentation.
func TestNUMATopologyAligned_MultiGPUFragmentationPrevention(t *testing.T) {
	ctx := context.Background()
	node := st.MakeNode().Name("node-8gpu-dual-socket").Capacity(map[v1.ResourceName]string{
		v1.ResourcePods:   "100",
		v1.ResourceCPU:    "64000m",
		v1.ResourceMemory: "256000Mi",
		ResourceNVIDIAGPU: "8",
	}).Obj()

	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var initPods []*v1.Pod

	// 8 running pods on the node, each consuming 1 GPU:
	// - Pods 0..3 on Socket 0 (NUMA 0)
	// - Pods 4..7 on Socket 1 (NUMA 1)
	for i := 0; i < 8; i++ {
		socketID := i / 4
		podName := fmt.Sprintf("gpu-pod-%d", i)
		stTime := metav1.NewTime(baseTime.Add(time.Duration(i) * time.Minute))
		p := st.MakePod().Name(podName).
			UID(fmt.Sprintf("uid-gpu-pod-%d", i)).
			Node("node-8gpu-dual-socket").
			Priority(100).
			StartTime(stTime).
			Annotation(AnnotationNUMANode, fmt.Sprintf("%d", socketID)).
			Req(map[v1.ResourceName]string{
				v1.ResourceCPU:    "2000m",
				v1.ResourceMemory: "8000Mi",
				ResourceNVIDIAGPU: "1",
			}).Obj()
		initPods = append(initPods, p)
	}

	// Preemptor requires 4 GPUs with SingleNUMANode policy.
	preemptor := st.MakePod().Name("preemptor-4gpu-singlenuma").
		UID("preemptor-uid").
		Priority(1000).
		Annotation(AnnotationTopologyPolicy, PolicySingleNumaNode).
		Req(map[v1.ResourceName]string{
			v1.ResourceCPU:    "8000m",
			v1.ResourceMemory: "32000Mi",
			ResourceNVIDIAGPU: "4",
		}).Obj()

	pl, _, nodeInfo, potentialVictims, cycleState := setupHighDensityTestEnv(t, node, initPods, preemptor, nil)

	// Configure per-domain topology in cache for node-8gpu-dual-socket.
	topo := &NodeHardwareTopology{
		NodeName:   "node-8gpu-dual-socket",
		NumSockets: 2,
		Domains: []HardwareLocalityDomain{
			{SocketID: 0, DomainID: "socket-0"},
			{SocketID: 1, DomainID: "socket-1"},
		},
		DomainAllocatable: map[string]map[v1.ResourceName]int64{
			"socket-0": {
				v1.ResourceCPU:    32000,
				v1.ResourceMemory: 128000 * 1024 * 1024,
				ResourceNVIDIAGPU: 4,
			},
			"socket-1": {
				v1.ResourceCPU:    32000,
				v1.ResourceMemory: 128000 * 1024 * 1024,
				ResourceNVIDIAGPU: 4,
			},
		},
	}
	pl.TopologyCache.SetNodeTopology("node-8gpu-dual-socket", topo)

	victimPods, numViolating, status := pl.SelectVictimsOnNode(
		ctx, cycleState.Clone(), preemptor, nodeInfo.Snapshot(), potentialVictims, nil,
	)
	if !status.IsSuccess() {
		t.Fatalf("SelectVictimsOnNode failed: %v", status)
	}

	if len(victimPods) != 4 {
		t.Fatalf("Expected exactly 4 victim pods, got %d", len(victimPods))
	}
	if numViolating != 0 {
		t.Errorf("Expected 0 PDB violations, got %d", numViolating)
	}

	// Verify that ALL 4 evicted pods originate from the EXACT SAME NUMA socket,
	// preventing cross-socket fragmentation!
	firstSocket := victimPods[0].Annotations[AnnotationNUMANode]
	for _, p := range victimPods {
		sock := p.Annotations[AnnotationNUMANode]
		if sock != firstSocket {
			t.Errorf("Victim pod %s is on socket %s, expected all victims to be on socket %s to prevent fragmentation",
				p.Name, sock, firstSocket)
		}
	}
}

// TestNUMATopologyAligned_StrictPriorityPreservation verifies that when choosing between multiple
// feasible NUMA sockets, the selector strictly prioritizes the socket with the lowest preemption cost
// (minimal highest victim priority), protecting higher-priority workloads on other sockets.
func TestNUMATopologyAligned_StrictPriorityPreservation(t *testing.T) {
	ctx := context.Background()
	node := st.MakeNode().Name("node-8gpu-prio").Capacity(map[v1.ResourceName]string{
		v1.ResourcePods:   "100",
		v1.ResourceCPU:    "64000m",
		v1.ResourceMemory: "256000Mi",
		ResourceNVIDIAGPU: "8",
	}).Obj()

	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var initPods []*v1.Pod

	// Socket 0 has 4 pods with priority 500 (Production workloads).
	// Socket 1 has 4 pods with priority 100 (Batch workloads).
	for i := 0; i < 8; i++ {
		socketID := i / 4
		var prio int32 = 100
		if socketID == 0 {
			prio = 500
		}
		podName := fmt.Sprintf("prio-pod-%d", i)
		stTime := metav1.NewTime(baseTime.Add(time.Duration(i) * time.Minute))
		p := st.MakePod().Name(podName).
			UID(fmt.Sprintf("uid-prio-pod-%d", i)).
			Node("node-8gpu-prio").
			Priority(prio).
			StartTime(stTime).
			Annotation(AnnotationNUMANode, fmt.Sprintf("%d", socketID)).
			Req(map[v1.ResourceName]string{
				v1.ResourceCPU:    "2000m",
				v1.ResourceMemory: "8000Mi",
				ResourceNVIDIAGPU: "1",
			}).Obj()
		initPods = append(initPods, p)
	}

	// Preemptor needs 4 GPUs with SingleNUMANode.
	preemptor := st.MakePod().Name("preemptor-prio-check").
		UID("preemptor-uid").
		Priority(1000).
		Annotation(AnnotationTopologyPolicy, PolicySingleNumaNode).
		Req(map[v1.ResourceName]string{
			v1.ResourceCPU:    "8000m",
			v1.ResourceMemory: "32000Mi",
			ResourceNVIDIAGPU: "4",
		}).Obj()

	pl, _, nodeInfo, potentialVictims, cycleState := setupHighDensityTestEnv(t, node, initPods, preemptor, nil)

	victimPods, _, status := pl.SelectVictimsOnNode(
		ctx, cycleState.Clone(), preemptor, nodeInfo.Snapshot(), potentialVictims, nil,
	)
	if !status.IsSuccess() {
		t.Fatalf("SelectVictimsOnNode failed: %v", status)
	}

	if len(victimPods) != 4 {
		t.Fatalf("Expected 4 victim pods, got %d", len(victimPods))
	}

	// Verify that Socket 1 (batch workloads with priority 100) was chosen,
	// preserving priority-500 workloads on Socket 0!
	for _, p := range victimPods {
		if *p.Spec.Priority != 100 {
			t.Errorf("Expected evicted pod %s to have priority 100, got %d", p.Name, *p.Spec.Priority)
		}
		if sock := p.Annotations[AnnotationNUMANode]; sock != "1" {
			t.Errorf("Expected evicted pod %s to be on socket 1, got %s", p.Name, sock)
		}
	}
}

// TestNUMATopologyAligned_PDBMinimizationInvariant verifies that when comparing NUMA sockets,
// the selector strictly avoids PDB-violating sockets (0 PDB violations strictly prioritized),
// preserving the scheduler's PDB minimization invariant.
func TestNUMATopologyAligned_PDBMinimizationInvariant(t *testing.T) {
	ctx := context.Background()
	node := st.MakeNode().Name("node-8gpu-pdb").Capacity(map[v1.ResourceName]string{
		v1.ResourcePods:   "100",
		v1.ResourceCPU:    "64000m",
		v1.ResourceMemory: "256000Mi",
		ResourceNVIDIAGPU: "8",
	}).Obj()

	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var initPods []*v1.Pod

	// Socket 0 has 4 pods with priority 100. Pod 0 is covered by a strict PDB (DisruptionsAllowed = 0).
	// Socket 1 has 4 pods with priority 100. No PDB violations.
	for i := 0; i < 8; i++ {
		socketID := i / 4
		podName := fmt.Sprintf("pdb-pod-%d", i)
		stTime := metav1.NewTime(baseTime.Add(time.Duration(i) * time.Minute))
		labels := map[string]string{"app": "worker"}
		if i == 0 {
			labels = map[string]string{"app": "protected-pdb"}
		}
		p := st.MakePod().Namespace("default").Name(podName).
			UID(fmt.Sprintf("uid-pdb-pod-%d", i)).
			Node("node-8gpu-pdb").
			Priority(100).
			StartTime(stTime).
			Labels(labels).
			Annotation(AnnotationNUMANode, fmt.Sprintf("%d", socketID)).
			Req(map[v1.ResourceName]string{
				v1.ResourceCPU:    "2000m",
				v1.ResourceMemory: "8000Mi",
				ResourceNVIDIAGPU: "1",
			}).Obj()
		initPods = append(initPods, p)
	}

	pdb := &policy.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "strict-pdb",
			Namespace: "default",
		},
		Spec: policy.PodDisruptionBudgetSpec{
			MinAvailable: &intstr.IntOrString{Type: intstr.Int, IntVal: 1},
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "protected-pdb"},
			},
		},
		Status: policy.PodDisruptionBudgetStatus{
			CurrentHealthy:        1,
			DesiredHealthy:        1,
			DisruptionsAllowed:    0, // Any preemption of this pod violates PDB!
			ExpectedPods:          1,
			DisruptedPods:         map[string]metav1.Time{},
			ObservedGeneration:    1,
		},
	}

	preemptor := st.MakePod().Namespace("default").Name("preemptor-pdb-check").
		UID("preemptor-uid").
		Priority(1000).
		Annotation(AnnotationTopologyPolicy, PolicySingleNumaNode).
		Req(map[v1.ResourceName]string{
			v1.ResourceCPU:    "8000m",
			v1.ResourceMemory: "32000Mi",
			ResourceNVIDIAGPU: "4",
		}).Obj()

	pl, _, nodeInfo, potentialVictims, cycleState := setupHighDensityTestEnv(t, node, initPods, preemptor, []*policy.PodDisruptionBudget{pdb})

	victimPods, numViolating, status := pl.SelectVictimsOnNode(
		ctx, cycleState.Clone(), preemptor, nodeInfo.Snapshot(), potentialVictims, []*policy.PodDisruptionBudget{pdb},
	)
	if !status.IsSuccess() {
		t.Fatalf("SelectVictimsOnNode failed: %v", status)
	}

	if numViolating != 0 {
		t.Fatalf("Expected 0 PDB violations, got %d", numViolating)
	}
	if len(victimPods) != 4 {
		t.Fatalf("Expected 4 victim pods, got %d", len(victimPods))
	}

	// Verify that Socket 1 was chosen because Socket 0 would cause a PDB violation!
	for _, p := range victimPods {
		if sock := p.Annotations[AnnotationNUMANode]; sock != "1" {
			t.Errorf("Expected victim %s to be on socket 1, got socket %s", p.Name, sock)
		}
	}
}

// TestNUMATopologyAligned_PCIeSwitchLocality verifies fine-grained victim slicing across
// PCIe switches within a NUMA socket.
func TestNUMATopologyAligned_PCIeSwitchLocality(t *testing.T) {
	ctx := context.Background()
	node := st.MakeNode().Name("node-pcie").Capacity(map[v1.ResourceName]string{
		v1.ResourcePods:   "100",
		v1.ResourceCPU:    "64000m",
		v1.ResourceMemory: "256000Mi",
		ResourceNVIDIAGPU: "8",
	}).Obj()

	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var initPods []*v1.Pod

	// Socket 0 has 4 GPUs partitioned into 2 PCIe switches:
	// - Switch 0: Pods 0, 1 (prio 100)
	// - Switch 1: Pods 2, 3 (prio 300)
	// Socket 1 has 4 GPUs (prio 400).
	for i := 0; i < 8; i++ {
		socketID := i / 4
		pcieSwitch := ""
		prio := int32(400)
		if socketID == 0 {
			switchIdx := (i % 4) / 2
			pcieSwitch = fmt.Sprintf("pcie-%d", switchIdx)
			if switchIdx == 0 {
				prio = 100
			} else {
				prio = 300
			}
		}
		podName := fmt.Sprintf("pcie-pod-%d", i)
		stTime := metav1.NewTime(baseTime.Add(time.Duration(i) * time.Minute))
		p := st.MakePod().Name(podName).
			UID(fmt.Sprintf("uid-pcie-pod-%d", i)).
			Node("node-pcie").
			Priority(prio).
			StartTime(stTime).
			Annotation(AnnotationNUMANode, fmt.Sprintf("%d", socketID)).
			Annotation(AnnotationPCIeSwitch, pcieSwitch).
			Req(map[v1.ResourceName]string{
				v1.ResourceCPU:    "2000m",
				v1.ResourceMemory: "8000Mi",
				ResourceNVIDIAGPU: "1",
			}).Obj()
		initPods = append(initPods, p)
	}

	// Preemptor needs 2 GPUs with PCIe switch alignment.
	preemptor := st.MakePod().Name("preemptor-2gpu-pcie").
		UID("preemptor-uid").
		Priority(1000).
		Annotation(AnnotationTopologyPolicy, PolicySingleNumaNode).
		Annotation(AnnotationPCIeSwitchAlignment, "true").
		Req(map[v1.ResourceName]string{
			v1.ResourceCPU:    "4000m",
			v1.ResourceMemory: "16000Mi",
			ResourceNVIDIAGPU: "2",
		}).Obj()

	pl, _, nodeInfo, potentialVictims, cycleState := setupHighDensityTestEnv(t, node, initPods, preemptor, nil)

	victimPods, _, status := pl.SelectVictimsOnNode(
		ctx, cycleState.Clone(), preemptor, nodeInfo.Snapshot(), potentialVictims, nil,
	)
	if !status.IsSuccess() {
		t.Fatalf("SelectVictimsOnNode failed: %v", status)
	}

	if len(victimPods) != 2 {
		t.Fatalf("Expected 2 victim pods, got %d", len(victimPods))
	}

	// Verify that victims are strictly from Switch 0 ("pcie-0") with priority 100.
	for _, p := range victimPods {
		if sw := p.Annotations[AnnotationPCIeSwitch]; sw != "pcie-0" {
			t.Errorf("Expected victim %s to be on PCIe switch pcie-0, got %s", p.Name, sw)
		}
		if *p.Spec.Priority != 100 {
			t.Errorf("Expected victim %s priority to be 100, got %d", p.Name, *p.Spec.Priority)
		}
	}
}

// TestNUMATopologyAligned_CrossSocketFallback verifies that when a pod requests more contiguous
// resources than any single NUMA socket can provide, the heuristic cleanly falls back to
// cluster-wide preemption across sockets without failing or hanging.
func TestNUMATopologyAligned_CrossSocketFallback(t *testing.T) {
	ctx := context.Background()
	node := st.MakeNode().Name("node-fallback").Capacity(map[v1.ResourceName]string{
		v1.ResourcePods:   "100",
		v1.ResourceCPU:    "64000m",
		v1.ResourceMemory: "256000Mi",
		ResourceNVIDIAGPU: "8",
	}).Obj()

	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var initPods []*v1.Pod

	// 8 pods on node (4 on Socket 0, 4 on Socket 1).
	for i := 0; i < 8; i++ {
		socketID := i / 4
		podName := fmt.Sprintf("fallback-pod-%d", i)
		stTime := metav1.NewTime(baseTime.Add(time.Duration(i) * time.Minute))
		p := st.MakePod().Name(podName).
			UID(fmt.Sprintf("uid-fb-pod-%d", i)).
			Node("node-fallback").
			Priority(100).
			StartTime(stTime).
			Annotation(AnnotationNUMANode, fmt.Sprintf("%d", socketID)).
			Req(map[v1.ResourceName]string{
				v1.ResourceCPU:    "2000m",
				v1.ResourceMemory: "8000Mi",
				ResourceNVIDIAGPU: "1",
			}).Obj()
		initPods = append(initPods, p)
	}

	// Preemptor requests 6 GPUs with SingleNUMANode policy.
	// Since each socket has only 4 GPUs, neither socket can satisfy 6 GPUs individually.
	// Cross-socket fallback must activate!
	preemptor := st.MakePod().Name("preemptor-6gpu").
		UID("preemptor-uid").
		Priority(1000).
		Annotation(AnnotationTopologyPolicy, PolicySingleNumaNode).
		Req(map[v1.ResourceName]string{
			v1.ResourceCPU:    "12000m",
			v1.ResourceMemory: "48000Mi",
			ResourceNVIDIAGPU: "6",
		}).Obj()

	pl, _, nodeInfo, potentialVictims, cycleState := setupHighDensityTestEnv(t, node, initPods, preemptor, nil)

	victimPods, _, status := pl.SelectVictimsOnNode(
		ctx, cycleState.Clone(), preemptor, nodeInfo.Snapshot(), potentialVictims, nil,
	)
	if !status.IsSuccess() {
		t.Fatalf("SelectVictimsOnNode failed: %v", status)
	}

	if len(victimPods) != 6 {
		t.Fatalf("Expected 6 victim pods from cross-socket fallback, got %d", len(victimPods))
	}
}

// TestNUMATopologyAligned_PolicyNotRequired verifies that pods without SingleNUMANode policy
// bypass NUMA-specific filtering and use standard cluster-wide victim selection.
func TestNUMATopologyAligned_PolicyNotRequired(t *testing.T) {
	ctx := context.Background()
	node := st.MakeNode().Name("node-nopolicy").Capacity(map[v1.ResourceName]string{
		v1.ResourcePods:   "100",
		v1.ResourceCPU:    "64000m",
		v1.ResourceMemory: "256000Mi",
		ResourceNVIDIAGPU: "8",
	}).Obj()

	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var initPods []*v1.Pod

	// 8 pods on node (4 on Socket 0, 4 on Socket 1).
	for i := 0; i < 8; i++ {
		socketID := i / 4
		podName := fmt.Sprintf("nopolicy-pod-%d", i)
		stTime := metav1.NewTime(baseTime.Add(time.Duration(i) * time.Minute))
		p := st.MakePod().Name(podName).
			UID(fmt.Sprintf("uid-np-pod-%d", i)).
			Node("node-nopolicy").
			Priority(100).
			StartTime(stTime).
			Annotation(AnnotationNUMANode, fmt.Sprintf("%d", socketID)).
			Req(map[v1.ResourceName]string{
				v1.ResourceCPU:    "2000m",
				v1.ResourceMemory: "8000Mi",
				ResourceNVIDIAGPU: "1",
			}).Obj()
		initPods = append(initPods, p)
	}

	// Preemptor requests 4 GPUs without SingleNUMANode policy.
	preemptor := st.MakePod().Name("preemptor-standard").
		UID("preemptor-uid").
		Priority(1000).
		Req(map[v1.ResourceName]string{
			v1.ResourceCPU:    "8000m",
			v1.ResourceMemory: "32000Mi",
			ResourceNVIDIAGPU: "4",
		}).Obj()

	pl, _, nodeInfo, potentialVictims, cycleState := setupHighDensityTestEnv(t, node, initPods, preemptor, nil)

	victimPods, _, status := pl.SelectVictimsOnNode(
		ctx, cycleState.Clone(), preemptor, nodeInfo.Snapshot(), potentialVictims, nil,
	)
	if !status.IsSuccess() {
		t.Fatalf("SelectVictimsOnNode failed: %v", status)
	}

	if len(victimPods) != 4 {
		t.Fatalf("Expected 4 victim pods, got %d", len(victimPods))
	}
}
