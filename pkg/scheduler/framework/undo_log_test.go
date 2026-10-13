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

package framework

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	"k8s.io/kubernetes/test/utils/ktesting"
)

func makeTestPod(name, uid string, cpu, mem string, hostPorts ...int32) *v1.Pod {
	var ports []v1.ContainerPort
	for _, p := range hostPorts {
		ports = append(ports, v1.ContainerPort{
			HostPort:      p,
			ContainerPort: p,
			Protocol:      v1.ProtocolTCP,
		})
	}
	pod := st.MakePod().Name(name).UID(uid).Node("test-node").Req(map[v1.ResourceName]string{
		v1.ResourceCPU:    cpu,
		v1.ResourceMemory: mem,
	}).Obj()
	if len(ports) > 0 {
		pod.Spec.Containers[0].Ports = ports
	}
	return pod
}

// TestUndoLogRollbackEquivalence verifies bit-for-bit state equivalence
// between NodeInfo before mutation and after ApplyWithUndo + Rollback.
func TestUndoLogRollbackEquivalence(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	logger := klog.FromContext(ctx)

	node := st.MakeNode().Name("test-node").Capacity(map[v1.ResourceName]string{
		v1.ResourceCPU:    "8000m",
		v1.ResourceMemory: "16000Mi",
	}).Obj()

	initialNodeInfo := NewNodeInfo()
	initialNodeInfo.SetNode(node)

	pod1 := makeTestPod("pod-1", "uid-1", "1000m", "2000Mi", 8080)
	pod2 := makeTestPod("pod-2", "uid-2", "2000m", "4000Mi", 9090)
	pod3 := makeTestPod("pod-3", "uid-3", "500m", "1000Mi")

	initialNodeInfo.AddPod(pod1)
	initialNodeInfo.AddPod(pod2)
	initialNodeInfo.AddPod(pod3)

	// Take a pristine baseline snapshot before trial mutations.
	baselineSnapshot := initialNodeInfo.SnapshotConcrete()

	// Perform multiple trial speculative mutations using UndoLog.
	undoLog := NewUndoLog()
	mutator := NewNodeInfoDeltaMutator(initialNodeInfo, undoLog)

	pInfo1, _ := NewPodInfo(pod1)
	pInfo2, _ := NewPodInfo(pod2)

	// Speculatively remove pod1 and pod2.
	if err := mutator.RemovePodWithUndo(logger, pInfo1); err != nil {
		t.Fatalf("Failed to RemovePodWithUndo pod1: %v", err)
	}
	if err := mutator.RemovePodWithUndo(logger, pInfo2); err != nil {
		t.Fatalf("Failed to RemovePodWithUndo pod2: %v", err)
	}

	// Verify that state actually changed.
	if initialNodeInfo.Requested.MilliCPU == baselineSnapshot.Requested.MilliCPU {
		t.Fatalf("Expected CPU request to decrease during trial eviction")
	}

	// Speculatively add a new trial pod.
	trialPod := makeTestPod("trial-preemptor", "uid-trial", "2500m", "5000Mi", 8080)
	pInfoTrial, _ := NewPodInfo(trialPod)
	if err := mutator.AddPodWithUndo(logger, pInfoTrial); err != nil {
		t.Fatalf("Failed to AddPodWithUndo trial pod: %v", err)
	}

	if undoLog.Len() != 3 {
		t.Fatalf("Expected 3 undo entries, got %d", undoLog.Len())
	}

	// Rollback all trial mutations.
	if err := undoLog.Rollback(); err != nil {
		t.Fatalf("UndoLog.Rollback failed: %v", err)
	}

	if undoLog.Len() != 0 {
		t.Fatalf("Expected 0 undo entries after rollback, got %d", undoLog.Len())
	}

	// Verify bit-for-bit equivalence of resources, pods, and used ports.
	if diff := cmp.Diff(baselineSnapshot.Requested, initialNodeInfo.Requested); diff != "" {
		t.Errorf("Requested resources mismatch after rollback (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(baselineSnapshot.NonZeroRequested, initialNodeInfo.NonZeroRequested); diff != "" {
		t.Errorf("NonZeroRequested mismatch after rollback (-want +got):\n%s", diff)
	}
	if len(initialNodeInfo.Pods) != len(baselineSnapshot.Pods) {
		t.Errorf("Pods count mismatch: got %d, want %d", len(initialNodeInfo.Pods), len(baselineSnapshot.Pods))
	}
	if diff := cmp.Diff(baselineSnapshot.UsedPorts, initialNodeInfo.UsedPorts); diff != "" {
		t.Errorf("UsedPorts mismatch after rollback (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(baselineSnapshot.PVCRefCounts, initialNodeInfo.PVCRefCounts); diff != "" {
		t.Errorf("PVCRefCounts mismatch after rollback (-want +got):\n%s", diff)
	}
}

// TestUndoLogRaceAndIsolation verifies concurrent independent undo operations
// across multiple isolated transactions without cross-contamination.
func TestUndoLogRaceAndIsolation(t *testing.T) {
	const workers = 10
	done := make(chan struct{})

	for w := 0; w < workers; w++ {
		go func(workerID int) {
			defer func() { done <- struct{}{} }()
			u := NewUndoLog()
			val := 0

			for i := 0; i < 50; i++ {
				old := val
				val += i
				u.Record(func() error {
					val = old
					return nil
				})
			}

			if err := u.Rollback(); err != nil {
				t.Errorf("Worker %d rollback error: %v", workerID, err)
			}
			if val != 0 {
				t.Errorf("Worker %d expected val 0 after rollback, got %d", workerID, val)
			}
		}(w)
	}

	for w := 0; w < workers; w++ {
		<-done
	}
}

// BenchmarkCloningVsUndoLog compares NodeInfo.Snapshot() vs UndoLog in-place mutations.
func BenchmarkCloningVsUndoLog(b *testing.B) {
	counts := []int{10, 50, 200, 500}

	for _, count := range counts {
		node := st.MakeNode().Name("bench-node").Capacity(map[v1.ResourceName]string{
			v1.ResourceCPU:    "128000m",
			v1.ResourceMemory: "512000Mi",
		}).Obj()

		initialNodeInfo := NewNodeInfo()
		initialNodeInfo.SetNode(node)

		var pods []fwk.PodInfo
		for i := 0; i < count; i++ {
			p := makeTestPod(fmt.Sprintf("pod-%d", i), fmt.Sprintf("uid-%d", i), "250m", "512Mi", int32(8000+(i%1000)))
			pi, _ := NewPodInfo(p)
			initialNodeInfo.AddPodInfo(pi)
			pods = append(pods, pi)
		}

		logger := klog.Background()

		// Scenario A: Deep Cloning (Current Upstream Pattern)
		b.Run(fmt.Sprintf("DeepCloning_Snapshot_%d_Pods", count), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Deep clone NodeInfo before trial eviction
				cloned := initialNodeInfo.SnapshotConcrete()
				// Evict 5 candidate victims
				for v := 0; v < 5 && v < len(pods); v++ {
					_ = cloned.RemovePod(logger, pods[v].GetPod())
				}
				// Discard cloned copy (simulates trial branch abort/failure)
			}
		})

		// Scenario B: Undo-Log In-Place Reversible Delta (Idea 7 Pattern)
		b.Run(fmt.Sprintf("UndoLog_Rollback_%d_Pods", count), func(b *testing.B) {
			b.ReportAllocs()
			undoLog := NewUndoLog()
			mutator := NewNodeInfoDeltaMutator(initialNodeInfo, undoLog)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Evict 5 candidate victims in-place with undo logging
				for v := 0; v < 5 && v < len(pods); v++ {
					_ = mutator.RemovePodWithUndo(logger, pods[v])
				}
				// Rollback in-place to restore baseline
				_ = undoLog.Rollback()
			}
		})
	}
}
