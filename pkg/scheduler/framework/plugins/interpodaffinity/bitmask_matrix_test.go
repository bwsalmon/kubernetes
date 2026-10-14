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

package interpodaffinity

import (
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework"
)

func createTestGangPod(name string, gangLabel string, withAntiAffinity bool) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			UID:       types.UID(name + "-uid"),
			Labels: map[string]string{
				"app": gangLabel,
			},
		},
		Spec: v1.PodSpec{},
	}
	if withAntiAffinity {
		pod.Spec.Affinity = &v1.Affinity{
			PodAntiAffinity: &v1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
					{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"app": gangLabel,
							},
						},
						TopologyKey: v1.LabelHostname,
					},
				},
			},
		}
	}
	return pod
}

func TestBitmaskMatrix_BasicAndFastPath(t *testing.T) {
	// Gang of 8 pods with mutual anti-affinity (fits into 1 word of 64 bits)
	numPods := 8
	gangPods := make([]*v1.Pod, numPods)
	for i := 0; i < numPods; i++ {
		gangPods[i] = createTestGangPod(fmt.Sprintf("pod-%d", i), "llama-train", true)
	}

	nodes := []string{"node-0", "node-1", "node-2", "node-3"}
	matrix, err := NewInterPodAntiAffinityBitmaskMatrix(gangPods, nodes, nil, v1.LabelHostname)
	if err != nil {
		t.Fatalf("unexpected error creating matrix: %v", err)
	}

	if matrix.NumWords() != 1 {
		t.Errorf("expected 1 word for 8 pods, got %d", matrix.NumWords())
	}
	if matrix.NumDomains() != len(nodes) {
		t.Errorf("expected %d domains, got %d", len(nodes), matrix.NumDomains())
	}
	if matrix.FallbackToStandard() {
		t.Errorf("expected fallbackToStandard to be false")
	}

	d0 := matrix.DomainIndexForNode("node-0")
	if d0 < 0 {
		t.Fatalf("node-0 not found")
	}

	// Initially, domain 0 is free: pod 0 has no violation
	if matrix.IsViolated(d0, 0) {
		t.Errorf("expected no violation for pod 0 on node-0 initially")
	}

	// Place pod 0 on node-0
	matrix.ApplyAssignment(d0, 0)

	// Now pod 1 (which conflicts with llama-train) should violate on node-0
	if !matrix.IsViolated(d0, 1) {
		t.Errorf("expected violation for pod 1 on node-0 after placing pod 0")
	}

	// Pod 1 should NOT violate on node-1
	d1 := matrix.DomainIndexForNode("node-1")
	if matrix.IsViolated(d1, 1) {
		t.Errorf("expected no violation for pod 1 on node-1")
	}

	// Revert pod 0 from node-0 (backtracking)
	matrix.RevertAssignment(d0, 0)

	// Now pod 1 should no longer violate on node-0
	if matrix.IsViolated(d0, 1) {
		t.Errorf("expected no violation for pod 1 on node-0 after revert")
	}
}

func TestBitmaskMatrix_MultiWordScaling(t *testing.T) {
	// Gang of 128 pods (requires 2 words of 64 bits) and 256 pods (requires 4 words)
	for _, numPods := range []int{128, 256} {
		t.Run(fmt.Sprintf("%d_Pods", numPods), func(t *testing.T) {
			gangPods := make([]*v1.Pod, numPods)
			for i := 0; i < numPods; i++ {
				gangPods[i] = createTestGangPod(fmt.Sprintf("pod-%d", i), "distributed-train", true)
			}

			nodes := make([]string, 500)
			for i := 0; i < 500; i++ {
				nodes[i] = fmt.Sprintf("node-%d", i)
			}

			matrix, err := NewInterPodAntiAffinityBitmaskMatrix(gangPods, nodes, nil, v1.LabelHostname)
			if err != nil {
				t.Fatalf("unexpected error creating matrix: %v", err)
			}

			expectedWords := (numPods + 63) / 64
			if matrix.NumWords() != expectedWords {
				t.Errorf("expected %d words, got %d", expectedWords, matrix.NumWords())
			}

			targetNode := "node-42"
			dIdx := matrix.DomainIndexForNode(targetNode)

			// Place pod 70 (lives in word 1) on targetNode
			pod70 := 70
			if matrix.IsViolated(dIdx, pod70) {
				t.Errorf("pod 70 should not violate initially")
			}
			matrix.ApplyAssignment(dIdx, pod70)

			// Pod 5 (lives in word 0) and Pod 100 (lives in word 1) should now violate
			if !matrix.IsViolated(dIdx, 5) {
				t.Errorf("pod 5 should violate because pod 70 is placed")
			}
			if !matrix.IsViolated(dIdx, 100) {
				t.Errorf("pod 100 should violate because pod 70 is placed")
			}

			// Backtrack pod 70
			matrix.RevertAssignment(dIdx, pod70)

			// Should be free again
			if matrix.IsViolated(dIdx, 5) {
				t.Errorf("pod 5 should not violate after revert")
			}
			if matrix.IsViolated(dIdx, 100) {
				t.Errorf("pod 100 should not violate after revert")
			}
		})
	}
}

func TestBitmaskMatrix_ExistingPodsAntiAffinity(t *testing.T) {
	numPods := 4
	gangPods := make([]*v1.Pod, numPods)
	for i := 0; i < numPods; i++ {
		gangPods[i] = createTestGangPod(fmt.Sprintf("pod-%d", i), "gang-app", true)
	}

	nodes := []string{"node-clean", "node-occupied"}

	// Existing pod on node-occupied has matching app=gang-app
	existingPods := map[string][]*v1.Pod{
		"node-occupied": {
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "existing-victim",
					Namespace: "default",
					Labels: map[string]string{
						"app": "gang-app",
					},
				},
			},
		},
	}

	matrix, err := NewInterPodAntiAffinityBitmaskMatrix(gangPods, nodes, existingPods, v1.LabelHostname)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dClean := matrix.DomainIndexForNode("node-clean")
	dOcc := matrix.DomainIndexForNode("node-occupied")

	// Clean node should not violate
	if matrix.IsViolated(dClean, 0) {
		t.Errorf("node-clean should not violate for pod 0")
	}

	// Occupied node should immediately violate for all gang pods
	for i := 0; i < numPods; i++ {
		if !matrix.IsViolated(dOcc, i) {
			t.Errorf("node-occupied must violate for pod %d due to existing pod", i)
		}
	}

	// ResetToExisting should preserve this
	matrix.ResetToExisting()
	if !matrix.IsViolated(dOcc, 0) {
		t.Errorf("node-occupied must still violate after ResetToExisting")
	}
}

func TestBitmaskMatrix_ComplexExpressionFallback(t *testing.T) {
	// Pod with NotIn match expression should trigger FallbackToStandard
	podWithNotIn := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-complex",
			Labels: map[string]string{
				"app": "complex",
			},
		},
		Spec: v1.PodSpec{
			Affinity: &v1.Affinity{
				PodAntiAffinity: &v1.PodAntiAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
						{
							LabelSelector: &metav1.LabelSelector{
								MatchExpressions: []metav1.LabelSelectorRequirement{
									{
										Key:      "tier",
										Operator: metav1.LabelSelectorOpNotIn,
										Values:   []string{"cache"},
									},
								},
							},
							TopologyKey: v1.LabelHostname,
						},
					},
				},
			},
		},
	}

	if CanAcceleratePod(podWithNotIn) {
		t.Errorf("expected CanAcceleratePod to return false for NotIn operator")
	}

	matrix, err := NewInterPodAntiAffinityBitmaskMatrix([]*v1.Pod{podWithNotIn}, []string{"node-0"}, nil, v1.LabelHostname)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !matrix.FallbackToStandard() {
		t.Errorf("matrix with complex selector must have FallbackToStandard == true")
	}
}

func TestBitmaskMatrix_CycleStateIntegrationAndClone(t *testing.T) {
	pod := createTestGangPod("p0", "job-1", true)
	state := schedulerframework.NewCycleState()

	matrix, err := GetOrCreateInterPodAntiAffinityBitmaskMatrix(state, []*v1.Pod{pod}, []string{"n0", "n1"}, nil, v1.LabelHostname)
	if err != nil {
		t.Fatalf("failed to create matrix in CycleState: %v", err)
	}

	retrieved, ok := GetInterPodAntiAffinityBitmaskMatrix(state)
	if !ok || retrieved != matrix {
		t.Fatalf("retrieved matrix does not match created matrix")
	}

	// Test Clone() isolation
	cloned := matrix.Clone().(*InterPodAntiAffinityBitmaskMatrix)
	d0 := matrix.DomainIndexForNode("n0")

	// Mutate original
	matrix.ApplyAssignment(d0, 0)
	if !matrix.IsViolated(d0, 0) {
		t.Errorf("original should have violation on n0")
	}

	// Cloned should remain unmutated
	if cloned.IsViolated(d0, 0) {
		t.Errorf("cloned matrix should not be affected by mutation on original")
	}
}
