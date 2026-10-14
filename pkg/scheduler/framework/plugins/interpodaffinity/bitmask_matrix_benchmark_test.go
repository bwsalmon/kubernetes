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
	"k8s.io/apimachinery/pkg/labels"
)

func BenchmarkBitmaskMatrix_IsViolated(b *testing.B) {
	for _, gangSize := range []int{64, 128, 256} {
		b.Run(fmt.Sprintf("Gang_%d_Nodes_1000", gangSize), func(b *testing.B) {
			gangPods := make([]*v1.Pod, gangSize)
			for i := 0; i < gangSize; i++ {
				gangPods[i] = createTestGangPod(fmt.Sprintf("pod-%d", i), "ai-train-bench", true)
			}
			nodes := make([]string, 1000)
			for i := 0; i < 1000; i++ {
				nodes[i] = fmt.Sprintf("node-%d", i)
			}

			matrix, err := NewInterPodAntiAffinityBitmaskMatrix(gangPods, nodes, nil, v1.LabelHostname)
			if err != nil {
				b.Fatalf("failed to create matrix: %v", err)
			}

			// Pre-assign every 10th pod
			for p := 0; p < gangSize; p += 10 {
				matrix.ApplyAssignment(p*3%1000, p)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				dIdx := i % 1000
				pIdx := i % gangSize
				_ = matrix.IsViolated(dIdx, pIdx)
			}
		})
	}
}

func BenchmarkStandardSelector_Match(b *testing.B) {
	selector, _ := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: map[string]string{"app": "ai-train-bench"},
	})
	targetLabels := labels.Set{"app": "ai-train-bench", "env": "prod", "tier": "ml"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = selector.Matches(targetLabels)
	}
}

func BenchmarkBitmaskMatrix_ApplyAndRevert(b *testing.B) {
	for _, gangSize := range []int{64, 256} {
		b.Run(fmt.Sprintf("Gang_%d_Nodes_1000", gangSize), func(b *testing.B) {
			gangPods := make([]*v1.Pod, gangSize)
			for i := 0; i < gangSize; i++ {
				gangPods[i] = createTestGangPod(fmt.Sprintf("pod-%d", i), "ai-train-bench", true)
			}
			nodes := make([]string, 1000)
			for i := 0; i < 1000; i++ {
				nodes[i] = fmt.Sprintf("node-%d", i)
			}

			matrix, err := NewInterPodAntiAffinityBitmaskMatrix(gangPods, nodes, nil, v1.LabelHostname)
			if err != nil {
				b.Fatalf("failed to create matrix: %v", err)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				dIdx := i % 1000
				pIdx := i % gangSize
				matrix.ApplyAssignment(dIdx, pIdx)
				matrix.RevertAssignment(dIdx, pIdx)
			}
		})
	}
}
