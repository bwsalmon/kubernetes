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

func BenchmarkSelectVictimsOnNode(b *testing.B) {
	densities := []int{10, 50, 200, 500}

	for _, count := range densities {
		b.Run(fmt.Sprintf("Logarithmic_Density_%d_Pods", count), func(b *testing.B) {
			ctx := context.Background()
			node := st.MakeNode().Name("node-bench").Capacity(map[v1.ResourceName]string{
				v1.ResourceCPU:    "1000m",
				v1.ResourceMemory: "10000Mi",
			}).Obj()

			baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			var initPods []*v1.Pod
			for i := 0; i < count; i++ {
				prio := int32(100 + (i % 50))
				stTime := metav1.NewTime(baseTime.Add(time.Duration(i) * time.Minute))
				p := st.MakePod().Name(fmt.Sprintf("bench-pod-%04d", i)).
					UID(fmt.Sprintf("uid-bench-%04d", i)).
					Node("node-bench").
					Priority(prio).
					StartTime(stTime).
					Req(map[v1.ResourceName]string{
						v1.ResourceCPU:    "1m",
						v1.ResourceMemory: "10Mi",
					}).Obj()
				initPods = append(initPods, p)
			}

			preemptor := st.MakePod().Name("bench-preemptor").
				UID("bench-preemptor-uid").
				Priority(10000).
				Req(map[v1.ResourceName]string{
					v1.ResourceCPU:    "500m",
					v1.ResourceMemory: "100Mi",
				}).Obj()

			// Use tb wrapper for testing.T helper
			pl, _, nodeInfo, potentialVictims, cycleState := setupHighDensityTestEnv(&testing.T{}, node, initPods, preemptor, nil)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				cs := cycleState.Clone()
				_, _, status := pl.SelectVictimsOnNode(ctx, cs, preemptor, nodeInfo.Snapshot(), potentialVictims, nil)
				if !status.IsSuccess() {
					b.Fatalf("SelectVictimsOnNode failed: %v", status)
				}
			}
		})

		b.Run(fmt.Sprintf("Linear_Density_%d_Pods", count), func(b *testing.B) {
			ctx := context.Background()
			node := st.MakeNode().Name("node-bench").Capacity(map[v1.ResourceName]string{
				v1.ResourceCPU:    "1000m",
				v1.ResourceMemory: "10000Mi",
			}).Obj()

			baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			var initPods []*v1.Pod
			for i := 0; i < count; i++ {
				prio := int32(100 + (i % 50))
				stTime := metav1.NewTime(baseTime.Add(time.Duration(i) * time.Minute))
				p := st.MakePod().Name(fmt.Sprintf("bench-pod-%04d", i)).
					UID(fmt.Sprintf("uid-bench-%04d", i)).
					Node("node-bench").
					Priority(prio).
					StartTime(stTime).
					Req(map[v1.ResourceName]string{
						v1.ResourceCPU:    "1m",
						v1.ResourceMemory: "10Mi",
					}).Obj()
				initPods = append(initPods, p)
			}

			preemptor := st.MakePod().Name("bench-preemptor").
				UID("bench-preemptor-uid").
				Priority(10000).
				Req(map[v1.ResourceName]string{
					v1.ResourceCPU:    "500m",
					v1.ResourceMemory: "100Mi",
				}).Obj()

			pl, _, nodeInfo, potentialVictims, cycleState := setupHighDensityTestEnv(&testing.T{}, node, initPods, preemptor, nil)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				cs := cycleState.Clone()
				_, _, status := linearSelectVictimsOnNode(ctx, pl, cs, preemptor, nodeInfo.Snapshot(), potentialVictims, nil)
				if !status.IsSuccess() {
					b.Fatalf("linearSelectVictimsOnNode failed: %v", status)
				}
			}
		})
	}
}
