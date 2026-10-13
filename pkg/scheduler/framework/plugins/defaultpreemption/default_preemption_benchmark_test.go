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
	"sort"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/preemption"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
)

// unbucketedSelectVictimsOnNode is a benchmark baseline using logarithmic reprieval without Idea 8's bucket pre-partitioning.
func unbucketedSelectVictimsOnNode(
	ctx context.Context,
	pl *DefaultPreemption,
	cycleState fwk.CycleState,
	preemptor *v1.Pod,
	nodeInfo fwk.NodeInfo,
	potentialVictims []*preemption.DomainVictim,
	pdbs []*policy.PodDisruptionBudget,
) ([]*v1.Pod, int, *fwk.Status) {
	logger := klog.FromContext(ctx)
	mainNodeName := nodeInfo.Node().Name
	nameToNode := map[string]fwk.NodeInfo{mainNodeName: nodeInfo}

	removeVictim := func(dv *preemption.DomainVictim) error {
		for _, pi := range dv.Pods() {
			nInfo := nameToNode[pi.GetPod().Spec.NodeName]
			if pi.GetPod().Spec.NodeName == mainNodeName {
				if err := nInfo.RemovePod(logger, pi.GetPod()); err != nil {
					return err
				}
			}
			status := pl.fh.RunPreFilterExtensionRemovePod(ctx, cycleState, preemptor, pi, nInfo)
			if !status.IsSuccess() {
				return status.AsError()
			}
		}
		return nil
	}

	addVictim := func(pu *preemption.DomainVictim) error {
		for _, pi := range pu.Pods() {
			nInfo := nameToNode[pi.GetPod().Spec.NodeName]
			if pi.GetPod().Spec.NodeName == mainNodeName {
				nInfo.AddPodInfo(pi)
			}
			status := pl.fh.RunPreFilterExtensionAddPod(ctx, cycleState, preemptor, pi, nInfo)
			if !status.IsSuccess() {
				return status.AsError()
			}
		}
		return nil
	}

	var eligibleVictims []*preemption.DomainVictim
	for _, victim := range potentialVictims {
		if pl.isPreemptionAllowedAcrossAllVictimNodes(victim, preemptor) {
			eligibleVictims = append(eligibleVictims, victim)
		}
	}
	if len(eligibleVictims) == 0 {
		return nil, 0, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "No preemption victims found for incoming pod")
	}

	for _, victim := range eligibleVictims {
		for name, nInfo := range victim.AffectedNodes() {
			if _, ok := nameToNode[name]; !ok {
				nameToNode[name] = nInfo
			}
		}
	}

	for _, victim := range eligibleVictims {
		if err := removeVictim(victim); err != nil {
			return nil, 0, fwk.AsStatus(err)
		}
	}

	if status := pl.fh.RunFilterPluginsWithNominatedPods(ctx, cycleState, preemptor, nodeInfo); !status.IsSuccess() {
		return nil, 0, status
	}

	sortedVictims := make([]*preemption.DomainVictim, len(eligibleVictims))
	copy(sortedVictims, eligibleVictims)
	sort.Slice(sortedVictims, func(i, j int) bool {
		return pl.MoreImportantVictim(sortedVictims[i], sortedVictims[j])
	})

	violatingVictims, nonViolatingVictims := preemption.FilterVictimsWithPDBViolation(sortedVictims, pdbs)
	var victims []*preemption.DomainVictim

	reprieveVictimLinear := func(v *preemption.DomainVictim) (bool, error) {
		if err := addVictim(v); err != nil {
			return false, err
		}
		status := pl.fh.RunFilterPluginsWithNominatedPods(ctx, cycleState, preemptor, nodeInfo)
		fits := status.IsSuccess()
		if !fits {
			if err := removeVictim(v); err != nil {
				return false, err
			}
			victims = append(victims, v)
		}
		return fits, nil
	}

	reprieveVictimsLogarithmic := func(candidates []*preemption.DomainVictim, onReject func(v *preemption.DomainVictim, index int)) error {
		n := len(candidates)
		if n < 4 {
			for i, v := range candidates {
				fits, err := reprieveVictimLinear(v)
				if err != nil {
					return err
				}
				if !fits {
					onReject(v, i)
				}
			}
			return nil
		}

		i := 0
		step := 1
		for i < n {
			batchSize := step
			if i+batchSize > n {
				batchSize = n - i
			}
			if batchSize == 1 {
				v := candidates[i]
				fits, err := reprieveVictimLinear(v)
				if err != nil {
					return err
				}
				if !fits {
					onReject(v, i)
					step = 1
				} else {
					step = 2
				}
				i++
				continue
			}

			chunk := candidates[i : i+batchSize]
			for j, v := range chunk {
				if err := addVictim(v); err != nil {
					for k := j - 1; k >= 0; k-- {
						_ = removeVictim(chunk[k])
					}
					return err
				}
			}

			status := pl.fh.RunFilterPluginsWithNominatedPods(ctx, cycleState, preemptor, nodeInfo)
			if status.IsSuccess() {
				i += batchSize
				step *= 2
				continue
			}

			for j := len(chunk) - 1; j >= 0; j-- {
				if err := removeVictim(chunk[j]); err != nil {
					return err
				}
			}

			low := i
			high := i + batchSize - 1
			bestFitIndex := i - 1
			currAdded := i - 1

			for low <= high {
				mid := low + (high-low)/2
				for j := currAdded + 1; j <= mid; j++ {
					if err := addVictim(candidates[j]); err != nil {
						return err
					}
				}
				currAdded = mid

				status := pl.fh.RunFilterPluginsWithNominatedPods(ctx, cycleState, preemptor, nodeInfo)
				if status.IsSuccess() {
					bestFitIndex = mid
					low = mid + 1
				} else {
					for j := currAdded; j > bestFitIndex; j-- {
						if err := removeVictim(candidates[j]); err != nil {
							return err
						}
					}
					currAdded = bestFitIndex
					high = mid - 1
				}
			}

			i = bestFitIndex + 1
			if i < n {
				v := candidates[i]
				victims = append(victims, v)
				onReject(v, i)
				i++
			}
			step = 1
		}
		return nil
	}

	numViolatingVictim := 0
	for _, violatingVictim := range violatingVictims {
		if fits, err := reprieveVictimLinear(violatingVictim.Victim); err != nil {
			return nil, 0, fwk.AsStatus(err)
		} else if !fits {
			numViolatingVictim += violatingVictim.ViolateCount
		}
	}

	if err := reprieveVictimsLogarithmic(nonViolatingVictims, func(v *preemption.DomainVictim, index int) {}); err != nil {
		return nil, 0, fwk.AsStatus(err)
	}

	if len(violatingVictims) != 0 && len(nonViolatingVictims) != 0 {
		sort.Slice(victims, func(i, j int) bool { return pl.MoreImportantVictim(victims[i], victims[j]) })
	}
	var victimPods []*v1.Pod
	for _, vi := range victims {
		for _, pi := range vi.Pods() {
			victimPods = append(victimPods, pi.GetPod())
		}
	}
	return victimPods, numViolatingVictim, nil
}

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

// BenchmarkSelectVictimsOnNode_MultiTiered evaluates Idea 8 (Priority-Tier Bucket Pre-Partitioning)
// against unbucketed logarithmic and linear baselines on nodes with 50 to 500 pods structured into
// 3 discrete priority tiers (Tier 1: Scavenger 30%, Tier 2: Batch 50%, Tier 3: Prod 20%).
func BenchmarkSelectVictimsOnNode_MultiTiered(b *testing.B) {
	densities := []int{50, 100, 250, 500}

	for _, count := range densities {
		b.Run(fmt.Sprintf("Density_%d_Pods", count), func(b *testing.B) {
			ctx := context.Background()
			node := st.MakeNode().Name("node-bench-tiered").Capacity(map[v1.ResourceName]string{
				v1.ResourcePods:   "1000",
				v1.ResourceCPU:    "50000m",
				v1.ResourceMemory: "250000Mi",
			}).Obj()

			baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			var initPods []*v1.Pod
			for i := 0; i < count; i++ {
				var prio int32
				ratio := float64(i) / float64(count)
				if ratio < 0.30 {
					prio = 0
				} else if ratio < 0.80 {
					prio = 1000
				} else {
					prio = 50000
				}
				stTime := metav1.NewTime(baseTime.Add(time.Duration(i) * time.Minute))
				p := st.MakePod().Name(fmt.Sprintf("bench-pod-%04d", i)).
					UID(fmt.Sprintf("uid-bench-%04d", i)).
					Node("node-bench-tiered").
					Priority(prio).
					StartTime(stTime).
					Req(map[v1.ResourceName]string{
						v1.ResourceCPU:    "100m",
						v1.ResourceMemory: "500Mi",
					}).Obj()
				initPods = append(initPods, p)
			}

			// Preemptor needs 500m CPU; Tier 1 (priority 0) has plenty of pods to satisfy this.
			preemptor := st.MakePod().Name("bench-preemptor").
				UID("bench-preemptor-uid").
				Priority(100000).
				Req(map[v1.ResourceName]string{
					v1.ResourceCPU:    "500m",
					v1.ResourceMemory: "2000Mi",
				}).Obj()

			pl, _, nodeInfo, potentialVictims, cycleState := setupHighDensityTestEnv(&testing.T{}, node, initPods, preemptor, nil)

			b.Run("Idea8_Bucket_Partitioning", func(b *testing.B) {
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

			b.Run("Baseline_Logarithmic_Unbucketed", func(b *testing.B) {
				b.ResetTimer()
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					cs := cycleState.Clone()
					_, _, status := unbucketedSelectVictimsOnNode(ctx, pl, cs, preemptor, nodeInfo.Snapshot(), potentialVictims, nil)
					if !status.IsSuccess() {
						b.Fatalf("unbucketedSelectVictimsOnNode failed: %v", status)
					}
				}
			})

			b.Run("Baseline_Linear", func(b *testing.B) {
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
		})
	}
}
