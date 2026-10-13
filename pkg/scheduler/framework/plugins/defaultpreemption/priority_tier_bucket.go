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
	"sort"

	v1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/preemption"
)

// priorityBucket aggregates candidate preemption victims sharing the same priority value.
type priorityBucket struct {
	priority int32
	victims  []*preemption.DomainVictim
}

// partitionIntoPriorityBuckets groups potential victims by their priority and orders buckets
// from lowest priority to highest priority. Within each bucket, victims are sorted in descending
// order of importance via moreImportantVictim, strictly preserving PR #140999 and PR #141785
// deterministic tie-breaking.
func partitionIntoPriorityBuckets(
	victims []*preemption.DomainVictim,
	moreImportantVictim MoreImportantVictimFunc,
) []priorityBucket {
	if len(victims) == 0 {
		return nil
	}

	prioMap := make(map[int32][]*preemption.DomainVictim)
	for _, v := range victims {
		prio := v.Priority()
		prioMap[prio] = append(prioMap[prio], v)
	}

	prios := make([]int32, 0, len(prioMap))
	for p := range prioMap {
		prios = append(prios, p)
	}
	sort.Slice(prios, func(i, j int) bool {
		return prios[i] < prios[j]
	})

	buckets := make([]priorityBucket, len(prios))
	for i, p := range prios {
		bVictims := prioMap[p]
		sort.Slice(bVictims, func(i, j int) bool {
			return moreImportantVictim(bVictims[i], bVictims[j])
		})
		buckets[i] = priorityBucket{
			priority: p,
			victims:  bVictims,
		}
	}

	return buckets
}

// calculateResourceDeficits computes the net deficits for CPU, memory, ephemeral storage,
// and scalar extended resources that the candidate node must reclaim from preemption victims
// to fit the incoming preemptor pod.
func calculateResourceDeficits(
	podReq v1.ResourceList,
	nodeInfo fwk.NodeInfo,
) (deficitCPU, deficitMem, deficitStorage int64, deficitScalars map[v1.ResourceName]int64) {
	if nodeInfo == nil {
		return
	}
	allocatable := nodeInfo.GetAllocatable()
	requested := nodeInfo.GetRequested()
	if allocatable == nil || requested == nil {
		return
	}

	podCPU := podReq.Cpu().MilliValue()
	podMem := podReq.Memory().Value()
	podStorage := podReq.StorageEphemeral().Value()

	availCPU := allocatable.GetMilliCPU() - requested.GetMilliCPU()
	if podCPU > availCPU {
		deficitCPU = podCPU - availCPU
	}

	availMem := allocatable.GetMemory() - requested.GetMemory()
	if podMem > availMem {
		deficitMem = podMem - availMem
	}

	availStorage := allocatable.GetEphemeralStorage() - requested.GetEphemeralStorage()
	if podStorage > availStorage {
		deficitStorage = podStorage - availStorage
	}

	allocatableScalars := allocatable.GetScalarResources()
	requestedScalars := requested.GetScalarResources()
	for rName, qty := range podReq {
		if rName == v1.ResourceCPU || rName == v1.ResourceMemory || rName == v1.ResourceEphemeralStorage {
			continue
		}
		val := qty.Value()
		if val <= 0 {
			continue
		}
		avail := int64(0)
		if allocatableScalars != nil {
			avail = allocatableScalars[rName]
		}
		if requestedScalars != nil {
			avail -= requestedScalars[rName]
		}
		if val > avail {
			if deficitScalars == nil {
				deficitScalars = make(map[v1.ResourceName]int64)
			}
			deficitScalars[rName] = val - avail
		}
	}

	return
}

// findBoundaryBucket determines the minimum prefix of priority buckets (ordered from lowest
// to highest priority) whose cumulative released resources on mainNodeName satisfy or exceed
// all calculated resource deficits.
func findBoundaryBucket(
	buckets []priorityBucket,
	mainNodeName string,
	deficitCPU, deficitMem, deficitStorage int64,
	deficitScalars map[v1.ResourceName]int64,
) int {
	if len(buckets) <= 1 {
		return 0
	}

	var cumCPU, cumMem, cumStorage int64
	cumScalars := make(map[v1.ResourceName]int64)

	for i, b := range buckets {
		for _, v := range b.victims {
			for _, pi := range v.Pods() {
				if pi.GetPod() != nil && pi.GetPod().Spec.NodeName == mainNodeName {
					calcRes := pi.CalculateResource()
					if calcRes.Resource != nil {
						cumCPU += calcRes.Resource.GetMilliCPU()
						cumMem += calcRes.Resource.GetMemory()
						cumStorage += calcRes.Resource.GetEphemeralStorage()
						for sName, sVal := range calcRes.Resource.GetScalarResources() {
							cumScalars[sName] += sVal
						}
					}
				}
			}
		}

		if cumCPU < deficitCPU || cumMem < deficitMem || cumStorage < deficitStorage {
			continue
		}
		scalarsMet := true
		for sName, sDef := range deficitScalars {
			if cumScalars[sName] < sDef {
				scalarsMet = false
				break
			}
		}
		if scalarsMet {
			return i
		}
	}

	return len(buckets) - 1
}
