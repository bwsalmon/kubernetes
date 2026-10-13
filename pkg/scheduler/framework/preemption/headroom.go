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

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/component-helpers/resource"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/helper"
)

// CanNodeFitPreemptorHeadroom performs a conservative, coarse-grained check to determine
// if the candidate node could possibly satisfy the preemptor pod's resource requests and
// static node constraints after preempting all lower-priority pods.
//
// It returns (true, nil) if the node could potentially fit the pod.
// It returns (false, status) if the node mathematically cannot fit the preemptor
// or violates immutable node constraints.
func CanNodeFitPreemptorHeadroom(
	logger klog.Logger,
	pod *v1.Pod,
	nodeInfo fwk.NodeInfo,
	podGroupSnapshot fwk.PodGroupLister,
	compositePodGroupSnapshot fwk.CompositePodGroupLister,
	enablePodLevelResources bool,
) (bool, *fwk.Status) {
	if pod == nil || nodeInfo == nil || nodeInfo.Node() == nil {
		return false, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "Node or pod is nil")
	}

	node := nodeInfo.Node()

	// 1. Check static NodeSelector labels
	if len(pod.Spec.NodeSelector) > 0 {
		selector := labels.SelectorFromSet(pod.Spec.NodeSelector)
		if !selector.Matches(labels.Set(node.Labels)) {
			return false, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "node(s) didn't match Pod's node selector")
		}
	}

	// 2. Check static NoSchedule and NoExecute taints
	if _, untolerated := corev1helpers.FindMatchingUntoleratedTaint(
		logger,
		node.Spec.Taints,
		pod.Spec.Tolerations,
		helper.DoNotScheduleTaintsFilterFunc(),
		false,
	); untolerated {
		return false, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "node(s) had untolerated taint(s)")
	}

	// 3. Check resource capacity and headroom
	allocatable := nodeInfo.GetAllocatable()
	if allocatable == nil {
		return true, nil
	}

	preemptorPriority := corev1helpers.PodPriority(pod)
	podReq := resource.PodRequests(pod, resource.PodResourcesOptions{
		SkipPodLevelResources: !enablePodLevelResources,
	})
	podCPU := podReq.Cpu().MilliValue()
	podMem := podReq.Memory().Value()
	podStorage := podReq.StorageEphemeral().Value()

	// Calculate non-reclaimable resource usage and victim count on node
	var nonPreemptibleCPU int64
	var nonPreemptibleMem int64
	var nonPreemptibleStorage int64
	var nonPreemptibleScalars map[v1.ResourceName]int64
	nonPreemptiblePodCount := 0
	hasLowerPriorityVictims := false

	for _, pi := range nodeInfo.GetPods() {
		p := pi.GetPod()
		if p == nil {
			continue
		}
		// If the pod on node is the preemptor itself (e.g. deferred in-place pod resize),
		// it is not a third-party non-preemptible pod.
		if (len(pod.UID) > 0 && p.UID == pod.UID) || (p.Name == pod.Name && p.Namespace == pod.Namespace) {
			continue
		}

		priority := getPodPriority(p, podGroupSnapshot, compositePodGroupSnapshot)
		if priority >= preemptorPriority || (p.Spec.PreemptionPolicy != nil && *p.Spec.PreemptionPolicy == v1.PreemptNever) {
			nonPreemptiblePodCount++
			res := pi.CalculateResource().Resource
			if res != nil {
				nonPreemptibleCPU += res.GetMilliCPU()
				nonPreemptibleMem += res.GetMemory()
				nonPreemptibleStorage += res.GetEphemeralStorage()
				if len(res.GetScalarResources()) > 0 {
					if nonPreemptibleScalars == nil {
						nonPreemptibleScalars = make(map[v1.ResourceName]int64)
					}
					for sName, sVal := range res.GetScalarResources() {
						nonPreemptibleScalars[sName] += sVal
					}
				}
			}
		} else {
			hasLowerPriorityVictims = true
		}
	}

	// If there are no lower priority victims on the node to preempt, preemption cannot find any victims.
	if !hasLowerPriorityVictims {
		return false, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "No preemption victims found for incoming pod")
	}

	// Headroom check: Allocatable - NonPreemptible
	if podCPU > (allocatable.GetMilliCPU() - nonPreemptibleCPU) {
		return false, fwk.NewStatus(fwk.Unschedulable, "Insufficient cpu")
	}

	if podMem > (allocatable.GetMemory() - nonPreemptibleMem) {
		return false, fwk.NewStatus(fwk.Unschedulable, "Insufficient memory")
	}

	if podStorage > 0 && podStorage > (allocatable.GetEphemeralStorage()-nonPreemptibleStorage) {
		return false, fwk.NewStatus(fwk.Unschedulable, "Insufficient ephemeral-storage")
	}

	allocatableScalars := allocatable.GetScalarResources()
	for rName, qty := range podReq {
		if rName == v1.ResourceCPU || rName == v1.ResourceMemory || rName == v1.ResourceEphemeralStorage {
			continue
		}
		val := qty.Value()
		if val <= 0 {
			continue
		}
		availScalar := allocatableScalars[rName] - nonPreemptibleScalars[rName]
		if val > availScalar {
			return false, fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("Insufficient %s", rName))
		}
	}

	// Check allowed pods count
	if allowedPods := allocatable.GetAllowedPodNumber(); allowedPods > 0 {
		if nonPreemptiblePodCount+1 > allowedPods {
			return false, fwk.NewStatus(fwk.Unschedulable, "Too many pods")
		}
	}

	return true, nil
}
