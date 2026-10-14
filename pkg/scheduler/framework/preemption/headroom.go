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
	"math"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/component-helpers/resource"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/helper"
)

func saturatingAdd(a, b int64) int64 {
	if b > 0 && a > math.MaxInt64-b {
		return math.MaxInt64
	}
	if b < 0 && a < math.MinInt64-b {
		return math.MinInt64
	}
	return a + b
}

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
	return CanNodeFitPreemptorHeadroomWithRequests(
		logger,
		pod,
		nil,
		nodeInfo,
		podGroupSnapshot,
		compositePodGroupSnapshot,
		enablePodLevelResources,
	)
}

// CanNodeFitPreemptorHeadroomWithRequests is an optimized variant of CanNodeFitPreemptorHeadroom
// that accepts precalculated pod requests to eliminate repeated allocation churn across candidate nodes.
func CanNodeFitPreemptorHeadroomWithRequests(
	logger klog.Logger,
	pod *v1.Pod,
	podReq v1.ResourceList,
	nodeInfo fwk.NodeInfo,
	podGroupSnapshot fwk.PodGroupLister,
	compositePodGroupSnapshot fwk.CompositePodGroupLister,
	enablePodLevelResources bool,
) (bool, *fwk.Status) {
	if pod == nil || nodeInfo == nil || nodeInfo.Node() == nil {
		return false, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "Node or pod is nil")
	}

	node := nodeInfo.Node()

	// In-Place Pod Vertical Scaling (KEP-1287 & KEP-5836):
	// If the pod has a deferred resize pending, it is already bound to a specific node (pod.Spec.NodeName).
	// A deferred resize pod cannot run on or preempt pods on any other node in the cluster.
	isDeferredResize := resource.IsPodResizeDeferred(pod)
	if isDeferredResize && len(pod.Spec.NodeName) > 0 && pod.Spec.NodeName != node.Name {
		return false, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "deferred resize pod cannot preempt on a different node")
	}

	// 1. Check static NodeSelector labels
	// When a pod is undergoing an in-place vertical resize on its existing assigned node,
	// NodeSelector is skipped (matching NodeAffinity filter plugin behavior).
	if !isDeferredResize && len(pod.Spec.NodeSelector) > 0 {
		selector := labels.SelectorFromSet(pod.Spec.NodeSelector)
		if !selector.Matches(labels.Set(node.Labels)) {
			return false, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "node(s) didn't match Pod's node selector")
		}
	}

	// 2. Check static NoSchedule and NoExecute taints
	// When a pod is undergoing an in-place vertical resize on its existing assigned node,
	// NoSchedule taints are skipped (matching TaintToleration filter plugin behavior).
	if !isDeferredResize {
		if _, untolerated := corev1helpers.FindMatchingUntoleratedTaint(
			logger,
			node.Spec.Taints,
			pod.Spec.Tolerations,
			helper.DoNotScheduleTaintsFilterFunc(),
			false,
		); untolerated {
			return false, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "node(s) had untolerated taint(s)")
		}
	}

	// 3. Check resource capacity and headroom
	allocatable := nodeInfo.GetAllocatable()
	if allocatable == nil {
		return true, nil
	}

	preemptorPriority := getPodPriority(pod, podGroupSnapshot, compositePodGroupSnapshot)
	if podReq == nil {
		podReq = resource.PodRequests(pod, resource.PodResourcesOptions{
			SkipPodLevelResources: !enablePodLevelResources,
		})
	}
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
				nonPreemptibleCPU = saturatingAdd(nonPreemptibleCPU, res.GetMilliCPU())
				nonPreemptibleMem = saturatingAdd(nonPreemptibleMem, res.GetMemory())
				nonPreemptibleStorage = saturatingAdd(nonPreemptibleStorage, res.GetEphemeralStorage())
				if len(res.GetScalarResources()) > 0 {
					if nonPreemptibleScalars == nil {
						nonPreemptibleScalars = make(map[v1.ResourceName]int64)
					}
					for sName, sVal := range res.GetScalarResources() {
						nonPreemptibleScalars[sName] = saturatingAdd(nonPreemptibleScalars[sName], sVal)
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

// ViableNodeHeadroom holds the precomputed available theoretical headroom of a viable node.
// Available headroom is: Allocatable - NonPreemptibleUsage.
type ViableNodeHeadroom struct {
	NodeName         string
	AvailableMilliCPU int64
	AvailableMemory   int64
	AvailableStorage  int64
	AvailableScalars  map[v1.ResourceName]int64
}

// CalculateNodePreemptibleHeadroom computes the theoretical upper bound on resources
// that can be made available on a node if all preemptible (lower-priority) pods are evicted.
// If the node cannot fit the preemptor pod due to static constraints (taints, selectors)
// or has zero lower-priority victims, it returns (nil, status).
func CalculateNodePreemptibleHeadroom(
	logger klog.Logger,
	pod *v1.Pod,
	podReq v1.ResourceList,
	nodeInfo fwk.NodeInfo,
	podGroupSnapshot fwk.PodGroupLister,
	compositePodGroupSnapshot fwk.CompositePodGroupLister,
	enablePodLevelResources bool,
) (*ViableNodeHeadroom, *fwk.Status) {
	if pod == nil || nodeInfo == nil || nodeInfo.Node() == nil {
		return nil, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "Node or pod is nil")
	}

	canFit, status := CanNodeFitPreemptorHeadroomWithRequests(
		logger,
		pod,
		podReq,
		nodeInfo,
		podGroupSnapshot,
		compositePodGroupSnapshot,
		enablePodLevelResources,
	)
	if !canFit {
		return nil, status
	}

	allocatable := nodeInfo.GetAllocatable()
	if allocatable == nil {
		return &ViableNodeHeadroom{
			NodeName: nodeInfo.Node().Name,
		}, nil
	}

	preemptorPriority := getPodPriority(pod, podGroupSnapshot, compositePodGroupSnapshot)

	var nonPreemptibleCPU int64
	var nonPreemptibleMem int64
	var nonPreemptibleStorage int64
	var nonPreemptibleScalars map[v1.ResourceName]int64

	for _, pi := range nodeInfo.GetPods() {
		p := pi.GetPod()
		if p == nil {
			continue
		}
		if (len(pod.UID) > 0 && p.UID == pod.UID) || (p.Name == pod.Name && p.Namespace == pod.Namespace) {
			continue
		}

		priority := getPodPriority(p, podGroupSnapshot, compositePodGroupSnapshot)
		if priority >= preemptorPriority || (p.Spec.PreemptionPolicy != nil && *p.Spec.PreemptionPolicy == v1.PreemptNever) {
			res := pi.CalculateResource().Resource
			if res != nil {
				nonPreemptibleCPU = saturatingAdd(nonPreemptibleCPU, res.GetMilliCPU())
				nonPreemptibleMem = saturatingAdd(nonPreemptibleMem, res.GetMemory())
				nonPreemptibleStorage = saturatingAdd(nonPreemptibleStorage, res.GetEphemeralStorage())
				if len(res.GetScalarResources()) > 0 {
					if nonPreemptibleScalars == nil {
						nonPreemptibleScalars = make(map[v1.ResourceName]int64)
					}
					for sName, sVal := range res.GetScalarResources() {
						nonPreemptibleScalars[sName] = saturatingAdd(nonPreemptibleScalars[sName], sVal)
					}
				}
			}
		}
	}

	availCPU := allocatable.GetMilliCPU() - nonPreemptibleCPU
	if availCPU < 0 {
		availCPU = 0
	}
	availMem := allocatable.GetMemory() - nonPreemptibleMem
	if availMem < 0 {
		availMem = 0
	}
	availStorage := allocatable.GetEphemeralStorage() - nonPreemptibleStorage
	if availStorage < 0 {
		availStorage = 0
	}

	var availScalars map[v1.ResourceName]int64
	if allocScalars := allocatable.GetScalarResources(); len(allocScalars) > 0 {
		availScalars = make(map[v1.ResourceName]int64, len(allocScalars))
		for sName, sVal := range allocScalars {
			av := sVal - nonPreemptibleScalars[sName]
			if av < 0 {
				av = 0
			}
			availScalars[sName] = av
		}
	}

	return &ViableNodeHeadroom{
		NodeName:         nodeInfo.Node().Name,
		AvailableMilliCPU: availCPU,
		AvailableMemory:   availMem,
		AvailableStorage:  availStorage,
		AvailableScalars:  availScalars,
	}, nil
}

// GangQuorumCheckResult captures the aggregate cluster-wide viability verdict.
type GangQuorumCheckResult struct {
	ViableNodeCount int
	TotalRequiredMembers int
	TotalPreemptibleCPU int64
	TotalPreemptibleMem int64
	TotalPreemptibleStorage int64
	TotalPreemptibleScalars map[v1.ResourceName]int64
	TotalRequiredCPU int64
	TotalRequiredMem int64
	TotalRequiredStorage int64
	TotalRequiredScalars map[v1.ResourceName]int64
	Passed bool
	Status *fwk.Status
}

// CheckGangQuorumAndCapacityFeasibility executes Idea 15 (Speculative Early-Exit Gang Quorum
// & Knapsack Suffix Feasibility Checker) across candidate nodes.
//
// Immediately following Layer 1 coarse-grained headroom checks:
// 1. Quorum Bound: If viableNodeCount < minMember, fast-fail immediate abort in O(1) time.
// 2. Aggregate Knapsack Bound: If sum(Allocatable_n - NonPreemptible_n) < sum(Request_p)
//    for any requested resource (CPU, Memory, Storage, GPUs/Scalars), fast-fail immediate abort.
func CheckGangQuorumAndCapacityFeasibility(
	gangPods []*v1.Pod,
	minMember int,
	viableHeadrooms []*ViableNodeHeadroom,
	enablePodLevelResources bool,
) *GangQuorumCheckResult {
	if minMember <= 0 {
		minMember = len(gangPods)
	}

	res := &GangQuorumCheckResult{
		ViableNodeCount:      len(viableHeadrooms),
		TotalRequiredMembers: minMember,
		TotalPreemptibleScalars: make(map[v1.ResourceName]int64),
		TotalRequiredScalars:    make(map[v1.ResourceName]int64),
	}

	// 1. Cluster-wide viable node quorum check: N_viable < minMember
	if len(viableHeadrooms) < minMember {
		res.Passed = false
		res.Status = fwk.NewStatus(
			fwk.Unschedulable,
			fmt.Sprintf("insufficient viable candidate nodes across cluster: %d viable nodes < %d required minMember", len(viableHeadrooms), minMember),
		)
		return res
	}

	// Aggregate available preemptible capacity across all viable nodes with saturating arithmetic
	for _, h := range viableHeadrooms {
		res.TotalPreemptibleCPU = saturatingAdd(res.TotalPreemptibleCPU, h.AvailableMilliCPU)
		res.TotalPreemptibleMem = saturatingAdd(res.TotalPreemptibleMem, h.AvailableMemory)
		res.TotalPreemptibleStorage = saturatingAdd(res.TotalPreemptibleStorage, h.AvailableStorage)
		for sName, sVal := range h.AvailableScalars {
			res.TotalPreemptibleScalars[sName] = saturatingAdd(res.TotalPreemptibleScalars[sName], sVal)
		}
	}

	// Aggregate required capacity across the entire gang
	for _, p := range gangPods {
		pReq := resource.PodRequests(p, resource.PodResourcesOptions{
			SkipPodLevelResources: !enablePodLevelResources,
		})
		res.TotalRequiredCPU = saturatingAdd(res.TotalRequiredCPU, pReq.Cpu().MilliValue())
		res.TotalRequiredMem = saturatingAdd(res.TotalRequiredMem, pReq.Memory().Value())
		res.TotalRequiredStorage = saturatingAdd(res.TotalRequiredStorage, pReq.StorageEphemeral().Value())
		for rName, qty := range pReq {
			if rName == v1.ResourceCPU || rName == v1.ResourceMemory || rName == v1.ResourceEphemeralStorage {
				continue
			}
			val := qty.Value()
			if val > 0 {
				res.TotalRequiredScalars[rName] = saturatingAdd(res.TotalRequiredScalars[rName], val)
			}
		}
	}

	// 2. Aggregate Scalar Capacity Knapsack Bound checks
	if res.TotalPreemptibleCPU < res.TotalRequiredCPU {
		res.Passed = false
		res.Status = fwk.NewStatus(
			fwk.Unschedulable,
			fmt.Sprintf("cluster aggregate preemptible cpu headroom insufficient for gang: %dm available < %dm required", res.TotalPreemptibleCPU, res.TotalRequiredCPU),
		)
		return res
	}

	if res.TotalPreemptibleMem < res.TotalRequiredMem {
		res.Passed = false
		res.Status = fwk.NewStatus(
			fwk.Unschedulable,
			fmt.Sprintf("cluster aggregate preemptible memory headroom insufficient for gang: %d bytes available < %d bytes required", res.TotalPreemptibleMem, res.TotalRequiredMem),
		)
		return res
	}

	if res.TotalRequiredStorage > 0 && res.TotalPreemptibleStorage < res.TotalRequiredStorage {
		res.Passed = false
		res.Status = fwk.NewStatus(
			fwk.Unschedulable,
			fmt.Sprintf("cluster aggregate preemptible ephemeral-storage headroom insufficient for gang: %d bytes available < %d bytes required", res.TotalPreemptibleStorage, res.TotalRequiredStorage),
		)
		return res
	}

	for sName, reqVal := range res.TotalRequiredScalars {
		availVal := res.TotalPreemptibleScalars[sName]
		if availVal < reqVal {
			res.Passed = false
			res.Status = fwk.NewStatus(
				fwk.Unschedulable,
				fmt.Sprintf("cluster aggregate preemptible %s headroom insufficient for gang: %d available < %d required", sName, availVal, reqVal),
			)
			return res
		}
	}

	res.Passed = true
	res.Status = fwk.NewStatus(fwk.Success)
	return res
}
