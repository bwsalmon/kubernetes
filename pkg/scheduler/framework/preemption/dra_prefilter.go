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

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/dynamicresources"
)

// NodeDRAInventory summarizes the DRA driver and device pool capacities available on a candidate node.
type NodeDRAInventory struct {
	// Drivers is the set of DRA drivers publishing devices available on this node.
	Drivers sets.Set[string]

	// TotalDevicesByDriver maps driver name -> count of devices accessible on this node.
	TotalDevicesByDriver map[string]int

	// TotalDevicesByPool maps "driver/pool" -> count of devices accessible on this node.
	TotalDevicesByPool map[string]int

	// TotalDevices is the aggregate count of devices across all drivers on this node.
	TotalDevices int
}

// SliceMatchesNode determines whether a ResourceSlice is applicable to a given node.
func SliceMatchesNode(slice *resourceapi.ResourceSlice, node *v1.Node) bool {
	if slice == nil || node == nil {
		return false
	}
	if slice.Spec.NodeName != nil && *slice.Spec.NodeName == node.Name {
		return true
	}
	if slice.Spec.AllNodes != nil && *slice.Spec.AllNodes {
		return true
	}
	if slice.Spec.NodeSelector != nil {
		selector, err := nodeaffinity.NewNodeSelector(slice.Spec.NodeSelector)
		if err == nil && selector.Match(node) {
			return true
		}
	}
	if slice.Spec.PerDeviceNodeSelection != nil && *slice.Spec.PerDeviceNodeSelection {
		return true
	}
	return false
}

// DeviceMatchesNode determines whether an individual Device in a ResourceSlice is applicable to a node.
func DeviceMatchesNode(device *resourceapi.Device, node *v1.Node) bool {
	if device == nil || node == nil {
		return false
	}
	// If per-device node selection fields are unset, the device inherits slice-level node matching.
	if device.NodeName == nil && device.AllNodes == nil && device.NodeSelector == nil {
		return true
	}
	if device.NodeName != nil && *device.NodeName == node.Name {
		return true
	}
	if device.AllNodes != nil && *device.AllNodes {
		return true
	}
	if device.NodeSelector != nil {
		selector, err := nodeaffinity.NewNodeSelector(device.NodeSelector)
		if err == nil && selector.Match(node) {
			return true
		}
	}
	return false
}

// BuildNodeDRAInventory constructs an inventory of DRA devices available on a candidate node in O(1) time.
func BuildNodeDRAInventory(node *v1.Node, slices []*resourceapi.ResourceSlice) *NodeDRAInventory {
	inv := &NodeDRAInventory{
		Drivers:              sets.New[string](),
		TotalDevicesByDriver: make(map[string]int),
		TotalDevicesByPool:   make(map[string]int),
	}
	if node == nil {
		return inv
	}

	for _, slice := range slices {
		if !SliceMatchesNode(slice, node) {
			continue
		}
		driver := slice.Spec.Driver
		poolKey := driver + "/" + slice.Spec.Pool.Name

		deviceCount := 0
		for i := range slice.Spec.Devices {
			if DeviceMatchesNode(&slice.Spec.Devices[i], node) {
				deviceCount++
			}
		}

		if deviceCount > 0 {
			inv.Drivers.Insert(driver)
			inv.TotalDevicesByDriver[driver] += deviceCount
			inv.TotalDevicesByPool[poolKey] += deviceCount
			inv.TotalDevices += deviceCount
		}
	}

	return inv
}

// countNonPreemptibleDevices calculates devices held by pods on nodeInfo that cannot be preempted.
func countNonPreemptibleDevices(
	preemptor *v1.Pod,
	nodeInfo fwk.NodeInfo,
	draManager fwk.SharedDRAManager,
	podGroupSnapshot fwk.PodGroupLister,
	compositePodGroupSnapshot fwk.CompositePodGroupLister,
) map[string]int {
	nonPreemptibleDevices := make(map[string]int)
	if nodeInfo == nil || draManager == nil || draManager.ResourceClaims() == nil {
		return nonPreemptibleDevices
	}

	preemptorPriority := getPodPriority(preemptor, podGroupSnapshot, compositePodGroupSnapshot)

	for _, pi := range nodeInfo.GetPods() {
		p := pi.GetPod()
		if p == nil {
			continue
		}
		// Skip self
		if (len(preemptor.UID) > 0 && p.UID == preemptor.UID) || (p.Name == preemptor.Name && p.Namespace == preemptor.Namespace) {
			continue
		}

		priority := getPodPriority(p, podGroupSnapshot, compositePodGroupSnapshot)
		// Pod is non-preemptible if priority >= preemptor priority or PreemptNever
		if priority >= preemptorPriority || (p.Spec.PreemptionPolicy != nil && *p.Spec.PreemptionPolicy == v1.PreemptNever) {
			for _, podClaim := range p.Spec.ResourceClaims {
				claimName := podClaim.Name
				claim, err := draManager.ResourceClaims().Get(p.Namespace, claimName)
				if err != nil && podClaim.ResourceClaimName != nil {
					claim, err = draManager.ResourceClaims().Get(p.Namespace, *podClaim.ResourceClaimName)
				}
				if err == nil && claim != nil && claim.Status.Allocation != nil {
					for _, res := range claim.Status.Allocation.Devices.Results {
						nonPreemptibleDevices[res.Driver]++
					}
				}
			}
		}
	}

	return nonPreemptibleDevices
}

// DRADevicePoolPreFilter evaluates per-node driver and device pool feasibility in O(1) time
// before invoking dry-run preemption simulations for pods requesting ResourceClaims (Idea 11).
func DRADevicePoolPreFilter(
	logger klog.Logger,
	state fwk.CycleState,
	pod *v1.Pod,
	nodeInfo fwk.NodeInfo,
	draManager fwk.SharedDRAManager,
	podGroupSnapshot fwk.PodGroupLister,
	compositePodGroupSnapshot fwk.CompositePodGroupLister,
) (bool, *fwk.Status) {
	// Invariant 5: Lazy evaluation - zero overhead on standard pods without ResourceClaims
	if pod == nil || len(pod.Spec.ResourceClaims) == 0 {
		return true, nil
	}
	if draManager == nil {
		return true, nil
	}
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return false, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "Node or pod is nil")
	}

	node := nodeInfo.Node()

	// Extract requested DRA requirements and canonical claim template signature
	reqs, sig, err := dynamicresources.ExtractPodDRARequirements(pod, draManager)
	if err != nil {
		return false, fwk.NewStatus(fwk.Error, fmt.Sprintf("failed to extract pod DRA requirements: %v", err))
	}
	if len(reqs) == 0 {
		return true, nil
	}

	// Layer 2A: Check cycle-scoped ClaimTemplateDriverCache on PodGroupCycleState
	var cache *dynamicresources.ClaimTemplateDriverCache
	if state != nil {
		cache = dynamicresources.GetOrCreateClaimTemplateDriverCache(state)
		if feas, hit := cache.GetFeasibility(sig, node.Name); hit {
			if feas.Feasible {
				return true, nil
			}
			return false, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, feas.Reason)
		}
	}

	// Aggregate requirements per driver
	requiredByDriver := make(map[string]int)
	for _, r := range reqs {
		requiredByDriver[r.Driver] += r.Count
	}

	// Obtain immutable ResourceSlice snapshot from scheduler cache (Invariant 2: Zero Mutated Global State)
	slices, err := draManager.ResourceSlices().ListWithDeviceTaintRules()
	if err != nil {
		return false, fwk.NewStatus(fwk.Error, fmt.Sprintf("failed to list resource slices: %v", err))
	}

	inventory := BuildNodeDRAInventory(node, slices)
	nonPreemptibleDevices := countNonPreemptibleDevices(pod, nodeInfo, draManager, podGroupSnapshot, compositePodGroupSnapshot)

	// Step 1: O(1) Driver presence check
	// Step 2: Total allocatable device count check
	// Step 3: Disruption accounting (Headroom after subtracting non-preemptible pods)
	for driver, reqCount := range requiredByDriver {
		if !inventory.Drivers.Has(driver) {
			reason := fmt.Sprintf("node %q does not host required DRA driver %q", node.Name, driver)
			if cache != nil {
				cache.SetFeasibility(sig, node.Name, &dynamicresources.ClaimDriverFeasibility{
					Feasible:      false,
					Reason:        reason,
					Driver:        driver,
					RequiredCount: reqCount,
				})
			}
			return false, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, reason)
		}

		total := inventory.TotalDevicesByDriver[driver]
		if total < reqCount {
			reason := fmt.Sprintf("insufficient total DRA devices: node %q has %d total from driver %q, required %d", node.Name, total, driver, reqCount)
			if cache != nil {
				cache.SetFeasibility(sig, node.Name, &dynamicresources.ClaimDriverFeasibility{
					Feasible:      false,
					Reason:        reason,
					Driver:        driver,
					RequiredCount: reqCount,
				})
			}
			return false, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, reason)
		}

		nonPreempt := nonPreemptibleDevices[driver]
		available := total - nonPreempt
		if available < reqCount {
			reason := fmt.Sprintf("insufficient DRA device headroom: node %q has %d available from driver %q after subtracting non-preemptible pods, required %d", node.Name, available, driver, reqCount)
			if cache != nil {
				cache.SetFeasibility(sig, node.Name, &dynamicresources.ClaimDriverFeasibility{
					Feasible:      false,
					Reason:        reason,
					Driver:        driver,
					RequiredCount: reqCount,
				})
			}
			return false, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, reason)
		}
	}

	// Cache positive feasibility result
	if cache != nil {
		cache.SetFeasibility(sig, node.Name, &dynamicresources.ClaimDriverFeasibility{
			Feasible: true,
		})
	}

	return true, nil
}

// CanNodeFitPreemptorHeadroomWithDRA combines scalar resource headroom checks with
// DRA structured resource claim allocation pre-filtering.
func CanNodeFitPreemptorHeadroomWithDRA(
	logger klog.Logger,
	state fwk.CycleState,
	pod *v1.Pod,
	podReq v1.ResourceList,
	nodeInfo fwk.NodeInfo,
	draManager fwk.SharedDRAManager,
	podGroupSnapshot fwk.PodGroupLister,
	compositePodGroupSnapshot fwk.CompositePodGroupLister,
	enablePodLevelResources bool,
) (bool, *fwk.Status) {
	canFitScalar, scalarStatus := CanNodeFitPreemptorHeadroomWithRequests(
		logger,
		pod,
		podReq,
		nodeInfo,
		podGroupSnapshot,
		compositePodGroupSnapshot,
		enablePodLevelResources,
	)
	if !canFitScalar {
		return false, scalarStatus
	}

	return DRADevicePoolPreFilter(
		logger,
		state,
		pod,
		nodeInfo,
		draManager,
		podGroupSnapshot,
		compositePodGroupSnapshot,
	)
}
