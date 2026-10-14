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
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/types"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/component-helpers/resource"
	"k8s.io/kubernetes/pkg/scheduler/framework/preemption"
)

const (
	// PolicySingleNumaNode matches Kubelet topology manager policy "single-numa-node" (KEP-693 / KEP-3633).
	PolicySingleNumaNode = "single-numa-node"

	// Standard annotations for NUMA / PCIe locality and topology policy.
	AnnotationTopologyPolicy        = "topology.kubernetes.io/policy"
	AnnotationTopologyManagerPolicy = "topology-manager.kubernetes.io/policy"
	AnnotationNUMANode              = "topology.kubernetes.io/numa-node"
	AnnotationNUMASocket            = "numa.topology.kubernetes.io/socket"
	AnnotationNUMANodeID            = "topology.node.kubernetes.io/numa-id"
	AnnotationPCIeSwitch            = "topology.kubernetes.io/pcie-switch"
	AnnotationPCIeSwitchAlignment   = "topology.kubernetes.io/pcie-switch-alignment"
	AnnotationAllocatedDevices      = "device.k8s.io/allocated-devices"

	// Standard accelerator resource names.
	ResourceNVIDIAGPU v1.ResourceName = "nvidia.com/gpu"
	ResourceGPU       v1.ResourceName = "gpu"
)

// HardwareLocalityDomain describes a physical hardware locality scope on a node.
type HardwareLocalityDomain struct {
	SocketID     int    `json:"socketID"`
	PCIeSwitchID string `json:"pcieSwitchID,omitempty"`
	DomainID     string `json:"domainID"` // Unique identifier e.g. "socket-0", "socket-0/switch-0"
}

// FormatDomainID generates a canonical domain identifier.
func FormatDomainID(socketID int, pcieSwitchID string) string {
	if pcieSwitchID != "" {
		return fmt.Sprintf("socket-%d/switch-%s", socketID, pcieSwitchID)
	}
	return fmt.Sprintf("socket-%d", socketID)
}

// NodeHardwareTopology holds the topology information of a node: its NUMA sockets and PCIe switches,
// along with allocatable resources per domain.
type NodeHardwareTopology struct {
	NodeName string
	Domains  []HardwareLocalityDomain
	// Capacity per domain: domainID -> resourceName -> capacity quantity
	DomainAllocatable map[string]map[v1.ResourceName]int64
	// Sockets count
	NumSockets int
}

// NodeResourceTopologyCache tracks device allocations and locality domains across nodes and pods.
type NodeResourceTopologyCache interface {
	GetPodLocality(nodeName string, podUID types.UID) (*HardwareLocalityDomain, bool)
	SetPodLocality(nodeName string, podUID types.UID, domain HardwareLocalityDomain)
	RemovePodLocality(nodeName string, podUID types.UID)
	GetNodeTopology(nodeName string) (*NodeHardwareTopology, bool)
	SetNodeTopology(nodeName string, topo *NodeHardwareTopology)
}

// DefaultNodeResourceTopologyCache provides a thread-safe in-memory cache for node and pod topology.
type DefaultNodeResourceTopologyCache struct {
	mu         sync.RWMutex
	podDomains map[string]map[types.UID]HardwareLocalityDomain
	nodeTopos  map[string]*NodeHardwareTopology
}

// NewNodeResourceTopologyCache constructs a new DefaultNodeResourceTopologyCache.
func NewNodeResourceTopologyCache() *DefaultNodeResourceTopologyCache {
	return &DefaultNodeResourceTopologyCache{
		podDomains: make(map[string]map[types.UID]HardwareLocalityDomain),
		nodeTopos:  make(map[string]*NodeHardwareTopology),
	}
}

// GetPodLocality returns the cached locality domain for a pod on a node.
func (c *DefaultNodeResourceTopologyCache) GetPodLocality(nodeName string, podUID types.UID) (*HardwareLocalityDomain, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	pods, ok := c.podDomains[nodeName]
	if !ok {
		return nil, false
	}
	dom, ok := pods[podUID]
	if !ok {
		return nil, false
	}
	res := dom
	return &res, true
}

// SetPodLocality records the locality domain for a pod on a node.
func (c *DefaultNodeResourceTopologyCache) SetPodLocality(nodeName string, podUID types.UID, domain HardwareLocalityDomain) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	pods, ok := c.podDomains[nodeName]
	if !ok {
		pods = make(map[types.UID]HardwareLocalityDomain)
		c.podDomains[nodeName] = pods
	}
	pods[podUID] = domain
}

// RemovePodLocality deletes a pod's locality entry from the cache.
func (c *DefaultNodeResourceTopologyCache) RemovePodLocality(nodeName string, podUID types.UID) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if pods, ok := c.podDomains[nodeName]; ok {
		delete(pods, podUID)
		if len(pods) == 0 {
			delete(c.podDomains, nodeName)
		}
	}
}

// GetNodeTopology retrieves the hardware topology for a node.
func (c *DefaultNodeResourceTopologyCache) GetNodeTopology(nodeName string) (*NodeHardwareTopology, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	topo, ok := c.nodeTopos[nodeName]
	return topo, ok
}

// SetNodeTopology sets the hardware topology for a node.
func (c *DefaultNodeResourceTopologyCache) SetNodeTopology(nodeName string, topo *NodeHardwareTopology) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodeTopos[nodeName] = topo
}

// ExtractPodLocality returns the hardware locality domain for a pod.
// It checks the cache first; if not found, it inspects standard annotations on the pod.
func ExtractPodLocality(pod *v1.Pod, cache NodeResourceTopologyCache) *HardwareLocalityDomain {
	if pod == nil {
		return nil
	}
	if cache != nil && pod.Spec.NodeName != "" {
		if dom, ok := cache.GetPodLocality(pod.Spec.NodeName, pod.UID); ok {
			return dom
		}
	}

	// Inspect pod annotations.
	ann := pod.Annotations
	if ann == nil {
		return nil
	}

	var socketID *int
	for _, key := range []string{AnnotationNUMANode, AnnotationNUMASocket, AnnotationNUMANodeID} {
		if val, ok := ann[key]; ok {
			if parsed, err := strconv.Atoi(strings.TrimSpace(val)); err == nil {
				socketID = &parsed
				break
			}
		}
	}

	var pcieSwitch string
	if val, ok := ann[AnnotationPCIeSwitch]; ok {
		pcieSwitch = strings.TrimSpace(val)
	}

	if socketID != nil {
		return &HardwareLocalityDomain{
			SocketID:     *socketID,
			PCIeSwitchID: pcieSwitch,
			DomainID:     FormatDomainID(*socketID, pcieSwitch),
		}
	}

	if pcieSwitch != "" {
		return &HardwareLocalityDomain{
			SocketID:     0,
			PCIeSwitchID: pcieSwitch,
			DomainID:     FormatDomainID(0, pcieSwitch),
		}
	}

	return nil
}

// IsSingleNUMAAlignmentRequired checks if the preemptor pod requires SingleNUMANode or single-PCIe alignment.
func IsSingleNUMAAlignmentRequired(pod *v1.Pod, nodeInfo fwk.NodeInfo) bool {
	if pod == nil {
		return false
	}
	ann := pod.Annotations

	// 1. Explicit pod-level topology policy annotation.
	if ann != nil {
		for _, key := range []string{AnnotationTopologyPolicy, AnnotationTopologyManagerPolicy} {
			if val, ok := ann[key]; ok && strings.EqualFold(strings.TrimSpace(val), PolicySingleNumaNode) {
				return true
			}
		}
		if val, ok := ann[AnnotationPCIeSwitchAlignment]; ok {
			s := strings.ToLower(strings.TrimSpace(val))
			if s == "true" || s == "required" {
				return true
			}
		}
	}

	// 2. Node-level topology manager policy: if node enforces single-numa-node and pod requests accelerators.
	if nodeInfo != nil && nodeInfo.Node() != nil {
		nodeAnn := nodeInfo.Node().Annotations
		if nodeAnn != nil {
			for _, key := range []string{AnnotationTopologyManagerPolicy, AnnotationTopologyPolicy} {
				if val, ok := nodeAnn[key]; ok && strings.EqualFold(strings.TrimSpace(val), PolicySingleNumaNode) {
					// Check if pod requests accelerators or multi-GPU resources.
					podReq := resource.PodRequests(pod, resource.PodResourcesOptions{})
					if q, ok := podReq[ResourceNVIDIAGPU]; ok && q.Value() > 0 {
						return true
					}
					if q, ok := podReq[ResourceGPU]; ok && q.Value() > 0 {
						return true
					}
					for rName := range podReq {
						if strings.Contains(string(rName), "gpu") || strings.Contains(string(rName), "accelerator") {
							return true
						}
					}
				}
			}
		}
	}

	return false
}

// LocalityDomainEvaluation represents the evaluation metrics of candidate preemption victims in a single locality domain.
type LocalityDomainEvaluation struct {
	Domain           HardwareLocalityDomain
	Victims          []*preemption.DomainVictim
	ViolatingCount   int
	MaxPriority      int32
	SumPriority      int64
	TotalVictimCount int
	CanSatisfy       bool
}

// NUMATopologyAlignedVictimSelector partitions victims by NUMA socket and PCIe switch locality,
// and identifies the optimal domain that satisfies the contiguous resource requirement with minimal preemption cost.
type NUMATopologyAlignedVictimSelector struct {
	cache NodeResourceTopologyCache
}

// NewNUMATopologyAlignedVictimSelector constructs a new NUMATopologyAlignedVictimSelector.
func NewNUMATopologyAlignedVictimSelector(cache NodeResourceTopologyCache) *NUMATopologyAlignedVictimSelector {
	return &NUMATopologyAlignedVictimSelector{cache: cache}
}

// EvaluateDomains groups potential victims by locality domain, checks contiguous capacity feasibility,
// and returns ranked candidate domains from lowest preemption cost to highest preemption cost.
func (s *NUMATopologyAlignedVictimSelector) EvaluateDomains(
	preemptor *v1.Pod,
	nodeInfo fwk.NodeInfo,
	potentialVictims []*preemption.DomainVictim,
	pdbs []*policy.PodDisruptionBudget,
	moreImportantVictim MoreImportantVictimFunc,
) (rankedDomains []LocalityDomainEvaluation, domainVictimsMap map[string][]*preemption.DomainVictim, unpinnedVictims []*preemption.DomainVictim) {
	if len(potentialVictims) == 0 {
		return nil, nil, nil
	}

	domainVictimsMap = make(map[string][]*preemption.DomainVictim)
	domainsByID := make(map[string]HardwareLocalityDomain)

	// Step 1: Partition candidate victims into hardware locality domains.
	for _, v := range potentialVictims {
		var domain *HardwareLocalityDomain
		for _, pi := range v.Pods() {
			d := ExtractPodLocality(pi.GetPod(), s.cache)
			if d != nil {
				domain = d
				break
			}
		}

		if domain != nil {
			domainVictimsMap[domain.DomainID] = append(domainVictimsMap[domain.DomainID], v)
			domainsByID[domain.DomainID] = *domain
		} else {
			unpinnedVictims = append(unpinnedVictims, v)
		}
	}

	// If no victims are pinned to a hardware locality domain, cannot slice by NUMA domain.
	if len(domainVictimsMap) == 0 {
		return nil, domainVictimsMap, unpinnedVictims
	}

	// Calculate preemptor resource requirements.
	podReq := resource.PodRequests(preemptor, resource.PodResourcesOptions{})
	reqCPU := podReq.Cpu().MilliValue()
	reqMem := podReq.Memory().Value()
	reqScalars := make(map[v1.ResourceName]int64)
	for rName, qty := range podReq {
		if rName == v1.ResourceCPU || rName == v1.ResourceMemory || rName == v1.ResourceEphemeralStorage {
			continue
		}
		if qty.Value() > 0 {
			reqScalars[rName] = qty.Value()
		}
	}

	// Retrieve node hardware topology if cached.
	var nodeTopo *NodeHardwareTopology
	if s.cache != nil && nodeInfo != nil && nodeInfo.Node() != nil {
		nodeTopo, _ = s.cache.GetNodeTopology(nodeInfo.Node().Name)
	}

	// Step 2: Evaluate each locality domain independently.
	var evaluations []LocalityDomainEvaluation

	for domainID, domVictims := range domainVictimsMap {
		dom := domainsByID[domainID]

		// Calculate total available resources in this domain (Headroom + Reclaimable from candidate victims).
		var availCPU, availMem int64
		availScalars := make(map[v1.ResourceName]int64)

		if nodeTopo != nil && nodeTopo.DomainAllocatable != nil {
			if caps, ok := nodeTopo.DomainAllocatable[domainID]; ok {
				availCPU = caps[v1.ResourceCPU]
				availMem = caps[v1.ResourceMemory]
				for sName, sVal := range caps {
					if sName != v1.ResourceCPU && sName != v1.ResourceMemory {
						availScalars[sName] = sVal
					}
				}
				// Subtract resources consumed by non-candidate running pods in this domain.
				if nodeInfo != nil {
					for _, pi := range nodeInfo.GetPods() {
						if pi == nil || pi.GetPod() == nil {
							continue
						}
						// If this pod is a candidate victim, its resource will be accounted as reclaimable.
						isCandidate := false
						for _, cv := range domVictims {
							for _, cpi := range cv.Pods() {
								if cpi.GetPod() != nil && cpi.GetPod().UID == pi.GetPod().UID {
									isCandidate = true
									break
								}
							}
							if isCandidate {
								break
							}
						}
						if !isCandidate {
							podDom := ExtractPodLocality(pi.GetPod(), s.cache)
							if podDom != nil && podDom.DomainID == domainID {
								calcRes := pi.CalculateResource()
								if calcRes.Resource != nil {
									availCPU -= calcRes.Resource.GetMilliCPU()
									availMem -= calcRes.Resource.GetMemory()
									for sName, sVal := range calcRes.Resource.GetScalarResources() {
										availScalars[sName] -= sVal
									}
								}
							}
						}
					}
				}
			}
		} else {
			// Without explicit per-domain cache, calculate reclaimable resources strictly from candidate victims.
			for _, v := range domVictims {
				for _, pi := range v.Pods() {
					calcRes := pi.CalculateResource()
					if calcRes.Resource != nil {
						availCPU += calcRes.Resource.GetMilliCPU()
						availMem += calcRes.Resource.GetMemory()
						for sName, sVal := range calcRes.Resource.GetScalarResources() {
							availScalars[sName] += sVal
						}
					}
				}
			}
			// Add any unallocated resources on the node proportionally or from nodeInfo headroom.
			if nodeInfo != nil && nodeInfo.GetAllocatable() != nil && nodeInfo.GetRequested() != nil {
				numDomains := int64(len(domainVictimsMap))
				if numDomains == 0 {
					numDomains = 1
				}
				nodeFreeCPU := nodeInfo.GetAllocatable().GetMilliCPU() - nodeInfo.GetRequested().GetMilliCPU()
				if nodeFreeCPU > 0 {
					availCPU += nodeFreeCPU / numDomains
				}
				nodeFreeMem := nodeInfo.GetAllocatable().GetMemory() - nodeInfo.GetRequested().GetMemory()
				if nodeFreeMem > 0 {
					availMem += nodeFreeMem / numDomains
				}
				for rName := range reqScalars {
					allocVal := int64(0)
					reqVal := int64(0)
					if nodeInfo.GetAllocatable().GetScalarResources() != nil {
						allocVal = nodeInfo.GetAllocatable().GetScalarResources()[rName]
					}
					if nodeInfo.GetRequested().GetScalarResources() != nil {
						reqVal = nodeInfo.GetRequested().GetScalarResources()[rName]
					}
					freeVal := allocVal - reqVal
					if freeVal > 0 {
						availScalars[rName] += freeVal / numDomains
					}
				}
			}
		}

		// Also unpinned candidate victims can contribute to satisfying deficits.
		for _, uv := range unpinnedVictims {
			for _, pi := range uv.Pods() {
				calcRes := pi.CalculateResource()
				if calcRes.Resource != nil {
					availCPU += calcRes.Resource.GetMilliCPU()
					availMem += calcRes.Resource.GetMemory()
					for sName, sVal := range calcRes.Resource.GetScalarResources() {
						availScalars[sName] += sVal
					}
				}
			}
		}

		// Check if domain can satisfy preemptor requests.
		canSatisfy := true
		if reqCPU > availCPU || reqMem > availMem {
			canSatisfy = false
		}
		for sName, sReq := range reqScalars {
			if availScalars[sName] < sReq {
				canSatisfy = false
				break
			}
		}

		if !canSatisfy {
			continue
		}

		// Domain is feasible! Calculate its aggregate preemption cost.
		// Sort victims descending by importance.
		sortedVictims := make([]*preemption.DomainVictim, len(domVictims))
		copy(sortedVictims, domVictims)
		sort.Slice(sortedVictims, func(i, j int) bool {
			return moreImportantVictim(sortedVictims[i], sortedVictims[j])
		})

		violating, _ := preemption.FilterVictimsWithPDBViolation(sortedVictims, pdbs)
		violatingCount := 0
		for _, vv := range violating {
			violatingCount += vv.ViolateCount
		}

		var maxPriority int32 = -2147483648
		var sumPriority int64 = 0
		for _, v := range sortedVictims {
			prio := v.Priority()
			if prio > maxPriority {
				maxPriority = prio
			}
			sumPriority += int64(prio)
		}
		if len(sortedVictims) == 0 {
			maxPriority = 0
		}

		eval := LocalityDomainEvaluation{
			Domain:           dom,
			Victims:          sortedVictims,
			ViolatingCount:   violatingCount,
			MaxPriority:      maxPriority,
			SumPriority:      sumPriority,
			TotalVictimCount: len(sortedVictims),
			CanSatisfy:       true,
		}
		evaluations = append(evaluations, eval)
	}

	if len(evaluations) == 0 {
		return nil, domainVictimsMap, unpinnedVictims
	}

	// Step 3: Rank feasible domains by preemption cost:
	// 1. Minimum PDB violations (0 violations strictly prioritized).
	// 2. Minimum highest priority (strict priority preservation).
	// 3. Minimum sum of priorities.
	// 4. Minimum victim count.
	// 5. Deterministic tie-breaker: DomainID string ordering.
	sort.Slice(evaluations, func(i, j int) bool {
		a, b := evaluations[i], evaluations[j]
		if a.ViolatingCount != b.ViolatingCount {
			return a.ViolatingCount < b.ViolatingCount
		}
		if a.MaxPriority != b.MaxPriority {
			return a.MaxPriority < b.MaxPriority
		}
		if a.SumPriority != b.SumPriority {
			return a.SumPriority < b.SumPriority
		}
		if a.TotalVictimCount != b.TotalVictimCount {
			return a.TotalVictimCount < b.TotalVictimCount
		}
		return a.Domain.DomainID < b.Domain.DomainID
	})

	return evaluations, domainVictimsMap, unpinnedVictims
}
