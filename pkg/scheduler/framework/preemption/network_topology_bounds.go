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
)

// Standard hierarchical topology labels used across AI/ML Kubernetes clusters.
const (
	LabelTopologyDataCenter = "topology.kubernetes.io/datacenter"
	LabelTopologySpinePod   = "topology.kubernetes.io/network-pod"
	LabelTopologySpine      = "network.tier/spine"
	LabelTopologyLeafRack   = "topology.kubernetes.io/network-rack"
	LabelTopologyLeaf       = "network.tier/leaf"
	LabelTopologyZone       = "topology.kubernetes.io/zone"
)

// Network fabric hop distance constants.
// Reflects physical leaf/spine switching tiers:
// HopSameNode: 0 (local memory / NVLink local)
// HopSameLeaf: 1 (same TOR/leaf switch - line rate)
// HopSameSpine: 2 (same spine switch / network pod)
// HopCrossFabric: 3 (cross-spine / cross-fabric)
const (
	HopSameNode    int = 0
	HopSameLeaf    int = 1
	HopSameSpine   int = 2
	HopCrossFabric int = 3
)

// NetworkTopologyCoordinate represents hierarchical network coordinates of a physical node.
type NetworkTopologyCoordinate struct {
	DataCenter string
	SpinePod   string // Spine tier or pod domain (e.g. spine group)
	LeafRack   string // Leaf / TOR switch rack
	NodeName   string
}

// ExtractNetworkCoordinate extracts hierarchical network coordinates from node labels.
func ExtractNetworkCoordinate(node *v1.Node) NetworkTopologyCoordinate {
	if node == nil {
		return NetworkTopologyCoordinate{}
	}
	labels := node.Labels
	coord := NetworkTopologyCoordinate{
		NodeName: node.Name,
	}
	if labels == nil {
		return coord
	}

	// 1. DataCenter / Zone tier
	if dc, ok := labels[LabelTopologyDataCenter]; ok {
		coord.DataCenter = dc
	} else if zone, ok := labels[LabelTopologyZone]; ok {
		coord.DataCenter = zone
	}

	// 2. Spine tier
	if spine, ok := labels[LabelTopologySpine]; ok {
		coord.SpinePod = spine
	} else if spinePod, ok := labels[LabelTopologySpinePod]; ok {
		coord.SpinePod = spinePod
	}

	// 3. Leaf / TOR tier
	if leaf, ok := labels[LabelTopologyLeaf]; ok {
		coord.LeafRack = leaf
	} else if rack, ok := labels[LabelTopologyLeafRack]; ok {
		coord.LeafRack = rack
	}

	return coord
}

// FabricDistance calculates physical hop distance between two network coordinates:
// 0: same host
// 1: same TOR/leaf rack
// 2: same spine/network pod
// 3: cross-spine / cross-fabric / different datacenter
func FabricDistance(c1, c2 NetworkTopologyCoordinate) int {
	if c1.NodeName != "" && c1.NodeName == c2.NodeName {
		return HopSameNode
	}
	// Check DataCenter
	if c1.DataCenter != "" && c2.DataCenter != "" && c1.DataCenter != c2.DataCenter {
		return HopCrossFabric
	}
	// Check SpinePod
	if c1.SpinePod != "" && c2.SpinePod != "" && c1.SpinePod != c2.SpinePod {
		return HopCrossFabric
	}
	// Check LeafRack
	if c1.LeafRack != "" && c2.LeafRack != "" && c1.LeafRack != c2.LeafRack {
		return HopSameSpine
	}
	// If both have LeafRack and they match
	if c1.LeafRack != "" && c1.LeafRack == c2.LeafRack {
		return HopSameLeaf
	}
	// If LeafRack is unknown but Spine matches
	if c1.SpinePod != "" && c1.SpinePod == c2.SpinePod {
		return HopSameSpine
	}
	// Fallback
	return HopCrossFabric
}

// FabricDistanceMatrix stores pre-indexed O(1) distances between candidate nodes.
type FabricDistanceMatrix struct {
	nodeToIndex map[string]int
	distances   []int
	size        int
}

// NewFabricDistanceMatrix builds an O(1) pairwise distance lookup matrix for candidate nodes.
func NewFabricDistanceMatrix(nodes []string, coords map[string]NetworkTopologyCoordinate) *FabricDistanceMatrix {
	n := len(nodes)
	m := &FabricDistanceMatrix{
		nodeToIndex: make(map[string]int, n),
		distances:   make([]int, n*n),
		size:        n,
	}
	for i, name := range nodes {
		m.nodeToIndex[name] = i
	}
	for i := 0; i < n; i++ {
		c1 := coords[nodes[i]]
		for j := i; j < n; j++ {
			c2 := coords[nodes[j]]
			d := FabricDistance(c1, c2)
			m.distances[i*n+j] = d
			m.distances[j*n+i] = d
		}
	}
	return m
}

// Distance returns the fabric hop distance between nodeA and nodeB in O(1) time.
func (m *FabricDistanceMatrix) Distance(nodeA, nodeB string) int {
	if m == nil || m.size == 0 {
		if nodeA == nodeB {
			return HopSameNode
		}
		return HopCrossFabric
	}
	idxA, okA := m.nodeToIndex[nodeA]
	idxB, okB := m.nodeToIndex[nodeB]
	if !okA || !okB {
		if nodeA == nodeB {
			return HopSameNode
		}
		return HopCrossFabric
	}
	return m.distances[idxA*m.size+idxB]
}

// CollectiveCommunicationBoundingSpec specifies network topology constraints for gang preemption.
type CollectiveCommunicationBoundingSpec struct {
	// MaxDiameter is the maximum allowable pairwise hop distance across all assigned nodes (0, 1, 2, or 3).
	// If MaxDiameter < 0, diameter bounding is disabled.
	MaxDiameter int

	// NodeCoordinates maps node name to its physical NetworkTopologyCoordinate.
	NodeCoordinates map[string]NetworkTopologyCoordinate

	// DistanceMatrix is a precomputed O(1) distance matrix.
	DistanceMatrix *FabricDistanceMatrix

	// OptimizeDiameter: if true, network diameter is minimized as a secondary objective.
	OptimizeDiameter bool
}

// NewCollectiveCommunicationBoundingSpec creates a spec with initialized distance matrix.
func NewCollectiveCommunicationBoundingSpec(maxDiameter int, coords map[string]NetworkTopologyCoordinate, candidateNodes []string) *CollectiveCommunicationBoundingSpec {
	spec := &CollectiveCommunicationBoundingSpec{
		MaxDiameter:      maxDiameter,
		NodeCoordinates:  coords,
		OptimizeDiameter: true,
	}
	if coords != nil && len(candidateNodes) > 0 {
		spec.DistanceMatrix = NewFabricDistanceMatrix(candidateNodes, coords)
	}
	return spec
}

// ParseTopologyLabels parses standard node labels to form coordinate map.
func ParseTopologyLabels(nodeLabels map[string]map[string]string) map[string]NetworkTopologyCoordinate {
	coords := make(map[string]NetworkTopologyCoordinate, len(nodeLabels))
	for nodeName, labels := range nodeLabels {
		coord := NetworkTopologyCoordinate{
			NodeName: nodeName,
		}
		if labels != nil {
			if dc, ok := labels[LabelTopologyDataCenter]; ok {
				coord.DataCenter = dc
			} else if zone, ok := labels[LabelTopologyZone]; ok {
				coord.DataCenter = zone
			}

			if spine, ok := labels[LabelTopologySpine]; ok {
				coord.SpinePod = spine
			} else if spinePod, ok := labels[LabelTopologySpinePod]; ok {
				coord.SpinePod = spinePod
			}

			if leaf, ok := labels[LabelTopologyLeaf]; ok {
				coord.LeafRack = leaf
			} else if rack, ok := labels[LabelTopologyLeafRack]; ok {
				coord.LeafRack = rack
			}
		}
		coords[nodeName] = coord
	}
	return coords
}

// ComputePlacementDiameter computes the maximum pairwise fabric distance across assigned nodes.
func ComputePlacementDiameter(nodes []string, matrix *FabricDistanceMatrix, coords map[string]NetworkTopologyCoordinate) int {
	if len(nodes) <= 1 {
		return HopSameNode
	}
	maxD := HopSameNode
	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			var d int
			if matrix != nil {
				d = matrix.Distance(nodes[i], nodes[j])
			} else {
				d = FabricDistance(coords[nodes[i]], coords[nodes[j]])
			}
			if d > maxD {
				maxD = d
			}
		}
	}
	return maxD
}

// NetworkDiameterBoundingTracker tracks partial assignments during branch-and-bound search.
// Maintains assigned node list and current partial diameter in O(1) per step.
type NetworkDiameterBoundingTracker struct {
	spec            *CollectiveCommunicationBoundingSpec
	assignedNodes   []string
	currentDiameter int
}

// NewNetworkDiameterBoundingTracker creates a new tracker for branch-and-bound search.
func NewNetworkDiameterBoundingTracker(spec *CollectiveCommunicationBoundingSpec) *NetworkDiameterBoundingTracker {
	return &NetworkDiameterBoundingTracker{
		spec:            spec,
		assignedNodes:   make([]string, 0, 32),
		currentDiameter: HopSameNode,
	}
}

// CanAcceptNode tests whether adding candidateNode would violate the MaxDiameter bounding sphere.
// Prunes in O(|assignedNodes|) <= O(gangSize) time before simulating allocations.
func (t *NetworkDiameterBoundingTracker) CanAcceptNode(candidateNode string) bool {
	if t.spec == nil || t.spec.MaxDiameter < 0 {
		return true
	}
	maxAllowed := t.spec.MaxDiameter
	for _, assigned := range t.assignedNodes {
		var dist int
		if t.spec.DistanceMatrix != nil {
			dist = t.spec.DistanceMatrix.Distance(assigned, candidateNode)
		} else if t.spec.NodeCoordinates != nil {
			dist = FabricDistance(t.spec.NodeCoordinates[assigned], t.spec.NodeCoordinates[candidateNode])
		} else {
			if assigned != candidateNode {
				dist = HopCrossFabric
			}
		}
		if dist > maxAllowed {
			return false
		}
	}
	return true
}

// PushNode adds a node to the tracker and updates the current diameter.
func (t *NetworkDiameterBoundingTracker) PushNode(node string) {
	if t.spec != nil {
		for _, assigned := range t.assignedNodes {
			var dist int
			if t.spec.DistanceMatrix != nil {
				dist = t.spec.DistanceMatrix.Distance(assigned, node)
			} else if t.spec.NodeCoordinates != nil {
				dist = FabricDistance(t.spec.NodeCoordinates[assigned], t.spec.NodeCoordinates[node])
			} else {
				if assigned != node {
					dist = HopCrossFabric
				}
			}
			if dist > t.currentDiameter {
				t.currentDiameter = dist
			}
		}
	}
	t.assignedNodes = append(t.assignedNodes, node)
}

// PopNode removes the most recently added node and restores current diameter.
func (t *NetworkDiameterBoundingTracker) PopNode() {
	if len(t.assignedNodes) == 0 {
		return
	}
	t.assignedNodes = t.assignedNodes[:len(t.assignedNodes)-1]
	// Recompute diameter across remaining assigned nodes
	t.currentDiameter = HopSameNode
	if t.spec != nil {
		for i := 0; i < len(t.assignedNodes); i++ {
			for j := i + 1; j < len(t.assignedNodes); j++ {
				var dist int
				if t.spec.DistanceMatrix != nil {
					dist = t.spec.DistanceMatrix.Distance(t.assignedNodes[i], t.assignedNodes[j])
				} else if t.spec.NodeCoordinates != nil {
					dist = FabricDistance(t.spec.NodeCoordinates[t.assignedNodes[i]], t.spec.NodeCoordinates[t.assignedNodes[j]])
				}
				if dist > t.currentDiameter {
					t.currentDiameter = dist
				}
			}
		}
	}
}

// Diameter returns the current placement diameter.
func (t *NetworkDiameterBoundingTracker) Diameter() int {
	return t.currentDiameter
}

// BuildFatTreeTopologyLabels generates synthetic fat-tree topology labels for benchmark testing.
// k: number of pods in fat-tree (k-ary fat tree).
// Total nodes = (k^3) / 4. For k=28, nodes = 5488 (> 5,000 nodes).
func BuildFatTreeTopologyLabels(k int) map[string]map[string]string {
	numPods := k
	leafsPerPod := k / 2
	hostsPerLeaf := k / 2
	totalNodes := numPods * leafsPerPod * hostsPerLeaf

	labels := make(map[string]map[string]string, totalNodes)
	for p := 0; p < numPods; p++ {
		podName := fmt.Sprintf("spine-pod-%d", p)
		for l := 0; l < leafsPerPod; l++ {
			leafName := fmt.Sprintf("leaf-%d-%d", p, l)
			for h := 0; h < hostsPerLeaf; h++ {
				nodeName := fmt.Sprintf("node-%d-%d-%d", p, l, h)
				labels[nodeName] = map[string]string{
					LabelTopologyDataCenter: "dc-1",
					LabelTopologySpinePod:   podName,
					LabelTopologyLeafRack:   leafName,
				}
			}
		}
	}
	return labels
}
