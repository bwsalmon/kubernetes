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

package framework

import (
	"sync"
	"sync/atomic"

	v1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"
)

const (
	// TemplateFeasibilityStateKey is the CycleState key for TemplateFeasibilityCache.
	TemplateFeasibilityStateKey fwk.StateKey = "k8s.io/scheduler/TemplateFeasibilityCache"
	// PodSignatureStateKey is the CycleState key for caching a pod's signature within its cycle.
	PodSignatureStateKey fwk.StateKey = "k8s.io/scheduler/PodSignatureWrapper"
)

// PodSignatureWrapper caches the pod signature on the pod's CycleState.
type PodSignatureWrapper struct {
	Signature string
}

func (w *PodSignatureWrapper) Clone() fwk.StateData {
	if w == nil {
		return nil
	}
	return &PodSignatureWrapper{Signature: w.Signature}
}

// StaticFilterPluginNames is the set of plugin names whose filtering logic depends
// purely on the pod's static template and the node's static definition (labels, taints,
// cordoned state, node name). These do not change across pods within a scheduling cycle.
var StaticFilterPluginNames = map[string]bool{
	"NodeAffinity":       true,
	"TaintToleration":    true,
	"NodeUnschedulable":  true,
	"NodeName":           true,
}

// IsStaticFilterPlugin returns true if the plugin is known to be static.
func IsStaticFilterPlugin(name string) bool {
	return StaticFilterPluginNames[name]
}

// NodeStaticFeasibility represents the static feasibility outcome of a pod template on a node.
type NodeStaticFeasibility struct {
	// Status contains the result of running static filter plugins.
	// If IsSuccess() is true, static filters passed.
	// If not, Status explains why the node is statically infeasible for this template.
	Status *fwk.Status
}

// TemplateFeasibilityCache caches static filter evaluations and node compatibility profiles
// for homogeneous pod templates within a gang scheduling / preemption cycle.
type TemplateFeasibilityCache struct {
	mu sync.RWMutex

	// staticNodeStatus maps podSignature -> nodeName -> NodeStaticFeasibility
	staticNodeStatus map[string]map[string]*NodeStaticFeasibility

	// Telemetry and profiling counters
	staticHits   int64
	staticMisses int64
	pluginsSaved int64
}

var _ fwk.StateData = &TemplateFeasibilityCache{}

// NewTemplateFeasibilityCache creates a new TemplateFeasibilityCache.
func NewTemplateFeasibilityCache() *TemplateFeasibilityCache {
	return &TemplateFeasibilityCache{
		staticNodeStatus: make(map[string]map[string]*NodeStaticFeasibility),
	}
}

// Clone implements fwk.StateData.
func (c *TemplateFeasibilityCache) Clone() fwk.StateData {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	clone := &TemplateFeasibilityCache{
		staticNodeStatus: make(map[string]map[string]*NodeStaticFeasibility, len(c.staticNodeStatus)),
		staticHits:       atomic.LoadInt64(&c.staticHits),
		staticMisses:     atomic.LoadInt64(&c.staticMisses),
		pluginsSaved:     atomic.LoadInt64(&c.pluginsSaved),
	}
	for sig, nodeMap := range c.staticNodeStatus {
		newNodeMap := make(map[string]*NodeStaticFeasibility, len(nodeMap))
		for node, feas := range nodeMap {
			newNodeMap[node] = feas
		}
		clone.staticNodeStatus[sig] = newNodeMap
	}
	return clone
}

// GetStaticFeasibility retrieves the cached static feasibility outcome for a template on a node.
func (c *TemplateFeasibilityCache) GetStaticFeasibility(sig string, nodeName string) (*fwk.Status, bool) {
	if c == nil || sig == "" || nodeName == "" {
		return nil, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodeMap, exists := c.staticNodeStatus[sig]
	if !exists {
		atomic.AddInt64(&c.staticMisses, 1)
		return nil, false
	}
	feas, exists := nodeMap[nodeName]
	if !exists || feas == nil {
		atomic.AddInt64(&c.staticMisses, 1)
		return nil, false
	}
	atomic.AddInt64(&c.staticHits, 1)
	return feas.Status, true
}

// SetStaticFeasibility stores the static feasibility outcome for a template on a node.
func (c *TemplateFeasibilityCache) SetStaticFeasibility(sig string, nodeName string, status *fwk.Status) {
	if c == nil || sig == "" || nodeName == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	nodeMap, exists := c.staticNodeStatus[sig]
	if !exists {
		nodeMap = make(map[string]*NodeStaticFeasibility)
		c.staticNodeStatus[sig] = nodeMap
	}
	nodeMap[nodeName] = &NodeStaticFeasibility{
		Status: status,
	}
}

// RecordPluginsSaved increments the counter of redundant plugin executions avoided.
func (c *TemplateFeasibilityCache) RecordPluginsSaved(count int64) {
	if c != nil {
		atomic.AddInt64(&c.pluginsSaved, count)
	}
}

// Stats returns the performance telemetry for this cache.
func (c *TemplateFeasibilityCache) Stats() (hits, misses, saved int64) {
	if c == nil {
		return 0, 0, 0
	}
	return atomic.LoadInt64(&c.staticHits), atomic.LoadInt64(&c.staticMisses), atomic.LoadInt64(&c.pluginsSaved)
}

// StateReaderWriter defines an interface for reading and writing cycle state data.
type StateReaderWriter interface {
	Read(key fwk.StateKey) (fwk.StateData, error)
	Write(key fwk.StateKey, val fwk.StateData)
}

// GetOrCreateTemplateFeasibilityCache retrieves or initializes the cache on a CycleState / PodGroupCycleState.
func GetOrCreateTemplateFeasibilityCache(state fwk.CycleState) *TemplateFeasibilityCache {
	if state == nil {
		return nil
	}
	var targetState StateReaderWriter = state
	if pgState := state.GetPodGroupCycleState(); pgState != nil {
		targetState = pgState
	}

	data, err := targetState.Read(TemplateFeasibilityStateKey)
	if err == nil && data != nil {
		if cache, ok := data.(*TemplateFeasibilityCache); ok {
			return cache
		}
	}

	cache := NewTemplateFeasibilityCache()
	targetState.Write(TemplateFeasibilityStateKey, cache)
	return cache
}

// ExtractPodSignature returns the string representation of a pod's signature if available.
func ExtractPodSignature(pod *v1.Pod, sig fwk.PodSignature) string {
	if len(sig) > 0 {
		return string(sig)
	}
	return ""
}
