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

package dynamicresources

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	fwk "k8s.io/kube-scheduler/framework"
)

const (
	// ClaimTemplateDriverCacheKey is the CycleState key for ClaimTemplateDriverCache.
	ClaimTemplateDriverCacheKey fwk.StateKey = "k8s.io/scheduler/ClaimTemplateDriverCache"
)

// ClaimDriverFeasibility stores the driver and device pool feasibility result for a claim template on a node.
type ClaimDriverFeasibility struct {
	Feasible      bool
	Reason        string
	Driver        string
	RequiredCount int
}

// ClaimTemplateDriverCache provides cycle-scoped memoization for DRA claim-to-driver compatibility
// across homogeneous gang pods within a scheduling cycle (Idea 11 Layer 2A).
type ClaimTemplateDriverCache struct {
	mu sync.RWMutex

	// feasibility maps claimTemplateSignature -> nodeName -> ClaimDriverFeasibility
	feasibility map[string]map[string]*ClaimDriverFeasibility

	// Telemetry and profiling counters
	hits   int64
	misses int64
	pruned int64
}

var _ fwk.StateData = &ClaimTemplateDriverCache{}

// NewClaimTemplateDriverCache constructs a new ClaimTemplateDriverCache.
func NewClaimTemplateDriverCache() *ClaimTemplateDriverCache {
	return &ClaimTemplateDriverCache{
		feasibility: make(map[string]map[string]*ClaimDriverFeasibility),
	}
}

// Clone implements fwk.StateData.
func (c *ClaimTemplateDriverCache) Clone() fwk.StateData {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	clone := &ClaimTemplateDriverCache{
		feasibility: make(map[string]map[string]*ClaimDriverFeasibility, len(c.feasibility)),
		hits:        atomic.LoadInt64(&c.hits),
		misses:      atomic.LoadInt64(&c.misses),
		pruned:      atomic.LoadInt64(&c.pruned),
	}
	for sig, nodeMap := range c.feasibility {
		newNodeMap := make(map[string]*ClaimDriverFeasibility, len(nodeMap))
		for node, feas := range nodeMap {
			if feas != nil {
				feasCopy := *feas
				newNodeMap[node] = &feasCopy
			}
		}
		clone.feasibility[sig] = newNodeMap
	}
	return clone
}

// GetFeasibility retrieves the cached driver feasibility outcome for a claim template on a node.
func (c *ClaimTemplateDriverCache) GetFeasibility(sig string, nodeName string) (*ClaimDriverFeasibility, bool) {
	if c == nil || sig == "" || nodeName == "" {
		return nil, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodeMap, exists := c.feasibility[sig]
	if !exists {
		atomic.AddInt64(&c.misses, 1)
		return nil, false
	}
	feas, exists := nodeMap[nodeName]
	if !exists || feas == nil {
		atomic.AddInt64(&c.misses, 1)
		return nil, false
	}
	atomic.AddInt64(&c.hits, 1)
	return feas, true
}

// SetFeasibility stores the driver feasibility outcome for a claim template on a node.
func (c *ClaimTemplateDriverCache) SetFeasibility(sig string, nodeName string, feasibility *ClaimDriverFeasibility) {
	if c == nil || sig == "" || nodeName == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	nodeMap, exists := c.feasibility[sig]
	if !exists {
		nodeMap = make(map[string]*ClaimDriverFeasibility)
		c.feasibility[sig] = nodeMap
	}
	nodeMap[nodeName] = feasibility
	if feasibility != nil && !feasibility.Feasible {
		atomic.AddInt64(&c.pruned, 1)
	}
}

// Stats returns the performance telemetry counters.
func (c *ClaimTemplateDriverCache) Stats() (hits, misses, pruned int64) {
	if c == nil {
		return 0, 0, 0
	}
	return atomic.LoadInt64(&c.hits), atomic.LoadInt64(&c.misses), atomic.LoadInt64(&c.pruned)
}

// StateReaderWriter defines an interface for reading and writing cycle state data.
type StateReaderWriter interface {
	Read(key fwk.StateKey) (fwk.StateData, error)
	Write(key fwk.StateKey, val fwk.StateData)
}

// GetOrCreateClaimTemplateDriverCache retrieves or initializes the cache on a CycleState or PodGroupCycleState.
func GetOrCreateClaimTemplateDriverCache(state fwk.CycleState) *ClaimTemplateDriverCache {
	if state == nil {
		return nil
	}
	var targetState StateReaderWriter = state
	if pgState := state.GetPodGroupCycleState(); pgState != nil {
		targetState = pgState
	}

	data, err := targetState.Read(ClaimTemplateDriverCacheKey)
	if err == nil && data != nil {
		if cache, ok := data.(*ClaimTemplateDriverCache); ok {
			return cache
		}
	}

	cache := NewClaimTemplateDriverCache()
	targetState.Write(ClaimTemplateDriverCacheKey, cache)
	return cache
}

// PodDRARequirement describes the required DRA driver, pool, count, and device class for a claim.
type PodDRARequirement struct {
	ClaimName       string
	DeviceClassName string
	Driver          string
	Pool            string
	Count           int
}

var driverCelRegex = regexp.MustCompile(`(?:device\.driver\s*==\s*["']([^"']+)["'])|(?:["']([^"']+)["']\s*==\s*device\.driver)`)

// ExtractDriverFromDeviceClass attempts to resolve the DRA driver name associated with a DeviceClass.
func ExtractDriverFromDeviceClass(dc *resourceapi.DeviceClass) string {
	if dc == nil {
		return ""
	}
	// 1. Check opaque driver configuration
	for _, cfg := range dc.Spec.Config {
		if cfg.Opaque != nil && len(cfg.Opaque.Driver) > 0 {
			return cfg.Opaque.Driver
		}
	}
	// 2. Check CEL selectors
	for _, sel := range dc.Spec.Selectors {
		matches := driverCelRegex.FindStringSubmatch(sel.CEL.Expression)
		if len(matches) > 1 {
			if matches[1] != "" {
				return matches[1]
			}
			if len(matches) > 2 && matches[2] != "" {
				return matches[2]
			}
		}
	}
	// 3. Fallback: If DeviceClass name contains a known driver domain prefix or suffix
	if strings.Contains(dc.Name, ".") || strings.Contains(dc.Name, "/") {
		return dc.Name
	}
	return ""
}

// ExtractPodDRARequirements parses a pod's ResourceClaims and resolves required drivers,
// device classes, counts, and computes a canonical signature for template caching.
func ExtractPodDRARequirements(pod *v1.Pod, draManager fwk.SharedDRAManager) ([]PodDRARequirement, string, error) {
	if pod == nil || len(pod.Spec.ResourceClaims) == 0 {
		return nil, "", nil
	}
	if draManager == nil {
		return nil, "", nil
	}

	var reqs []PodDRARequirement
	var sigParts []string

	for _, podClaim := range pod.Spec.ResourceClaims {
		claimName := podClaim.Name
		claim, err := draManager.ResourceClaims().Get(pod.Namespace, claimName)
		if err != nil {
			// If not found by podClaim.Name, try podClaim.ResourceClaimName
			if podClaim.ResourceClaimName != nil {
				claim, err = draManager.ResourceClaims().Get(pod.Namespace, *podClaim.ResourceClaimName)
			}
		}
		if err != nil || claim == nil {
			continue
		}

		// Iterate through device requests
		for _, req := range claim.Spec.Devices.Requests {
			if req.Exactly != nil {
				className := req.Exactly.DeviceClassName
				driver := ""
				if draManager.DeviceClasses() != nil {
					dc, _ := draManager.DeviceClasses().Get(className)
					if dc != nil {
						driver = ExtractDriverFromDeviceClass(dc)
					}
				}
				// If driver not found in DeviceClass, check claim request selectors
				if driver == "" {
					for _, sel := range req.Exactly.Selectors {
						matches := driverCelRegex.FindStringSubmatch(sel.CEL.Expression)
						if len(matches) > 1 {
							if matches[1] != "" {
								driver = matches[1]
								break
							}
							if len(matches) > 2 && matches[2] != "" {
								driver = matches[2]
								break
							}
						}
					}
				}
				// Fallback driver to class name if still empty
				if driver == "" {
					driver = className
				}

				count := int(req.Exactly.Count)
				if count <= 0 {
					count = 1
				}

				reqs = append(reqs, PodDRARequirement{
					ClaimName:       claim.Name,
					DeviceClassName: className,
					Driver:          driver,
					Count:           count,
				})
				sigParts = append(sigParts, fmt.Sprintf("req:%s:class:%s:drv:%s:cnt:%d", req.Name, className, driver, count))
			} else if len(req.FirstAvailable) > 0 {
				// For firstAvailable, record the subrequests
				for _, sub := range req.FirstAvailable {
					className := sub.DeviceClassName
					driver := ""
					if draManager.DeviceClasses() != nil {
						dc, _ := draManager.DeviceClasses().Get(className)
						if dc != nil {
							driver = ExtractDriverFromDeviceClass(dc)
						}
					}
					if driver == "" {
						driver = className
					}
					count := int(sub.Count)
					if count <= 0 {
						count = 1
					}
					reqs = append(reqs, PodDRARequirement{
						ClaimName:       claim.Name,
						DeviceClassName: className,
						Driver:          driver,
						Count:           count,
					})
					sigParts = append(sigParts, fmt.Sprintf("req:%s/%s:class:%s:drv:%s:cnt:%d", req.Name, sub.Name, className, driver, count))
				}
			}
		}
	}

	sort.Strings(sigParts)
	hasher := sha256.New()
	for _, part := range sigParts {
		hasher.Write([]byte(part))
		hasher.Write([]byte(";"))
	}
	signature := hex.EncodeToString(hasher.Sum(nil))

	return reqs, signature, nil
}
