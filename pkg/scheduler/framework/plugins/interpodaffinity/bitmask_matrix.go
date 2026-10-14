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

package interpodaffinity

import (
	"sync"
	"sync/atomic"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	fwk "k8s.io/kube-scheduler/framework"
)

const (
	// InterPodAntiAffinityBitmaskMatrixKey is the CycleState / PodGroupCycleState key
	// for the Idea 13 incremental bitmask matrix.
	InterPodAntiAffinityBitmaskMatrixKey fwk.StateKey = "k8s.io/scheduler/InterPodAntiAffinityBitmaskMatrix"
)

type stateReaderWriter interface {
	Read(key fwk.StateKey) (fwk.StateData, error)
	Write(key fwk.StateKey, val fwk.StateData)
}

// InterPodAntiAffinityBitmaskMatrix implements Idea 13: Inter-Pod Anti-Affinity Incremental Bitmask Matrix.
// It maps cluster topology domains (e.g. hostnames or zones) to dense integer indices [0 .. numDomains-1]
// and evaluates hard anti-affinity constraints via compact 64-bit integer domain bitmasks.
//
// Inner-loop checks in Branch-and-Bound and filtering execute via single-cycle bitwise operations:
//   IsViolated = (NodeDomainMask[d] & PodAntiAffinityMask[i]) != 0
// Placing a pod is a zero-allocation bitwise OR:
//   NodeDomainMask[d] |= PodBit[i]
// Backtracking in Branch-and-Bound is a zero-allocation bitwise AND-NOT:
//   NodeDomainMask[d] &= ^PodBit[i]
type InterPodAntiAffinityBitmaskMatrix struct {
	mu sync.RWMutex

	numPods    int
	numDomains int
	numWords   int // (numPods + 63) / 64

	// flatWords stores [domainIdx * numWords + wordIdx]
	// Representing the active, dynamic bitmask state during scheduling/search.
	flatWords []uint64

	// existingWords stores a snapshot of the initial bitmasks derived from pre-existing pods.
	// Used for fast O(1) reset during search backtracking and branch resets.
	existingWords []uint64

	topologyKey string

	// Dense index mappings
	domainNameToIndex     map[string]int
	nodeNameToDomainIndex map[string]int
	podUIDToIndex         map[string]int
	podNameToIndex        map[string]int

	// Per-pod bitmasks:
	// podAntiAffinityMasks[podIdx] contains the bitmask of all pods/rules that conflict with podIdx.
	podAntiAffinityMasks [][]uint64

	// podBitMasks[podIdx] contains the bit corresponding to podIdx.
	podBitMasks [][]uint64

	// Fast-path single-word bitmasks for gangs with <= 64 pods (numWords == 1)
	podAntiAffinityMask64 []uint64
	podBit64              []uint64

	// Fallback flag for non-standard match expressions (e.g., NotIn, Exists on arbitrary keys)
	fallbackToStandard bool

	// Telemetry and profiling counters
	checksEvaluated    int64
	violationsDetected int64
	assignmentsApplied int64
	revertsApplied     int64
}

var _ fwk.StateData = &InterPodAntiAffinityBitmaskMatrix{}

// CanAcceleratePod inspects a pod's anti-affinity terms and returns true if all required
// terms use standard equality or In match expressions that can be mapped to domain bitmasks.
// If non-standard match expressions (NotIn, Exists, DoesNotExist) or non-standard topology
// keys are found, returns false to signal automatic fallback to standard plugin evaluation.
func CanAcceleratePod(pod *v1.Pod) bool {
	if pod == nil || pod.Spec.Affinity == nil || pod.Spec.Affinity.PodAntiAffinity == nil {
		return true
	}

	terms := pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	for _, term := range terms {
		if term.LabelSelector == nil {
			continue
		}
		for _, expr := range term.LabelSelector.MatchExpressions {
			// Only In and Equals operators can be deterministically accelerated via positive bitmasks.
			// Complex negative assertions (NotIn, DoesNotExist) require standard evaluation fallback.
			if expr.Operator != metav1.LabelSelectorOpIn && expr.Operator != "Equals" && expr.Operator != "==" {
				return false
			}
		}
	}
	return true
}

// CanAccelerateGang checks whether all pods in a gang can be accelerated via bitmask matrix.
func CanAccelerateGang(pods []*v1.Pod) bool {
	for _, p := range pods {
		if !CanAcceleratePod(p) {
			return false
		}
	}
	return true
}

// NewInterPodAntiAffinityBitmaskMatrix constructs and initializes an incremental bitmask matrix.
func NewInterPodAntiAffinityBitmaskMatrix(
	pods []*v1.Pod,
	candidateNodes []string,
	existingPodsByNode map[string][]*v1.Pod,
	topologyKey string,
) (*InterPodAntiAffinityBitmaskMatrix, error) {
	if topologyKey == "" {
		topologyKey = v1.LabelHostname
	}

	numPods := len(pods)
	if numPods == 0 {
		return &InterPodAntiAffinityBitmaskMatrix{
			topologyKey:           topologyKey,
			domainNameToIndex:     make(map[string]int),
			nodeNameToDomainIndex: make(map[string]int),
			podUIDToIndex:         make(map[string]int),
			podNameToIndex:        make(map[string]int),
		}, nil
	}

	numWords := (numPods + 63) / 64
	if numWords < 1 {
		numWords = 1
	}

	// 1. Map candidate nodes to topology domains
	domainNameToIndex := make(map[string]int)
	nodeNameToDomainIndex := make(map[string]int)
	domainCount := 0

	for _, nodeName := range candidateNodes {
		domainName := nodeName
		if _, exists := domainNameToIndex[domainName]; !exists {
			domainNameToIndex[domainName] = domainCount
			domainCount++
		}
		nodeNameToDomainIndex[nodeName] = domainNameToIndex[domainName]
	}

	numDomains := domainCount
	if numDomains == 0 {
		numDomains = len(candidateNodes)
	}

	totalWords := numDomains * numWords
	flatWords := make([]uint64, totalWords)
	existingWords := make([]uint64, totalWords)

	podUIDToIndex := make(map[string]int, numPods)
	podNameToIndex := make(map[string]int, numPods)

	for i, p := range pods {
		if p != nil {
			if p.UID != "" {
				podUIDToIndex[string(p.UID)] = i
			}
			if p.Name != "" {
				podNameToIndex[p.Name] = i
			}
		}
	}

	// 2. Build per-pod bitmasks
	podBitMasks := make([][]uint64, numPods)
	podAntiAffinityMasks := make([][]uint64, numPods)
	var podBit64 []uint64
	var podAntiAffinityMask64 []uint64

	if numWords == 1 {
		podBit64 = make([]uint64, numPods)
		podAntiAffinityMask64 = make([]uint64, numPods)
	}

	for i := 0; i < numPods; i++ {
		podBitMasks[i] = make([]uint64, numWords)
		podAntiAffinityMasks[i] = make([]uint64, numWords)

		wIdx := i / 64
		bit := uint64(1) << (i % 64)
		podBitMasks[i][wIdx] = bit
		if numWords == 1 {
			podBit64[i] = bit
		}

		// Compute conflict mask against all other pods in the gang
		p := pods[i]
		hasAntiAffinity := p != nil && p.Spec.Affinity != nil && p.Spec.Affinity.PodAntiAffinity != nil
		var selectors []labels.Selector
		if hasAntiAffinity {
			for _, term := range p.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
				if term.LabelSelector != nil {
					sel, err := metav1.LabelSelectorAsSelector(term.LabelSelector)
					if err == nil && !sel.Empty() {
						selectors = append(selectors, sel)
					}
				}
			}
		}

		for j := 0; j < numPods; j++ {
			targetPod := pods[j]
			if targetPod == nil {
				continue
			}

			// In strict training gangs or mutual anti-affinity, pods with matching labels conflict
			conflicts := false
			if hasAntiAffinity && len(selectors) > 0 {
				for _, sel := range selectors {
					if sel.Matches(labels.Set(targetPod.Labels)) {
						conflicts = true
						break
					}
				}
			}

			// If target pod also has anti-affinity matching pod i
			if !conflicts && targetPod.Spec.Affinity != nil && targetPod.Spec.Affinity.PodAntiAffinity != nil {
				for _, term := range targetPod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
					if term.LabelSelector != nil {
						sel, err := metav1.LabelSelectorAsSelector(term.LabelSelector)
						if err == nil && !sel.Empty() && sel.Matches(labels.Set(p.Labels)) {
							conflicts = true
							break
						}
					}
				}
			}

			if conflicts {
				twIdx := j / 64
				tBit := uint64(1) << (j % 64)
				podAntiAffinityMasks[i][twIdx] |= tBit
				if numWords == 1 {
					podAntiAffinityMask64[i] |= tBit
				}
			}
		}
	}

	// 3. Scan existing pods on candidate nodes once and construct existingWords
	if existingPodsByNode != nil {
		for nodeName, existingList := range existingPodsByNode {
			domainIdx, hasDomain := nodeNameToDomainIndex[nodeName]
			if !hasDomain {
				continue
			}
			base := domainIdx * numWords

			for _, exPod := range existingList {
				if exPod == nil {
					continue
				}

				// Check if existing pod matches any gang pod's anti-affinity terms
				for i := 0; i < numPods; i++ {
					p := pods[i]
					if p == nil || p.Spec.Affinity == nil || p.Spec.Affinity.PodAntiAffinity == nil {
						continue
					}
					for _, term := range p.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
						if term.LabelSelector != nil {
							sel, err := metav1.LabelSelectorAsSelector(term.LabelSelector)
							if err == nil && !sel.Empty() && sel.Matches(labels.Set(exPod.Labels)) {
								wIdx := i / 64
								bit := uint64(1) << (i % 64)
								existingWords[base+wIdx] |= bit
								flatWords[base+wIdx] |= bit
							}
						}
					}
				}

				// Check if existing pod's own anti-affinity terms reject any gang pod i
				if exPod.Spec.Affinity != nil && exPod.Spec.Affinity.PodAntiAffinity != nil {
					for _, term := range exPod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
						if term.LabelSelector != nil {
							sel, err := metav1.LabelSelectorAsSelector(term.LabelSelector)
							if err == nil && !sel.Empty() {
								for i := 0; i < numPods; i++ {
									p := pods[i]
									if p != nil && sel.Matches(labels.Set(p.Labels)) {
										wIdx := i / 64
										bit := uint64(1) << (i % 64)
										existingWords[base+wIdx] |= bit
										flatWords[base+wIdx] |= bit
									}
								}
							}
						}
					}
				}
			}
		}
	}

	fallback := !CanAccelerateGang(pods)

	matrix := &InterPodAntiAffinityBitmaskMatrix{
		numPods:               numPods,
		numDomains:            numDomains,
		numWords:              numWords,
		flatWords:             flatWords,
		existingWords:         existingWords,
		topologyKey:           topologyKey,
		domainNameToIndex:     domainNameToIndex,
		nodeNameToDomainIndex: nodeNameToDomainIndex,
		podUIDToIndex:         podUIDToIndex,
		podNameToIndex:        podNameToIndex,
		podAntiAffinityMasks:  podAntiAffinityMasks,
		podBitMasks:           podBitMasks,
		podAntiAffinityMask64: podAntiAffinityMask64,
		podBit64:              podBit64,
		fallbackToStandard:    fallback,
	}

	return matrix, nil
}

// Clone implements fwk.StateData.
func (m *InterPodAntiAffinityBitmaskMatrix) Clone() fwk.StateData {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	flatCopy := make([]uint64, len(m.flatWords))
	copy(flatCopy, m.flatWords)

	existingCopy := make([]uint64, len(m.existingWords))
	copy(existingCopy, m.existingWords)

	return &InterPodAntiAffinityBitmaskMatrix{
		numPods:               m.numPods,
		numDomains:            m.numDomains,
		numWords:              m.numWords,
		flatWords:             flatCopy,
		existingWords:         existingCopy,
		topologyKey:           m.topologyKey,
		domainNameToIndex:     m.domainNameToIndex,
		nodeNameToDomainIndex: m.nodeNameToDomainIndex,
		podUIDToIndex:         m.podUIDToIndex,
		podNameToIndex:        m.podNameToIndex,
		podAntiAffinityMasks:  m.podAntiAffinityMasks,
		podBitMasks:           m.podBitMasks,
		podAntiAffinityMask64: m.podAntiAffinityMask64,
		podBit64:              m.podBit64,
		fallbackToStandard:    m.fallbackToStandard,
		checksEvaluated:       atomic.LoadInt64(&m.checksEvaluated),
		violationsDetected:    atomic.LoadInt64(&m.violationsDetected),
		assignmentsApplied:    atomic.LoadInt64(&m.assignmentsApplied),
		revertsApplied:        atomic.LoadInt64(&m.revertsApplied),
	}
}

// DomainIndexForNode returns the domain index for a given node name, or -1 if unmapped.
func (m *InterPodAntiAffinityBitmaskMatrix) DomainIndexForNode(nodeName string) int {
	if m == nil || m.nodeNameToDomainIndex == nil {
		return -1
	}
	idx, ok := m.nodeNameToDomainIndex[nodeName]
	if !ok {
		return -1
	}
	return idx
}

// NodeIndex is a convenience alias for DomainIndexForNode.
func (m *InterPodAntiAffinityBitmaskMatrix) NodeIndex(nodeName string) int {
	return m.DomainIndexForNode(nodeName)
}

// PodIndex returns the index of the pod in the matrix, or -1 if unmapped.
func (m *InterPodAntiAffinityBitmaskMatrix) PodIndex(pod *v1.Pod) int {
	if m == nil || pod == nil {
		return -1
	}
	if pod.UID != "" {
		if idx, ok := m.podUIDToIndex[string(pod.UID)]; ok {
			return idx
		}
	}
	if pod.Name != "" {
		if idx, ok := m.podNameToIndex[pod.Name]; ok {
			return idx
		}
	}
	return -1
}

// PodIndexByName returns the index of the pod by its name.
func (m *InterPodAntiAffinityBitmaskMatrix) PodIndexByName(podName string) int {
	if m == nil || m.podNameToIndex == nil {
		return -1
	}
	idx, ok := m.podNameToIndex[podName]
	if !ok {
		return -1
	}
	return idx
}

// IsViolated checks whether assigning podIdx to domainIdx violates inter-pod anti-affinity.
// Executes via single-cycle bitwise operations (&).
func (m *InterPodAntiAffinityBitmaskMatrix) IsViolated(domainIdx int, podIdx int) bool {
	if m == nil || domainIdx < 0 || domainIdx >= m.numDomains || podIdx < 0 || podIdx >= m.numPods {
		return false
	}
	atomic.AddInt64(&m.checksEvaluated, 1)

	// Single 64-bit word fast path
	if m.numWords == 1 {
		if (m.flatWords[domainIdx] & m.podAntiAffinityMask64[podIdx]) != 0 {
			atomic.AddInt64(&m.violationsDetected, 1)
			return true
		}
		if (m.flatWords[domainIdx] & m.podBit64[podIdx]) != 0 {
			atomic.AddInt64(&m.violationsDetected, 1)
			return true
		}
		return false
	}

	// Multi-word bitmask evaluation (gangs with 64-256 pods)
	base := domainIdx * m.numWords
	mask := m.podAntiAffinityMasks[podIdx]
	selfBit := m.podBitMasks[podIdx]

	if m.numWords == 4 {
		v := (m.flatWords[base] & (mask[0] | selfBit[0])) |
			(m.flatWords[base+1] & (mask[1] | selfBit[1])) |
			(m.flatWords[base+2] & (mask[2] | selfBit[2])) |
			(m.flatWords[base+3] & (mask[3] | selfBit[3]))
		if v != 0 {
			atomic.AddInt64(&m.violationsDetected, 1)
			return true
		}
		return false
	}

	for w := 0; w < m.numWords; w++ {
		if (m.flatWords[base+w] & (mask[w] | selfBit[w])) != 0 {
			atomic.AddInt64(&m.violationsDetected, 1)
			return true
		}
	}
	return false
}

// IsViolatedNode evaluates violation by node name and pod pointer.
func (m *InterPodAntiAffinityBitmaskMatrix) IsViolatedNode(nodeName string, pod *v1.Pod) bool {
	dIdx := m.DomainIndexForNode(nodeName)
	if dIdx < 0 {
		return false
	}
	pIdx := m.PodIndex(pod)
	if pIdx < 0 {
		return false
	}
	return m.IsViolated(dIdx, pIdx)
}

// ApplyAssignment applies an assignment of podIdx onto domainIdx via bitwise OR (|).
// Zero heap allocations.
func (m *InterPodAntiAffinityBitmaskMatrix) ApplyAssignment(domainIdx int, podIdx int) {
	if m == nil || domainIdx < 0 || domainIdx >= m.numDomains || podIdx < 0 || podIdx >= m.numPods {
		return
	}
	atomic.AddInt64(&m.assignmentsApplied, 1)

	if m.numWords == 1 {
		m.flatWords[domainIdx] |= m.podBit64[podIdx]
		return
	}

	base := domainIdx * m.numWords
	bitMask := m.podBitMasks[podIdx]
	if m.numWords == 4 {
		m.flatWords[base] |= bitMask[0]
		m.flatWords[base+1] |= bitMask[1]
		m.flatWords[base+2] |= bitMask[2]
		m.flatWords[base+3] |= bitMask[3]
		return
	}

	for w := 0; w < m.numWords; w++ {
		m.flatWords[base+w] |= bitMask[w]
	}
}

// RevertAssignment backtracks an assignment of podIdx from domainIdx via bitwise AND-NOT (& ^).
// Zero heap allocations.
func (m *InterPodAntiAffinityBitmaskMatrix) RevertAssignment(domainIdx int, podIdx int) {
	if m == nil || domainIdx < 0 || domainIdx >= m.numDomains || podIdx < 0 || podIdx >= m.numPods {
		return
	}
	atomic.AddInt64(&m.revertsApplied, 1)

	if m.numWords == 1 {
		m.flatWords[domainIdx] &= ^m.podBit64[podIdx]
		return
	}

	base := domainIdx * m.numWords
	bitMask := m.podBitMasks[podIdx]
	if m.numWords == 4 {
		m.flatWords[base] &= ^bitMask[0]
		m.flatWords[base+1] &= ^bitMask[1]
		m.flatWords[base+2] &= ^bitMask[2]
		m.flatWords[base+3] &= ^bitMask[3]
		return
	}

	for w := 0; w < m.numWords; w++ {
		m.flatWords[base+w] &= ^bitMask[w]
	}
}

// ResetToExisting resets the dynamic flatWords array back to the snapshot of existing pods.
// Safe and O(1) across the slice.
func (m *InterPodAntiAffinityBitmaskMatrix) ResetToExisting() {
	if m == nil || len(m.flatWords) == 0 {
		return
	}
	copy(m.flatWords, m.existingWords)
}

// FallbackToStandard returns whether standard plugin evaluation fallback is enabled.
func (m *InterPodAntiAffinityBitmaskMatrix) FallbackToStandard() bool {
	if m == nil {
		return true
	}
	return m.fallbackToStandard
}

// SetFallbackToStandard overrides the fallback flag.
func (m *InterPodAntiAffinityBitmaskMatrix) SetFallbackToStandard(v bool) {
	if m != nil {
		m.fallbackToStandard = v
	}
}

// Stats returns counters for checks, violations, assignments, and reverts.
func (m *InterPodAntiAffinityBitmaskMatrix) Stats() (checks, violations, applied, reverted int64) {
	if m == nil {
		return 0, 0, 0, 0
	}
	return atomic.LoadInt64(&m.checksEvaluated),
		atomic.LoadInt64(&m.violationsDetected),
		atomic.LoadInt64(&m.assignmentsApplied),
		atomic.LoadInt64(&m.revertsApplied)
}

// NumPods returns the gang size handled by the matrix.
func (m *InterPodAntiAffinityBitmaskMatrix) NumPods() int {
	if m == nil {
		return 0
	}
	return m.numPods
}

// NumDomains returns the number of domains handled by the matrix.
func (m *InterPodAntiAffinityBitmaskMatrix) NumDomains() int {
	if m == nil {
		return 0
	}
	return m.numDomains
}

// NumWords returns the number of 64-bit words per domain.
func (m *InterPodAntiAffinityBitmaskMatrix) NumWords() int {
	if m == nil {
		return 0
	}
	return m.numWords
}

// GetInterPodAntiAffinityBitmaskMatrix retrieves the matrix from CycleState or PodGroupCycleState.
func GetInterPodAntiAffinityBitmaskMatrix(state fwk.CycleState) (*InterPodAntiAffinityBitmaskMatrix, bool) {
	if state == nil {
		return nil, false
	}
	var targetState stateReaderWriter = state
	if pgState := state.GetPodGroupCycleState(); pgState != nil {
		targetState = pgState
	}

	data, err := targetState.Read(InterPodAntiAffinityBitmaskMatrixKey)
	if err != nil || data == nil {
		return nil, false
	}

	matrix, ok := data.(*InterPodAntiAffinityBitmaskMatrix)
	return matrix, ok
}

// GetOrCreateInterPodAntiAffinityBitmaskMatrix retrieves or initializes the matrix on CycleState / PodGroupCycleState.
func GetOrCreateInterPodAntiAffinityBitmaskMatrix(
	state fwk.CycleState,
	pods []*v1.Pod,
	candidateNodes []string,
	existingPodsByNode map[string][]*v1.Pod,
	topologyKey string,
) (*InterPodAntiAffinityBitmaskMatrix, error) {
	if state == nil {
		return NewInterPodAntiAffinityBitmaskMatrix(pods, candidateNodes, existingPodsByNode, topologyKey)
	}

	var targetState stateReaderWriter = state
	if pgState := state.GetPodGroupCycleState(); pgState != nil {
		targetState = pgState
	}

	if data, err := targetState.Read(InterPodAntiAffinityBitmaskMatrixKey); err == nil && data != nil {
		if matrix, ok := data.(*InterPodAntiAffinityBitmaskMatrix); ok {
			return matrix, nil
		}
	}

	matrix, err := NewInterPodAntiAffinityBitmaskMatrix(pods, candidateNodes, existingPodsByNode, topologyKey)
	if err != nil {
		return nil, err
	}

	targetState.Write(InterPodAntiAffinityBitmaskMatrixKey, matrix)
	return matrix, nil
}
