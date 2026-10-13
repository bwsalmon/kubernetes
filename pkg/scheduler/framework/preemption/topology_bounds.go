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
	"math"
	"sort"
)

// GetDomains returns the list of all domains relevant to this constraint.
// If Domains is already populated, it is returned directly. Otherwise, it extracts
// all unique domain identifiers from NodeDomains and InitialDomainCounts, caches them,
// and returns the sorted slice for determinism.
func (t *TopologySpreadConstraint) GetDomains() []string {
	if len(t.Domains) > 0 {
		return t.Domains
	}

	domainSet := make(map[string]struct{})
	for _, d := range t.NodeDomains {
		if d != "" {
			domainSet[d] = struct{}{}
		}
	}
	for d := range t.InitialDomainCounts {
		if d != "" {
			domainSet[d] = struct{}{}
		}
	}

	domains := make([]string, 0, len(domainSet))
	for d := range domainSet {
		domains = append(domains, d)
	}
	sort.Strings(domains)
	t.Domains = domains
	return domains
}

// CheckFeasibility analytically verifies whether it is mathematically possible to distribute
// remainingPods across all topology domains such that the final maximum skew does not exceed MaxSkew.
//
// Mathematical derivation:
// Let D be the number of domains, S = MaxSkew, remainingPods = M.
// Let A_d be the accumulated pods in domain d (InitialDomainCounts[d] + currentCounts[d]).
// Final count C_d = A_d + x_d, where x_d >= 0 and sum(x_d) = M.
// Target total C_total = sum(A_d) + M.
// Feasibility requires: max_d C_d - min_d C_d <= S.
// Let L = min_d C_d. Then for all d: L <= C_d <= L + S.
// Since x_d >= 0, C_d >= max(A_d, L).
// Thus, sum(max(A_d, L)) <= C_total <= D * (L + S).
// From C_total <= D * (L + S) => L >= ceil((C_total - D * S) / D).
// From C_d <= L + S => L >= max_d A_d - S.
// From sum(max(A_d, L)) >= D * L => L <= floor(C_total / D).
//
// A valid integer allocation exists if and only if:
//
//	L_low <= L_high AND sum(max(A_d, L_low)) <= C_total.
func (t *TopologySpreadConstraint) CheckFeasibility(currentCounts map[string]int, remainingPods int) bool {
	if t.MaxSkew <= 0 {
		return true
	}
	domains := t.GetDomains()
	numDomains := len(domains)
	if numDomains <= 1 {
		return true
	}

	aTotal := 0
	maxA := math.MinInt32
	for _, d := range domains {
		count := 0
		if t.InitialDomainCounts != nil {
			count += t.InitialDomainCounts[d]
		}
		if currentCounts != nil {
			count += currentCounts[d]
		}
		aTotal += count
		if count > maxA {
			maxA = count
		}
	}

	cTotal := aTotal + remainingPods
	lHigh := cTotal / numDomains

	minSkewBound := int(math.Ceil(float64(cTotal-numDomains*t.MaxSkew) / float64(numDomains)))
	lLow := 0
	if maxA-t.MaxSkew > lLow {
		lLow = maxA - t.MaxSkew
	}
	if minSkewBound > lLow {
		lLow = minSkewBound
	}

	if t.MinDomains > 0 && numDomains < t.MinDomains {
		if 0 > lLow {
			lLow = 0
		}
	}

	if lLow > lHigh {
		return false
	}

	minTotal := 0
	for _, d := range domains {
		count := 0
		if t.InitialDomainCounts != nil {
			count += t.InitialDomainCounts[d]
		}
		if currentCounts != nil {
			count += currentCounts[d]
		}
		if count > lLow {
			minTotal += count
		} else {
			minTotal += lLow
		}
	}

	return minTotal <= cTotal
}

// CanDomainAcceptPod determines whether placing an incoming pod from the gang into targetDomain
// is analytically admissible. It guarantees strict mathematical admissibility: it returns false
// if and only if NO legal placement of the remaining (remainingPods - 1) pods can satisfy MaxSkew.
//
// By pruning branches and candidate nodes where CanDomainAcceptPod returns false, the search space
// eliminates futile preemption evaluations in topologically saturated domains in O(D) time.
func (t *TopologySpreadConstraint) CanDomainAcceptPod(targetDomain string, currentCounts map[string]int, remainingPods int) bool {
	if remainingPods <= 0 {
		return false
	}
	if t.MaxSkew <= 0 {
		return true
	}
	domains := t.GetDomains()
	numDomains := len(domains)
	if numDomains <= 1 {
		return true
	}

	// Tentatively add 1 pod to targetDomain
	aTotal := 0
	maxA := math.MinInt32
	for _, d := range domains {
		count := 0
		if t.InitialDomainCounts != nil {
			count += t.InitialDomainCounts[d]
		}
		if currentCounts != nil {
			count += currentCounts[d]
		}
		if d == targetDomain {
			count++
		}
		aTotal += count
		if count > maxA {
			maxA = count
		}
	}

	// Remaining pods after this placement is (remainingPods - 1)
	cTotal := aTotal + (remainingPods - 1)
	lHigh := cTotal / numDomains

	minSkewBound := int(math.Ceil(float64(cTotal-numDomains*t.MaxSkew) / float64(numDomains)))
	lLow := 0
	if maxA-t.MaxSkew > lLow {
		lLow = maxA - t.MaxSkew
	}
	if minSkewBound > lLow {
		lLow = minSkewBound
	}

	if t.MinDomains > 0 && numDomains < t.MinDomains {
		if 0 > lLow {
			lLow = 0
		}
	}

	if lLow > lHigh {
		return false
	}

	minTotal := 0
	for _, d := range domains {
		count := 0
		if t.InitialDomainCounts != nil {
			count += t.InitialDomainCounts[d]
		}
		if currentCounts != nil {
			count += currentCounts[d]
		}
		if d == targetDomain {
			count++
		}
		if count > lLow {
			minTotal += count
		} else {
			minTotal += lLow
		}
	}

	return minTotal <= cTotal
}

// isDomainCountFeasible checks if assigning count cd to domain targetDomain can be extended
// to a valid configuration for all other domains within MaxSkew.
func (t *TopologySpreadConstraint) isDomainCountFeasible(targetDomain string, cd int, currentCounts map[string]int, remainingPods int) bool {
	domains := t.GetDomains()
	numDomains := len(domains)
	if numDomains <= 1 || t.MaxSkew <= 0 {
		return true
	}

	aTotal := 0
	otherDomains := make([]string, 0, numDomains-1)
	for _, d := range domains {
		count := 0
		if t.InitialDomainCounts != nil {
			count += t.InitialDomainCounts[d]
		}
		if currentCounts != nil {
			count += currentCounts[d]
		}
		aTotal += count
		if d != targetDomain {
			otherDomains = append(otherDomains, d)
		}
	}

	cTotal := aTotal + remainingPods
	targetOther := cTotal - cd
	if targetOther < 0 {
		return false
	}

	minL := 0
	if cd-t.MaxSkew > minL {
		minL = cd - t.MaxSkew
	}
	maxL := cd

	for l := minL; l <= maxL; l++ {
		validL := true
		minSum := 0
		maxSum := 0
		for _, od := range otherDomains {
			odCount := 0
			if t.InitialDomainCounts != nil {
				odCount += t.InitialDomainCounts[od]
			}
			if currentCounts != nil {
				odCount += currentCounts[od]
			}
			if odCount > l+t.MaxSkew {
				validL = false
				break
			}
			if odCount > l {
				minSum += odCount
			} else {
				minSum += l
			}
			maxSum += l + t.MaxSkew
		}
		if !validL {
			continue
		}

		if minSum <= targetOther && targetOther <= maxSum {
			return true
		}
	}

	return false
}

// ComputeDomainBounds computes closed-form analytical lower and upper bounds on the number
// of gang pods that can be assigned to each domain from remainingPods.
//
// For domain d:
// Upper bound is the maximum pods domain d can receive without violating MaxSkew.
// Lower bound is the minimum pods domain d must receive so that other domains do not exceed MaxSkew.
func (t *TopologySpreadConstraint) ComputeDomainBounds(currentCounts map[string]int, remainingPods int) (map[string]int, map[string]int) {
	domains := t.GetDomains()
	lowerBounds := make(map[string]int, len(domains))
	upperBounds := make(map[string]int, len(domains))

	if t.MaxSkew <= 0 || len(domains) <= 1 {
		for _, d := range domains {
			lowerBounds[d] = 0
			upperBounds[d] = remainingPods
		}
		return lowerBounds, upperBounds
	}

	for _, d := range domains {
		currentD := 0
		if t.InitialDomainCounts != nil {
			currentD += t.InitialDomainCounts[d]
		}
		if currentCounts != nil {
			currentD += currentCounts[d]
		}

		minPods := remainingPods
		maxPods := 0
		found := false

		for pods := 0; pods <= remainingPods; pods++ {
			if t.isDomainCountFeasible(d, currentD+pods, currentCounts, remainingPods) {
				if !found {
					minPods = pods
					found = true
				}
				maxPods = pods
			}
		}

		if !found {
			lowerBounds[d] = 0
			upperBounds[d] = 0
		} else {
			lowerBounds[d] = minPods
			upperBounds[d] = maxPods
		}
	}

	return lowerBounds, upperBounds
}
