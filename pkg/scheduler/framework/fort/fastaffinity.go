package fort

import (
	"encoding/json"

	v1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"
)

type aff struct {
	t []fwk.AffinityTerm
}

func FastPodAffinity(spec StateSpec) {
	MapReduce(
		spec,
		"affinityTerms",
		func(kv *KeyValue[string]) KeyValueSet[string] {
			podInfo := kv.Value.(fwk.PodInfo)
			affinityTerms := podInfo.GetRequiredAffinityTerms()
			antiAffinityTerms := podInfo.GetRequiredAntiAffinityTerms()
			termsId, _ := json.Marshal(affinityTerms)
			antiTermsId, _ := json.Marshal(antiAffinityTerms)
			return KeyValueSet[string]{
				{Key: string(termsId), Value: &aff{t: affinityTerms}},
				{Key: string(antiTermsId), Value: &aff{t: antiAffinityTerms}},
			}
		},
		Identical,
		"podInfos",
	)

	MapReduce(
		spec,
		"outgoingNodeAntiAffinityTerms",
		func(kv *KeyValue[string]) KeyValueSet[StrTuple] {
			podInfo := kv.Value.(fwk.PodInfo)
			antiAffinityTerms := podInfo.GetRequiredAntiAffinityTerms()
			antiTermsId, _ := json.Marshal(antiAffinityTerms)
			nodeName := podInfo.GetPod().Spec.NodeName
			return KeyValueSet[StrTuple]{
				{
					Key:   StrTuple{string(antiTermsId), nodeName},
					Value: &aff{t: antiAffinityTerms},
				},
			}
		},
		Identical,
		"podInfos",
	)

	Join[string, string](spec, "podTerms", "affinityTerms", "podInfos")

	MapReduce(
		spec,
		"podsMatchingTermsOnNode",
		func(kv *KeyValue[JoinKey]) KeyValueSet[StrTuple] {
			val := kv.Value.(JoinValue[string, string])

			affinityKey := val.Left.Key
			affinityTerms := val.Left.Value.(*aff)
			pod := val.Right.Value.(fwk.PodInfo).GetPod()

			if podMatchesAllAffinityTerms(affinityTerms.t, pod) {
				return KeyValueSet[StrTuple]{
					{
						Key:   StrTuple{affinityKey, pod.Spec.NodeName},
						Value: 1,
					},
				}
			}

			return KeyValueSet[StrTuple]{}
		},
		Identical,
		"podTerms",
	)
}

// returns true IFF the given pod matches all the given terms.
func podMatchesAllAffinityTerms(terms []fwk.AffinityTerm, pod *v1.Pod) bool {
	if len(terms) == 0 {
		return false
	}
	for _, t := range terms {
		// The incoming pod NamespaceSelector was merged into the Namespaces set, and so
		// we are not explicitly passing in namespace labels.
		if !t.Matches(pod, nil) {
			return false
		}
	}
	return true
}

func Filter(nodeInfo fwk.NodeInfo, podInfo fwk.PodInfo, state State, matchingAffinityTerms []string, affinityTerms string, antiAffinityTerms string, matchingPods KeyValueMap[StrTuple], outgoing KeyValueMap[StrTuple]) bool {
	nodeName := nodeInfo.Node().Name

	// If our pod has affinity terms and the node does not have
	// a pod that matches, then we cannot use the node.
	if len(affinityTerms) > 0 {
		if !matchingPods.Has(StrTuple{affinityTerms, nodeName}) {
			return false
		}
	}

	// If our pod has anti-affinity terms and the node has some
	// pod that matches, then we cannot use the node.
	if len(antiAffinityTerms) > 0 {
		if matchingPods.Has(StrTuple{antiAffinityTerms, nodeName}) {
			return false
		}
	}

	// If the node has pods with outgoing anti-affinity terms that
	// match us, then we can't use the node.
	for _, termId := range matchingAffinityTerms {
		if outgoing.Has(StrTuple{termId, nodeName}) {
			return false
		}
	}

	return true
}

func getPodMatchingAffinityTerms(pod fwk.PodInfo, state State) []string {
	matching := []string{}
	terms := GetMap[string](state, "affinityTerms")
	for termId, terms := range terms.All() {
		if podMatchesAllAffinityTerms(terms.(*aff).t, pod.GetPod()) {
			matching = append(matching, termId)
		}
	}
	return matching
}

func filterWithFastPodAffinity(pods []fwk.PodInfo, nodes []fwk.NodeInfo) {
	spec := NewSpec()
	NewSource[string](spec, "podInfos")
	FastPodAffinity(spec)

	state := New(spec)

	podMap := Source[string](state, "podInfos")
	matchingPods := GetMap[StrTuple](state, "podsMatchingTermsOnNode")
	outgoing := GetMap[StrTuple](state, "outgoingNodeAntiAffinityTerms")

	for _, podInfo := range pods {
		matching := getPodMatchingAffinityTerms(podInfo, state)

		// If our pod has affinity terms and the node does not have
		// a pod that matches, then we cannot use the node.
		affinityTerms := podInfo.GetRequiredAffinityTerms()
		aff := ""
		if len(affinityTerms) > 0 {
			termsId, _ := json.Marshal(affinityTerms)
			aff = string(termsId)
		}

		// If our pod has anti-affinity terms and the node has some
		// pod that matches, then we cannot use the node.
		antiAffinityTerms := podInfo.GetRequiredAntiAffinityTerms()
		anti := ""
		if len(affinityTerms) > 0 {
			termsId, _ := json.Marshal(antiAffinityTerms)
			anti = string(termsId)
		}

		currNode := nodes[0]
		for _, node := range nodes {
			if Filter(node, podInfo, state, matching, aff, anti, matchingPods, outgoing) &&
				len(node.GetPods()) < 5 {
				currNode = node
			}
		}

		podInfo.GetPod().Spec.NodeName = currNode.Node().Name
		podMap.Update(string(podInfo.GetPod().GetUID()), podInfo)
	}
}
