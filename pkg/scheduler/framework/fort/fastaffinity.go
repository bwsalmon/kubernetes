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
	spec.MapReduce(
		"affinityTerms",
		func(kv *KeyValue) KeyValueSet {
			podInfo := kv.Value.(fwk.PodInfo)
			affinityTerms := podInfo.GetRequiredAffinityTerms()
			antiAffinityTerms := podInfo.GetRequiredAntiAffinityTerms()
			termsId, _ := json.Marshal(affinityTerms)
			antiTermsId, _ := json.Marshal(antiAffinityTerms)
			return KeyValueSet{
				string(termsId):     &aff{t: affinityTerms},
				string(antiTermsId): &aff{t: antiAffinityTerms},
			}
		},
		Identical,
		"podInfos",
	)

	spec.MapReduce(
		"outgoingNodeAntiAffinityTerms",
		func(kv *KeyValue) KeyValueSet {
			podInfo := kv.Value.(fwk.PodInfo)
			antiAffinityTerms := podInfo.GetRequiredAntiAffinityTerms()
			antiTermsId, _ := json.Marshal(antiAffinityTerms)
			nodeName := podInfo.GetPod().Spec.NodeName
			return KeyValueSet{
				string(antiTermsId) + "/" + nodeName: &aff{t: antiAffinityTerms},
			}
		},
		Identical,
		"podInfos",
	)

	spec.Join("podTerms", "affinityTerms", "podInfos")

	spec.MapReduce(
		"podsMatchingTermsOnNode",
		func(kv *KeyValue) KeyValueSet {
			val := kv.Value.(JoinValue)

			affinityKey := val.Left.Key
			affinityTerms := val.Left.Value.(*aff)
			pod := val.Right.Value.(fwk.PodInfo).GetPod()

			if podMatchesAllAffinityTerms(affinityTerms.t, pod) {
				return KeyValueSet{
					affinityKey + "/" + pod.Spec.NodeName: 1,
				}
			}

			return KeyValueSet{}
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

func Filter(nodeInfo fwk.NodeInfo, podInfo fwk.PodInfo, state State, matchingAffinityTerms []string) bool {
	nodeName := nodeInfo.Node().Name

	matchingPods := state.Get("podsMatchingTermsOnNode")

	// If our pod has affinity terms and the node does not have
	// a pod that matches, then we cannot use the node.
	affinityTerms := podInfo.GetRequiredAffinityTerms()
	if len(affinityTerms) > 0 {
		termsId, _ := json.Marshal(affinityTerms)
		if !matchingPods.Has(string(termsId) + "/" + nodeName) {
			return false
		}
	}

	// If our pod has anti-affinity terms and the node has some
	// pod that matches, then we cannot use the node.
	antiAffinityTerms := podInfo.GetRequiredAntiAffinityTerms()
	if len(affinityTerms) > 0 {
		termsId, _ := json.Marshal(antiAffinityTerms)
		if matchingPods.Has(string(termsId) + "/" + nodeName) {
			return false
		}
	}

	// If the node has pods with outgoing anti-affinity terms that
	// match us, then we can't use the node.
	outgoing := state.Get("outgoingNodeAntiAffinityTerms")
	for _, termId := range matchingAffinityTerms {
		if outgoing.Has(termId + "/" + nodeName) {
			return false
		}
	}

	return true
}

func getPodMatchingAffinityTerms(pod fwk.PodInfo, state State) []string {
	matching := []string{}
	terms := state.Get("affinityTerms")
	for termId, terms := range terms.All() {
		if podMatchesAllAffinityTerms(terms.(*aff).t, pod.GetPod()) {
			matching = append(matching, termId)
		}
	}
	return matching
}

func filterWithFastPodAffinity(pods []fwk.PodInfo, nodes []fwk.NodeInfo) {
	spec := NewSpec()
	spec.Source("podInfos")
	FastPodAffinity(spec)

	state := New(spec)

	podMap := state.Source("podInfos")

	for _, pod := range pods {
		matching := getPodMatchingAffinityTerms(pod, state)
		for _, node := range nodes {
			Filter(node, pod, state, matching)
		}
		podMap.Update(string(pod.GetPod().GetUID()), pod)
	}
}
