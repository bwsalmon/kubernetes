package fort

/*
import (
	"encoding/json"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type aff struct {
	t []fwk.AffinityTerm
}

// Set up data structures for filtering based on pod affinity.
func FastPodAffinity(spec StateSpec) {
	// XXX Note that in a real implementation we need to use topology terms
	// rather than node names, but this is a good first approximation of
	// cost (it assumes only node topology terms).
	// Adding additional topology terms should be nominally more expensive, but probably
	// doesn't add much fidelity to the model.

	// Generate a set of unique affinityTerms across all pods in the system.
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
		AnyValue,
		"podInfos",
	)

	// Generate a set of antiAffinityTerms found on at least one pod
	// on each node in the system.

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
		AnyValue,
		"podInfos",
	)

	// Generate a set of affinity term / node pairs, where the given node
	// has at least one pod matching that term.

	// Start by pairing each affinity term with each pod.
	FullJoin[string, string](spec, "podTerms", "affinityTerms", "podInfos")

	// Then use map reduce to see if each pod matches the corresponding
	// affinity term. If it does, track that the node hosting the pod
	// has a pod matching the given term.
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
		AnyValue,
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
	NewExternalSource[string](spec, "podInfos")
	FastPodAffinity(spec)

	state := New(spec)

	podMap := Source[string](state, "podInfos")
	matchingPods := GetMap[StrTuple](state, "podsMatchingTermsOnNode")
	outgoing := GetMap[StrTuple](state, "outgoingNodeAntiAffinityTerms")

	start := time.Now()

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

		newPodInfo, _ := framework.NewPodInfo(podInfo.GetPod().DeepCopy())
		newPodInfo.GetPod().Spec.NodeName = currNode.Node().Name
		podMap.Update(string(newPodInfo.GetPod().GetUID()), newPodInfo)
	}

	stop := time.Now()
	fmt.Printf("Time %f", float64(stop.Sub(start))/float64(time.Second))
}

*/
