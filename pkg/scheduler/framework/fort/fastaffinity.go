package fort

/*
import (
	"encoding/json"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// Set up data structures for filtering based on pod affinity.
func FastPodAffinity(spec StateSpec) {
	// Generate a set of unique affinityTerms across all pods in the system.
	spec.New(
		"affinityTerms",
		MapReduce(
			func(kv *KeyValue[string]) KeyValueSet[string] {
				ret := KeyValueSet[string]{}
				podInfo := kv.Value.(fwk.PodInfo)
				pod := podInfo.GetPod()
				affinityTerms := podInfo.GetRequiredAffinityTerms()
				for i, t := range affinityTerms {
					// XXX this is an exceptionally horrible hack that must be fixed.
					termId, _ := json.Marshal(pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution[i])
					ret = append(ret, KeyValue[string]{
						Key:   string(termId),
						Value: &t,
					})
				}
				antiAffinityTerms := podInfo.GetRequiredAntiAffinityTerms()
				for i, t := range antiAffinityTerms {
					// XXX this is an exceptionally horrible hack that must be fixed.
					termId, _ := json.Marshal(pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[i])
					ret = append(ret, KeyValue[string]{
						Key:   string(termId),
						Value: &t,
					})
				}
				return ret
			},
			AnyValue,
			"podInfos",
		),
	)

	spec.New(
		"podNodes",
		LookupJoin(
			"podInfos",
			"nodes",
			func(kv *KeyValue[string]) string {
				pod := kv.Value.(fwk.PodInfo).GetPod()
				return pod.Spec.NodeName
			},
		),
	)

	spec.New(
		"podsHavingTermDomain",
		MapReduce(
			func(kv *KeyValue[string]) KeyValueSet[[4]string] {
				ret := KeyValueSet[[4]string]{}
				podInfo := kv.Value.(JoinValue).Left.(fwk.PodInfo)
				node := kv.Value.(JoinValue).Right.(*v1.Node)
				pod := podInfo.GetPod()
				affinityTerms := podInfo.GetRequiredAffinityTerms()
				for i, t := range affinityTerms {
					// XXX this is an exceptionally horrible hack that must be fixed.
					termId, _ := json.Marshal(pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution[i])
					domain := node.Labels[t.TopologyKey]
					ret = append(ret, KeyValue[[4]string]{
						Key:   [4]string{"p", string(termId), t.TopologyKey, domain},
						Value: &t,
					})
				}
				antiAffinityTerms := podInfo.GetRequiredAntiAffinityTerms()
				for i, t := range antiAffinityTerms {
					// XXX this is an exceptionally horrible hack that must be fixed.
					termId, _ := json.Marshal(pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[i])
					domain := node.Labels[t.TopologyKey]
					ret = append(ret, KeyValue[[4]string]{
						Key:   [4]string{"n", string(termId), t.TopologyKey, domain},
						Value: &t,
					})
				}
				return ret
			},
			AnyValue,
			"podNodes",
		),
	)

	// Start by pairing each affinity term with each pod.
	spec.New(
		"termPodNodes",
		FullJoin[string, string](
			"affinityTerms",
			"podNodes",
		),
	)

	// Then use map reduce to see if each pod matches the corresponding
	// affinity term. If it does, track that the node hosting the pod
	// has a pod matching the given term.
	spec.New(
		"podsMatchingTermDomain",
		MapReduce(
			func(kv *KeyValue[JoinKey[string, string]]) KeyValueSet[StrTriple] {
				val := kv.Value.(JoinValue)

				termKey := kv.Key.Left
				term := val.Left.(*fwk.AffinityTerm)
				pod := val.Right.(JoinValue).Left.(fwk.PodInfo).GetPod()
				node := val.Right.(JoinValue).Right.(*v1.Node)

				ret := KeyValueSet[StrTriple]{}
				// XXX need to use the namespace selector as well by using term.Matches
				// rather than term.Selector.Matches.
				if term.Selector.Matches(labels.Set(pod.Labels)) {
					if domain, hasKey := node.Labels[term.TopologyKey]; hasKey {
						ret = append(ret, KeyValue[StrTriple]{
							Key:   StrTriple{termKey, term.TopologyKey, domain},
							Value: true,
						})
					}
				}

				return ret
			},
			Count,
			"termPodNodes",
		),
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

func Filter(nodeInfo fwk.NodeInfo, podInfo fwk.PodInfo, state State, matchingAffinityTerms []string, affinityTerms string, antiAffinityTerms string, matchingPods KeyValueMap[StrTriple]) bool {
	node := nodeInfo.Node()
	nodeName := nodeInfo.Node().Name

	// If our pod has affinity terms and the node does not have
	// a pod that matches, then we cannot use the node.
	if len(affinityTerms) > 0 {
		for _, term := range affinityTerms {
			domain := node.Labels[term.TopologyKey]
			if !matchingPods.Has(StrTriple{term.Id, term.TopologyKey, domain}) {
				return false
			}
		}
	}

	// If our pod has anti-affinity terms and the node has some
	// pod that matches, then we cannot use the node.
	if len(antiAffinityTerms) > 0 {
		if matchingPods.Has(StrTuple{antiAffinityTerms, nodeName}) {
			return false
		}
	}

	/*
		// If the node has pods with outgoing anti-affinity terms that
		// match us, then we can't use the node.
		for _, termId := range matchingAffinityTerms {
			if outgoing.Has(StrTuple{termId, nodeName}) {
				return false
			}
		}
	*

	return true
}

func getPodMatchingAffinityTerms(pod fwk.PodInfo, state State) []string {
	matching := []string{}
	terms := GetMap[string](state, "affinityTerms")
	for termId, term := range terms.All() {
		// XXX need to use the namespace selector as well by using term.Matches
		// rather than term.Selector.Matches.
		if term.(*fwk.AffinityTerm).Selector.Matches(labels.Set(pod.GetPod().Labels)) {
			matching = append(matching, termId)
		}
	}
	return matching
}

func filterWithFastPodAffinity(pods []fwk.PodInfo, nodes []fwk.NodeInfo) {
	spec := NewSpec()
	spec.New("podInfos", NewExternalSource[string]())
	spec.New("nodes", NewExternalSource[string]())
	FastPodAffinity(spec)

	state := New(spec)

	podMap := GetExternalSource[string](state, "podInfos")
	nodeMap := GetExternalSource[string](state, "nodes")
	matchingPods := GetMap[StrTuple](state, "podsPerTermDomain")

	for _, node := range nodes {
		nodeMap.Update(node.Node().Name, node)
	}

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
			if Filter(node, podInfo, state, matching, aff, anti, matchingPods) &&
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
