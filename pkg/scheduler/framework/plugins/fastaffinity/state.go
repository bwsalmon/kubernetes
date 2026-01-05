package fastaffinity

import (
	v1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/fort"
)

type affinityTermKey struct {
	antiAffinity bool
	termId       string
}

type affinityTermTopoKey struct {
	antiAffinity bool
	termId       string
	topoKey      string
	topoValue    string
}

func getTopoDomainIfPodOnNodeMatchesTerm(nodeInfo fwk.NodeInfo, termId affinityTermKey, term *fwk.AffinityTerm) fort.KeyValueSet[affinityTermTopoKey] {
	node := nodeInfo.Node()
	for _, existingPod := range nodeInfo.GetPods() {
		if term.Matches(existingPod.GetPod(), nil) {
			if topoValue, found := node.Labels[term.TopologyKey]; found {
				return fort.KeyValueSet[affinityTermTopoKey]{{
					Key: affinityTermTopoKey{
						antiAffinity: termId.antiAffinity,
						termId:       termId.termId,
						topoKey:      term.TopologyKey,
						topoValue:    topoValue,
					},
					Value: true,
				}}
			}
		}
	}
	return fort.KeyValueSet[affinityTermTopoKey]{}
}

func getTermId(term *fwk.AffinityTerm) string {
	return ""
}

func addTerms(terms []fwk.AffinityTerm, anti bool, out *fort.KeyValueSet[affinityTermKey]) {
	for _, t := range terms {
		termsId := getTermId(&t)
		*out = append(*out, fort.KeyValue[affinityTermKey]{
			Key: affinityTermKey{
				antiAffinity: anti,
				termId:       termsId,
			},
			Value: &t,
		})
	}
}

func getRequiredAffinityTermsFromPodsOnNodeIndexedByTermId(nodeInfo fwk.NodeInfo) fort.KeyValueSet[affinityTermKey] {
	ret := fort.KeyValueSet[affinityTermKey]{}
	for _, existingPod := range nodeInfo.GetPods() {
		addTerms(existingPod.GetRequiredAffinityTerms(), false, &ret)
		addTerms(existingPod.GetRequiredAntiAffinityTerms(), true, &ret)
	}
	return ret
}

func addTermsTopo(terms []fwk.AffinityTerm, anti bool, node *v1.Node, out *fort.KeyValueSet[affinityTermTopoKey]) {
	for _, t := range terms {
		termId := getTermId(&t)
		for _, t := range terms {
			if topoValue, found := node.Labels[t.TopologyKey]; found {
				*out = append(*out, fort.KeyValue[affinityTermTopoKey]{
					Key: affinityTermTopoKey{
						antiAffinity: anti,
						termId:       termId,
						topoKey:      t.TopologyKey,
						topoValue:    topoValue,
					},
					Value: true,
				})
			}
		}
	}
}

func getRequiredAffinityTermsFromPodsOnNodeIndexedByTopoDomain(nodeInfo fwk.NodeInfo) fort.KeyValueSet[affinityTermTopoKey] {
	node := nodeInfo.Node()

	ret := fort.KeyValueSet[affinityTermTopoKey]{}
	for _, existingPod := range nodeInfo.GetPods() {
		addTermsTopo(existingPod.GetRequiredAffinityTerms(), false, node, &ret)
		addTermsTopo(existingPod.GetRequiredAntiAffinityTerms(), true, node, &ret)
	}
	return ret
}

func ifNodeMatchesDomainReturnNodeNameAndTermKey(kv *fort.KeyValue[fort.JoinKey[string, affinityTermTopoKey]]) fort.KeyValueSet[string] {
	nodeName := kv.Key.Left
	termKey := kv.Key.Right
	nodeInfo := kv.Value.(fort.JoinValue).Left.(fwk.NodeInfo)
	if nodeTopoValue, found := nodeInfo.Node().Labels[termKey.topoKey]; found {
		if nodeTopoValue == termKey.topoValue {
			return fort.KeyValueSet[string]{{
				Key:   nodeName,
				Value: affinityTermKey{antiAffinity: termKey.antiAffinity, termId: termKey.termId},
			}}
		}
	}
	return fort.KeyValueSet[string]{}
}

func setupState(spec fort.Spec) {
	spec.New(
		"nodeInfos",
		fort.NewExternalSource[string](),
	)

	// Collect all the unique affinity terms across all of the pods in the system.
	// Do this by mapping each node (and each pod on the node) and creating
	// a key-value set with a unique affinity term id as the key.
	spec.New(
		"_terms",
		fort.MapReduce(
			func(kv *fort.KeyValue[string]) fort.KeyValueSet[affinityTermKey] {
				nodeInfo := kv.Value.(fwk.NodeInfo)
				return getRequiredAffinityTermsFromPodsOnNodeIndexedByTermId(nodeInfo)
			},
			fort.AnyValue, // since all terms with the same id are identical, keep a single value (any one) for each key.
			"nodeInfos",   // map across all the nodeInfos.
		),
	)

	// Collect the count of pods owning each affinity term by topology domain.
	// Do this by mapping each node (and each pod on the node) and creating
	// a key-value set with a combination of a unique affinity term id and the topology domain
	// as the key. Note that this is a very similar operation to the "terms" mapping
	// but keeps domain counts rather than the value of distinct affinity terms.
	spec.New(
		"topoDomainOwning",
		fort.MapReduce(
			func(kv *fort.KeyValue[string]) fort.KeyValueSet[affinityTermTopoKey] {
				nodeInfo := kv.Value.(fwk.NodeInfo)
				return getRequiredAffinityTermsFromPodsOnNodeIndexedByTopoDomain(nodeInfo)
			},
			fort.AnyValue, // we only care if some pod matches the term in this domain, not the actual count.
			"nodeInfos",   // map across all nodeInfos.
		),
	)

	// Create a logical map with each node paired with each affinity term.
	spec.New(
		"nodeTerms",
		fort.FullJoin[affinityTermKey, string]("nodeInfos", "_terms"),
	)

	// Collect the count of pods that match each affinity term by topology domain. Do this by
	// mapping each affinity term / node pair and returning the counts of pods on the node that match
	// the given term keyed by a combination of affinity term id and topology domain.
	spec.New(
		"topoDomainMatching",
		fort.MapReduce(
			func(kv *fort.KeyValue[fort.JoinKey[string, affinityTermKey]]) fort.KeyValueSet[affinityTermTopoKey] {
				nodeInfo := kv.Value.(fort.JoinValue).Left.(fwk.NodeInfo)
				termsId := kv.Key.Right
				term := kv.Value.(fort.JoinValue).Right.(*fwk.AffinityTerm)
				return getTopoDomainIfPodOnNodeMatchesTerm(nodeInfo, termsId, term)
			},
			fort.AnyValue, // only care if some pod matches, not the actual count.
			"nodeTerms",   // map across all nodeInfos.
		),
	)

	// For each node, get a set of terms where at least one
	// pod in one domain associated with the node owns the term.

	// First join each node with each term / domain count.
	spec.New(
		"nodeTopoDomainOwning",
		fort.FullJoin[string, affinityTermTopoKey]("nodeInfos", "topoDomainOwning"),
	)

	// Find topo domains that match the node, and then aggregate by node name and
	// affinity term. The result is an entry for each node with the keys of terms where
	// at least one pod in one topo domain matching the given node owns the given term.
	spec.New(
		"_nodeMatchesPodOwningTerm",
		fort.MapReduce(
			ifNodeMatchesDomainReturnNodeNameAndTermKey,
			fort.Set, // collect a set of values with the given key.
			"nodeTopoDomainOwning",
		),
	)

	// For each node, get a set of terms where at least one
	// pod in one domain associated with the node matches the term.

	spec.New(
		"nodeTopoDomainMatching",
		fort.FullJoin[string, affinityTermTopoKey]("nodeInfos", "topoDomainMatching"),
	)

	// Find topo domains that match the node, and then aggregate by node name and
	// affinity term. The result is an entry for each node with the keys of terms where
	// at least one pod in one topo domain matching the given node matches the given term.
	spec.New(
		"_nodeMatchesPodMatchingTerm",
		fort.MapReduce(
			ifNodeMatchesDomainReturnNodeNameAndTermKey,
			fort.Set, // collect a set of values with the given key.
			"nodeTopoDomainMatching",
		),
	)

	// Externally queryable maps.
	spec.New(
		"terms",
		fort.Materialize[affinityTermKey]("_terms"),
	)

	spec.New(
		"nodeMatchesPodOwningTerm",
		fort.Materialize[string]("_nodeMatchesPodOwningTerm"),
	)

	spec.New(
		"nodeMatchesPodMatchingTerm",
		fort.Materialize[string]("_nodeMatchesPodMatchingTerm"),
	)
}
