package fastaffinity

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/fort"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
)

type FastPodAffinityPodState struct {
	PodOwningTerms       []affinityTermKey
	PodMatchingTerms     sets.Set[affinityTermKey]
	PodOwnsAffinityTerms bool
	NodeOwningMap        fort.KeyValueMap[string]
	NodeMatchingMap      fort.KeyValueMap[string]
}

func (s *FastPodAffinityPodState) Clone() fwk.StateData {
	return s
}

type FastPodSpreadPlugin struct {
	state fort.DataFort
}

var _ fwk.PreFilterPlugin = &FastPodSpreadPlugin{}
var _ fwk.FilterPlugin = &FastPodSpreadPlugin{}

// Name is the name of the plugin used in the plugin registry and configurations.
const Name = names.FastPodAffinity

func (p *FastPodSpreadPlugin) Name() string {
	return Name
}

func New(_ context.Context, plArgs runtime.Object, h fwk.Handle, fts feature.Features) (fwk.Plugin, error) {
	spec := fort.NewSpec()
	setupState(spec)
	state := fort.New(spec)
	return &FastPodSpreadPlugin{state: state}, nil
}

func (p *FastPodSpreadPlugin) PreFilter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	podState := p.state //p.state.Clone()

	// Find all the affinity terms in the system that match this pod.
	affinityTermsMap := fort.GetItem[fort.KeyValueMap[affinityTermKey]](podState, "terms")
	podMatches := sets.New[affinityTermKey]()
	for termId, termItem := range affinityTermsMap.All() {
		term := termItem.(*fwk.AffinityTerm)
		if term.Matches(pod, nil) {
			podMatches.Insert(termId)
		}
	}

	// Get ids from the affinity terms the pod owns.
	podInfo, err := framework.NewPodInfo(pod)
	if err != nil {
		return nil, fwk.AsStatus(err)
	}

	podOwns := []affinityTermKey{}
	hasAffinityTerms := len(podInfo.GetRequiredAffinityTerms()) > 0
	for _, aff := range podInfo.GetRequiredAffinityTerms() {
		termId := affinityTermKey{
			antiAffinity: false,
			termId:       getTermId(&aff),
		}
		podOwns = append(podOwns, termId)
	}
	for _, aff := range podInfo.GetRequiredAntiAffinityTerms() {
		termId := affinityTermKey{
			antiAffinity: true,
			termId:       getTermId(&aff),
		}
		podOwns = append(podOwns, termId)
	}

	// Construct our prefilter state for this pod. This includes our matching and
	// owning terms, in addition to the pre-built maps for node matching and owning terms.
	prefilterState := &FastPodAffinityPodState{
		PodOwningTerms:       podOwns,
		PodMatchingTerms:     podMatches,
		PodOwnsAffinityTerms: hasAffinityTerms,
		NodeOwningMap:        fort.GetItem[fort.KeyValueMap[string]](podState, "nodeMatchesPodOwningTerm"),
		NodeMatchingMap:      fort.GetItem[fort.KeyValueMap[string]](podState, "nodeMatchesPodMatchingTerm"),
	}

	state.Write("FastPodAffinity", prefilterState)

	return nil, nil
}

func (p *FastPodSpreadPlugin) PreFilterExtensions() fwk.PreFilterExtensions {
	return nil
}

func (p *FastPodSpreadPlugin) Filter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	node := nodeInfo.Node()

	stateHandle, err := cycleState.Read("FastPodAffinity")
	if err != nil {
		return nil
	}

	state := stateHandle.(*FastPodAffinityPodState)

	// The node *must* match all of our pod's affinity terms and *must not* match
	// any of our pod's anti-affinity terms.
	if nodeMatchesItem, found := state.NodeMatchingMap.Get(node.Name); found {
		nodeMatches := nodeMatchesItem.(map[any]int)
		for _, aff := range state.PodOwningTerms {
			_, found := nodeMatches[aff]
			if (found && aff.antiAffinity) || (!found && !aff.antiAffinity) {
				return fwk.NewStatus(fwk.Unschedulable)
			}
		}
	} else if state.PodOwnsAffinityTerms {
		// If the node has no matching terms but the pod has required
		// affinity terms then the node can't match.
		return fwk.NewStatus(fwk.Unschedulable)
	}

	// Our pod *must* match all the node's affinity terms and *must not* match
	// any of the node's anti-affinity terms.
	if nodeOwnsItem, found := state.NodeOwningMap.Get(node.Name); found {
		nodeOwns := nodeOwnsItem.(map[any]int)
		for item := range nodeOwns {
			aff := item.(affinityTermKey)
			_, found := state.PodMatchingTerms[aff]
			if (found && aff.antiAffinity) || (!found && !aff.antiAffinity) {
				return fwk.NewStatus(fwk.Unschedulable)
			}
		}
	}

	// If the node owns no terms then the pod trivially matches all of them.

	return nil
}
