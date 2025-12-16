package fastpodspread

import (
	"context"
	"encoding/json"
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/fort"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
)

type ConstraintWId struct {
	Id         string
	Constraint Constraint
}

type Constraint struct {
	Selector        map[string]string
	TopoConstraints []v1.TopologySpreadConstraint
}

func podMatchesSelector(pod *v1.Pod, selector map[string]string) bool {
	if selector == nil {
		// nil selectors match nothing, not everything.
		return false
	}
	selectorObj := labels.Set(selector).AsSelectorPreValidated()
	return selectorObj.Matches(labels.Set(pod.Labels))
}

func SetupState(informerFactory informers.SharedInformerFactory, spec fort.StateSpec, defaultConstraints []v1.TopologySpreadConstraint) {
	spec.New(
		"pods",
		fort.WrapInformer(informerFactory.Core().V1().Pods().Informer()),
	)
	spec.New(
		"services",
		fort.WrapInformer(informerFactory.Core().V1().Services().Informer()),
	)
	spec.New(
		"nodes",
		fort.WrapInformer(informerFactory.Core().V1().Nodes().Informer()),
	)
	spec.New(
		"rcs",
		fort.WrapInformer(informerFactory.Core().V1().ReplicationControllers().Informer()),
	)

	spec.New("_podServices",
		fort.FullJoin[string, string](
			"pods",
			"services",
		),
	)

	spec.New("_selectorConstraints",
		fort.MapReduce(
			func(kv *fort.KeyValue[fort.JoinKey[string, string]]) fort.KeyValueSet[string] {
				podInfo := kv.Value.(fort.JoinValue).Left.(fwk.PodInfo)
				service := kv.Value.(fort.JoinValue).Right.(*v1.Service)
				pod := podInfo.GetPod()

				ret := fort.KeyValueSet[string]{}

				if podMatchesSelector(podInfo.GetPod(), service.Spec.Selector) {
					topo := defaultConstraints
					if len(pod.Spec.TopologySpreadConstraints) > 0 {
						topo = pod.Spec.TopologySpreadConstraints
					}

					constraint := &ConstraintWId{
						Constraint: Constraint{
							Selector:        service.Spec.Selector,
							TopoConstraints: topo,
						},
					}

					constraintId, _ := json.Marshal(constraint.Constraint)
					constraint.Id = string(constraintId)

					ret = append(ret, fort.KeyValue[string]{
						Key:   pod.Name,
						Value: constraint,
					})
				}
				return ret
			},
			fort.AnyValue,
			"_podServices",
		),
	)

	spec.New("_podRcs",
		fort.LookupJoin(
			"pods",
			"rcs",
			func(kv *fort.KeyValue[string]) string {
				pod := kv.Value.(*v1.Pod)
				owner := metav1.GetControllerOfNoCopy(pod)
				if owner != nil {
					return owner.Name
				}
				return ""
			},
		),
	)

	spec.New("_rcConstraints",
		fort.MapReduce(
			func(kv *fort.KeyValue[fort.JoinKey[string, string]]) fort.KeyValueSet[string] {
				podInfo := kv.Value.(fort.JoinValue).Left.(fwk.PodInfo)
				rc := kv.Value.(fort.JoinValue).Right.(*v1.ReplicationController)
				pod := podInfo.GetPod()

				ret := fort.KeyValueSet[string]{}

				topo := defaultConstraints
				if len(pod.Spec.TopologySpreadConstraints) > 0 {
					topo = pod.Spec.TopologySpreadConstraints
				}

				constraint := &ConstraintWId{
					Constraint: Constraint{
						Selector:        rc.Spec.Selector,
						TopoConstraints: topo,
					},
				}

				constraintId, _ := json.Marshal(constraint.Constraint)
				constraint.Id = string(constraintId)

				ret = append(ret, fort.KeyValue[string]{
					Key:   pod.Name,
					Value: constraint,
				})
				return ret
			},
			fort.AnyValue,
			"_podRcs",
		),
	)

	spec.New("_allConstraints",
		fort.Union(map[string]string{
			"rc":       "_rcConstraints",
			"selector": "_selectorConstraints",
		}),
	)

	spec.New("_constraints",
		fort.MapReduce(
			func(kv *fort.KeyValue[string]) fort.KeyValueSet[string] {
				constraint := kv.Value.(ConstraintWId)
				return fort.KeyValueSet[string]{{Key: constraint.Id, Value: constraint}}
			},
			fort.AnyValue,
			"_allConstraints",
		),
	)

	spec.New("_topoKeys",
		fort.MapReduce(
			func(kv *fort.KeyValue[string]) fort.KeyValueSet[string] {
				constraint := kv.Value.(Constraint)
				ret := fort.KeyValueSet[string]{}
				for _, c := range constraint.TopoConstraints {
					ret = append(ret, fort.KeyValue[string]{Key: c.TopologyKey, Value: true})
				}
				return ret
			},
			fort.AnyValue,
			"_constraints",
		),
	)

	spec.New("_nodeTopoKeys",
		fort.FullJoin[string, string](
			"nodes",
			"_topoKeys",
		),
	)

	spec.New("_topoDomains",
		fort.MapReduce(
			func(kv *fort.KeyValue[fort.JoinKey[string, string]]) fort.KeyValueSet[fort.StrTuple] {
				joined := kv.Value.(fort.JoinValue)
				node := joined.Left.(*v1.Node)
				topoKey := joined.Right.(string)
				if topoDomain, hasValue := node.Labels[topoKey]; hasValue {
					return fort.KeyValueSet[fort.StrTuple]{
						{
							Key:   fort.StrTuple{topoKey, topoDomain},
							Value: true,
						},
					}
				}
				return fort.KeyValueSet[fort.StrTuple]{}
			},
			fort.AnyValue,
			"_nodeTopoKeys",
		),
	)

	spec.New("_topoKeyNumDomains",
		fort.MapReduce(
			func(kv *fort.KeyValue[fort.StrTuple]) fort.KeyValueSet[string] {
				return fort.KeyValueSet[string]{
					{
						Key:   kv.Key[0],
						Value: kv.Key[1],
					},
				}
			},
			fort.Count,
			"_topoDomains",
		),
	)

	spec.New("_podNodes",
		fort.LookupJoin(
			"pods",
			"nodes",
			func(kv *fort.KeyValue[string]) string {
				pod := kv.Value.(fwk.PodInfo).GetPod()
				return pod.Spec.NodeName
			},
		),
	)

	spec.New("_constraintPodNodes",
		fort.FullJoin[string, string](
			"_constraints",
			"_podNodes",
		),
	)

	spec.New("_constraintDomainPodCounts",
		fort.MapReduce(
			func(kv *fort.KeyValue[fort.JoinKey[string, string]]) fort.KeyValueSet[fort.StrTriple] {
				constraints := kv.Value.(fort.JoinValue).Left.(ConstraintWId)
				podInfo := kv.Value.(fort.JoinValue).Right.(fort.JoinValue).Left.(fwk.PodInfo)
				node := kv.Value.(fort.JoinValue).Right.(fort.JoinValue).Right.(*v1.Node)
				pod := podInfo.GetPod()

				ret := fort.KeyValueSet[fort.StrTriple]{}
				if podMatchesSelector(pod, constraints.Constraint.Selector) {
					for _, c := range constraints.Constraint.TopoConstraints {
						topoValue := node.Labels[c.TopologyKey]
						ret = append(ret, fort.KeyValue[fort.StrTriple]{
							Key:   fort.StrTriple{constraints.Id, c.TopologyKey, topoValue},
							Value: true,
						})
					}
				}
				return ret
			},
			fort.Count,
			"_constraintPodNodes",
		),
	)

	spec.New("_constraintPodCounts",
		fort.MapReduce(
			func(kv *fort.KeyValue[fort.JoinKey[string, string]]) fort.KeyValueSet[string] {
				constraints := kv.Value.(fort.JoinValue).Left.(ConstraintWId)
				podInfo := kv.Value.(fort.JoinValue).Right.(fort.JoinValue).Left.(fwk.PodInfo)
				pod := podInfo.GetPod()

				if podMatchesSelector(pod, constraints.Constraint.Selector) {
					return fort.KeyValueSet[string]{{Key: constraints.Id, Value: true}}
				}
				return fort.KeyValueSet[string]{}
			},
			fort.Count,
			"_constraintPodNodes",
		),
	)

	spec.New("constraintDomainPodCounts", fort.Materialize[fort.StrTriple]("_constraintDomainPodCounts"))
	spec.New("constraintPodCounts", fort.Materialize[string]("_constraintPodCounts"))
	spec.New("constraints", fort.Materialize[string]("_constraints"))
	spec.New("topoKeyNumDomains", fort.Materialize[string]("_topoKeyNumDomains"))
}

type FastPodSpreadPodState struct {
	Constraints         *ConstraintWId
	NumDomains          map[string]int
	PodCount            int
	ConstraintPodCounts fort.KeyValueMap[fort.StrTriple]
}

func (s *FastPodSpreadPodState) Clone() fwk.StateData {
	return s
}

type FastPodSpreadPlugin struct {
	state fort.State
}

var _ fwk.PreFilterPlugin = &FastPodSpreadPlugin{}
var _ fwk.FilterPlugin = &FastPodSpreadPlugin{}

// Name is the name of the plugin used in the plugin registry and configurations.
const Name = names.FastPodSpread

var systemDefaultConstraints = []v1.TopologySpreadConstraint{
	{
		TopologyKey:       v1.LabelHostname,
		WhenUnsatisfiable: v1.ScheduleAnyway,
		MaxSkew:           3,
	},
	{
		TopologyKey:       v1.LabelTopologyZone,
		WhenUnsatisfiable: v1.ScheduleAnyway,
		MaxSkew:           5,
	},
}

func New(_ context.Context, plArgs runtime.Object, h fwk.Handle, fts feature.Features) (fwk.Plugin, error) {
	spec := fort.NewSpec()
	SetupState(h.SharedInformerFactory(), spec, systemDefaultConstraints)
	state := fort.New(spec)
	return &FastPodSpreadPlugin{state: state}, nil
}

func (p *FastPodSpreadPlugin) Name() string {
	return Name
}

func (p *FastPodSpreadPlugin) PreFilter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	podState := p.state //p.state.Clone()
	constraintsMap := fort.GetMap[string](podState, "constraints")
	constraintPodCountsMap := fort.GetMap[string](podState, "constraintPodCounts")
	topoKeyDomainCountsMap := fort.GetMap[string](podState, "topoKeyNumDomains")
	constraintDomainPodCounts := fort.GetMap[fort.StrTriple](podState, "constraintDomainPodCounts")

	id := string(pod.UID)
	thisPodConstraints, found := constraintsMap.Get(id)
	if !found {
		return nil, nil
	}

	constraints := thisPodConstraints.(*ConstraintWId)
	podCountVal, _ := constraintPodCountsMap.Get(constraints.Id)
	podCount := podCountVal.(int)

	numDomains := map[string]int{}
	for _, c := range constraints.Constraint.TopoConstraints {
		val, _ := topoKeyDomainCountsMap.Get(c.TopologyKey)
		numDomains[c.TopologyKey] = val.(int)
	}

	st := &FastPodSpreadPodState{
		Constraints:         constraints,
		NumDomains:          numDomains,
		PodCount:            podCount,
		ConstraintPodCounts: constraintDomainPodCounts,
	}

	fmt.Println("Filter presuccess")

	state.Write("FastPodSpread", st)

	return nil, nil
}

func (p *FastPodSpreadPlugin) PreFilterExtensions() fwk.PreFilterExtensions {
	return nil
}

func (p *FastPodSpreadPlugin) Filter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	stateHandle, err := cycleState.Read("FastPodSpread")
	if err != nil {
		return nil
	}

	state := stateHandle.(*FastPodSpreadPodState)
	domainCounts := map[string]int{}
	for _, c := range state.Constraints.Constraint.TopoConstraints {
		domain := nodeInfo.Node().Labels[c.TopologyKey]
		val, _ := state.ConstraintPodCounts.Get(fort.StrTriple{state.Constraints.Id, c.TopologyKey, domain})
		domainCounts[c.TopologyKey] = val.(int)
	}

	for key, domains := range state.NumDomains {
		count := domainCounts[key]
		minPods := count / domains
		_ = count + 1 - minPods
	}

	return nil
}
