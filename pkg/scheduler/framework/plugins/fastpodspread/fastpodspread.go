package fastpodspread

/*
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

type Constraint struct {
	Owner           *metav1.OwnerReference
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

func getPodConstraint(pod *v1.Pod, defaultConstraints []v1.TopologySpreadConstraint) (string, Constraint) {
	topo := defaultConstraints
	if len(pod.Spec.TopologySpreadConstraints) > 0 {
		topo = pod.Spec.TopologySpreadConstraints
	}

	constraint := Constraint{
		Owner:           metav1.GetControllerOf(pod),
		TopoConstraints: topo,
	}

	constraintSerial, _ := json.Marshal(constraint)
	constraintId := string(constraintSerial)

	return constraintId, constraint
}

func SetupState(informerFactory informers.SharedInformerFactory, spec fort.Spec, defaultConstraints []v1.TopologySpreadConstraint) {
	spec.New(
		"nodeInfos",
		fort.WrapInformer(informerFactory.Core().V1().Nodes().Informer()),
	)

	spec.New(
		"constraintDomainPodCounts",
		fort.MapReduce(
			func(kv *fort.KeyValue[string]) fort.KeyValueSet[fort.StrTriple] {
				nodeInfo := kv.Value.(fwk.NodeInfo)

				ret := fort.KeyValueSet[fort.StrTriple]{}

				for _, podInfo := range nodeInfo.GetPods() {
					pod := podInfo.GetPod()

					constraintId, constraint := getPodConstraint(pod, defaultConstraints)

					for _, t := range constraint.TopoConstraints {
						domain := nodeInfo.Node().Labels[t.TopologyKey]
						ret = append(ret, fort.KeyValue[fort.StrTriple]{
							Key:   fort.StrTriple{constraintId, t.TopologyKey, domain},
							Value: true,
						})
					}
				}
				return ret
			},
			fort.Count,
			"nodeInfos",
		),
	)

	spec.New(
		"constraintCounts",
		fort.MapReduce(
			func(kv *fort.KeyValue[fort.StrTriple]) fort.KeyValueSet[string] {
				return fort.KeyValueSet[string]{{Key: kv.Key[0], Value: kv.Value}}
			},
			fort.Sum,
			"constraintDomainPodCounts",
		),
	)

	spec.New(
		"topoDomains",
		fort.MapReduce(
			func(kv *fort.KeyValue[fort.StrTriple]) fort.KeyValueSet[fort.StrTuple] {
				return fort.KeyValueSet[fort.StrTuple]{{
					Key:   fort.StrTuple{kv.Key[1], kv.Key[2]},
					Value: true,
				}}
			},
			fort.AnyValue,
			"constraintDomainPodCounts",
		),
	)

	spec.New("topoKeyNumDomains",
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
			"topoDomains",
		),
	)

	spec.New("constraintDomainPodCounts", fort.Materialize[fort.StrTriple]("_constraintDomainPodCounts"))
	spec.New("constraintPodCounts", fort.Materialize[string]("_constraintPodCounts"))
	spec.New("topoKeyNumDomains", fort.Materialize[string]("_topoKeyNumDomains"))
}

type FastPodSpreadPodState struct {
	ConstraintsId       string
	Constraints         Constraint
	NumDomains          map[string]int
	PodCount            int
	ConstraintPodCounts fort.KeyValueMap[fort.StrTriple]
}

func (s *FastPodSpreadPodState) Clone() fwk.StateData {
	return s
}

type FastPodSpreadPlugin struct {
	state fort.DataFort
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
	constraintPodCountsMap := fort.GetMap[string](podState, "constraintPodCounts")
	topoKeyDomainCountsMap := fort.GetMap[string](podState, "topoKeyNumDomains")
	constraintDomainPodCounts := fort.GetMap[fort.StrTriple](podState, "constraintDomainPodCounts")

	constraintsId, constraints := getPodConstraint(pod, systemDefaultConstraints)

	podCountVal, _ := constraintPodCountsMap.Get(constraintsId)
	podCount := podCountVal.(int)

	numDomains := map[string]int{}
	for _, c := range constraints.TopoConstraints {
		val, _ := topoKeyDomainCountsMap.Get(c.TopologyKey)
		numDomains[c.TopologyKey] = val.(int)
	}

	st := &FastPodSpreadPodState{
		ConstraintsId:       constraintsId,
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
	for _, c := range state.Constraints.TopoConstraints {
		domain := nodeInfo.Node().Labels[c.TopologyKey]
		val, _ := state.ConstraintPodCounts.Get(fort.StrTriple{state.ConstraintsId, c.TopologyKey, domain})
		domainCounts[c.TopologyKey] = val.(int)
	}

	for key, domains := range state.NumDomains {
		count := domainCounts[key]
		minPods := count / domains
		_ = count + 1 - minPods
	}

	return nil
}
*/
