package fort

import (
	"encoding/json"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	fwk "k8s.io/kube-scheduler/framework"
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

func SetupState(spec StateSpec, defaultConstraints []v1.TopologySpreadConstraint) {
	spec.New("pods", NewExternalSource[string]())
	spec.New("services", NewExternalSource[string]())
	spec.New("nodes", NewExternalSource[string]())

	spec.New("_podServices",
		FullJoin[string, string](
			"pods",
			"services",
		),
	)

	spec.New("_podConstraints",
		MapReduce(
			func(kv *KeyValue[JoinKey[string, string]]) KeyValueSet[string] {
				podInfo := kv.Value.(JoinValue).Left.(fwk.PodInfo)
				service := kv.Value.(JoinValue).Right.(*v1.Service)
				pod := podInfo.GetPod()

				ret := KeyValueSet[string]{}

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

					ret = append(ret, KeyValue[string]{
						Key:   pod.Name,
						Value: constraint,
					})
				}
				return ret
			},
			AnyValue,
			"_podServices",
		),
	)

	spec.New("_constraints",
		MapReduce(
			func(kv *KeyValue[string]) KeyValueSet[string] {
				constraint := kv.Value.(ConstraintWId)
				return KeyValueSet[string]{{Key: constraint.Id, Value: constraint}}
			},
			AnyValue,
			"_podConstraints",
		),
	)

	spec.New("_topoKeys",
		MapReduce(
			func(kv *KeyValue[string]) KeyValueSet[string] {
				constraint := kv.Value.(Constraint)
				ret := KeyValueSet[string]{}
				for _, c := range constraint.TopoConstraints {
					ret = append(ret, KeyValue[string]{Key: c.TopologyKey, Value: true})
				}
				return ret
			},
			AnyValue,
			"_constraints",
		),
	)

	spec.New("_nodeTopoKeys",
		FullJoin[string, string]("nodes", "_topoKeys"),
	)

	spec.New("_topoKeyDomains",
		MapReduce(
			func(kv *KeyValue[JoinKey[string, string]]) KeyValueSet[StrTuple] {
				joined := kv.Value.(JoinValue)
				node := joined.Left.(*v1.Node)
				topoKey := joined.Right.(string)
				if topoDomain, hasValue := node.Labels[topoKey]; hasValue {
					return KeyValueSet[StrTuple]{
						{
							Key:   StrTuple{topoKey, topoDomain},
							Value: true,
						},
					}
				}
				return KeyValueSet[StrTuple]{}
			},
			AnyValue,
			"_nodeTopoKeys",
		),
	)

	spec.New("_topoKeyNumDomains",
		MapReduce(
			func(kv *KeyValue[StrTuple]) KeyValueSet[string] {
				return KeyValueSet[string]{
					{
						Key:   kv.Key[0],
						Value: kv.Key[1],
					},
				}
			},
			Count,
			"_nodeTopoKeys",
		),
	)

	spec.New("_podNodes",
		LookupJoin(
			"pods",
			"nodes",
			func(kv *KeyValue[string]) string {
				pod := kv.Value.(fwk.PodInfo).GetPod()
				return pod.Spec.NodeName
			},
		),
	)

	spec.New("_constraintPodNodes",
		FullJoin[string, string](
			"_constraints",
			"_podNodes",
		),
	)

	spec.New("_constraintDomainPodCounts",
		MapReduce(
			func(kv *KeyValue[JoinKey[string, string]]) KeyValueSet[StrTriple] {
				constraints := kv.Value.(JoinValue).Left.(ConstraintWId)
				podInfo := kv.Value.(JoinValue).Right.(JoinValue).Left.(fwk.PodInfo)
				node := kv.Value.(JoinValue).Right.(JoinValue).Right.(*v1.Node)
				pod := podInfo.GetPod()

				ret := KeyValueSet[StrTriple]{}
				if podMatchesSelector(pod, constraints.Constraint.Selector) {
					for _, c := range constraints.Constraint.TopoConstraints {
						topoValue := node.Labels[c.TopologyKey]
						ret = append(ret, KeyValue[StrTriple]{
							Key:   StrTriple{constraints.Id, c.TopologyKey, topoValue},
							Value: true,
						})
					}
				}
				return ret
			},
			Count,
			"_constraintPodNodes",
		),
	)

	spec.New("_constraintPodCounts",
		MapReduce(
			func(kv *KeyValue[JoinKey[string, string]]) KeyValueSet[string] {
				constraints := kv.Value.(JoinValue).Left.(ConstraintWId)
				podInfo := kv.Value.(JoinValue).Right.(JoinValue).Left.(fwk.PodInfo)
				pod := podInfo.GetPod()

				if podMatchesSelector(pod, constraints.Constraint.Selector) {
					return KeyValueSet[string]{{Key: constraints.Id, Value: true}}
				}
				return KeyValueSet[string]{}
			},
			Count,
			"_constraintPodNodes",
		),
	)

	spec.New("constraintDomainPodCounts", Materialize[StrTriple]("_constraintDomainPodCounts"))
	spec.New("constraintPodCounts", Materialize[string]("_constraintPodCounts"))
	spec.New("podConstraints", Materialize[string]("_podConstraints"))
	spec.New("topoKeyNumDomains", Materialize[string]("_topoKeyNumDomains"))
}
