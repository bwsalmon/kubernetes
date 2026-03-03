package forttest

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/pkg/scheduler/backend/fort"
)

// Data structures used for our test.
type TestNode struct {
	Name    string
	Domains []string
	Pods    []TestPod
}

type TestPod struct {
	Label string
}

type DomLabel struct {
	Domain string
	Label  string
}

type DomNode struct {
	Domain string
	Node   string
}

type DomCount struct {
	Domain string
	Count  int64
}

// The state managed by Fort.

type TestData struct {
	Nodes             fort.WriteMap[string, *TestNode]
	DomainLabelCounts fort.Source[DomLabel, int64]
	LabelSetsInt      fort.Source[string, sets.Set[DomCount]]
	LabelSets         fort.ReadMap[string, sets.Set[DomCount]]
}

// If passed an empty TestData as cloneFrom this will intitialize
// a new TestData structure. If passed a non-nil cloneFrom
// this will make the given TestData a clone of cloneFrom.
func (d *TestData) InitOrClone(cloneFrom *TestData) {
	d.Nodes = fort.NewWriteMap(cloneFrom.Nodes)

	d.DomainLabelCounts = fort.MapReduce(
		d.Nodes,
		func(kv *fort.KeyValue[string, *TestNode]) fort.KeyValueSet[DomLabel, int64] {
			node := kv.Value
			ret := fort.KeyValueSet[DomLabel, int64]{}

			// For each node, create an entry for each pod associated
			// with the pod's label and each domain of the node.
			for _, p := range node.Pods {
				for _, dom := range node.Domains {
					ret = append(ret, fort.KeyValue[DomLabel, int64]{
						Key:   DomLabel{Domain: dom, Label: p.Label},
						Value: 1,
					})
				}
			}
			return ret
		},
		// Sum the number of pods in each domain, label pair.
		fort.Sum[int64],
		cloneFrom.DomainLabelCounts,
	)

	d.LabelSetsInt = fort.MapReduce(
		d.DomainLabelCounts,
		func(kv *fort.KeyValue[DomLabel, int64]) fort.KeyValueSet[string, DomCount] {
			// Now that we have the counts per label/domain, group them by label.
			return fort.KeyValueSet[string, DomCount]{
				{
					Key:   kv.Key.Label,
					Value: DomCount{Domain: kv.Key.Domain, Count: kv.Value},
				},
			}
		},
		// Track the domains and their counts per label.
		fort.Distinct[DomCount],
		cloneFrom.LabelSetsInt,
	)

	// Make our label indexed map available externally.
	d.LabelSets = fort.NewReadMap(
		d.LabelSetsInt,
		cloneFrom.LabelSets,
	)
}

func TestLightPodSpread(t *testing.T) {
	d := fort.New[TestData]()

	d.Nodes.Update("foo", &TestNode{
		Domains: []string{"d"},
		Pods: []TestPod{
			{
				Label: "p",
			},
		},
	})

	d.Nodes.Update("foo2", &TestNode{
		Domains: []string{"d"},
		Pods: []TestPod{
			{
				Label: "p",
			},
			{
				Label: "q",
			},
		},
	})

	d.Nodes.Update("foo3", &TestNode{
		Domains: []string{"e"},
		Pods: []TestPod{
			{
				Label: "p",
			},
			{
				Label: "q",
			},
		},
	})

	d.DomainLabelCounts.Print()
	d.LabelSetsInt.Print()

	myClone := fort.Clone(d)

	myClone.DomainLabelCounts.Print()
	myClone.LabelSetsInt.Print()
}
