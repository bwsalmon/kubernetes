package fort

import (
	"fmt"
	"math/rand"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// Constants to ensure "random" labels actually match affinity selectors
var (
	TopologyZones   = []string{"us-east-1a", "us-east-1b", "us-west-2a"}
	TopologyRegions = []string{"us-east", "us-west"}
	AppLabels       = []string{"web", "database", "cache", "worker"}
	Namespaces      = []string{"default", "system", "prod"}
)

// TestData holds our generated world view
type TestData struct {
	Nodes        []fwk.NodeInfo
	IncomingPods []fwk.PodInfo
}

func TestFastAffinity(t *testing.T) {
	/*
		f, err := os.Create("profile.pprof")
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
		rand.Seed(time.Now().UnixNano())
	*/

	// Generate 10 nodes with 50 existing pods spread across them
	// Generate 20 incoming pods with various affinity rules
	data := GenerateSchedulerTestData(10000, 20000, 40000)

	fmt.Printf("Generated %d Nodes and %d Incoming Pods.\n", len(data.Nodes), len(data.IncomingPods))
	fmt.Printf("Example Node: %s (Zone: %s)\n", data.Nodes[0].Node().Name, data.Nodes[0].Node().Labels["topology.kubernetes.io/zone"])

	filterWithFastPodAffinity(data.IncomingPods, data.Nodes)
}

func GenerateSchedulerTestData(numNodes, numExistingPods, numIncomingPods int) TestData {
	nodes := make([]fwk.NodeInfo, 0, numNodes)
	nodeNames := make([]string, 0, numNodes)

	// 1. Create Nodes with Topology Labels
	for i := 0; i < numNodes; i++ {
		nodeName := fmt.Sprintf("node-%d", i)
		nodeNames = append(nodeNames, nodeName)

		node := &v1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: nodeName,
				Labels: map[string]string{
					"kubernetes.io/hostname":        nodeName,
					"topology.kubernetes.io/zone":   getRandom(TopologyZones),
					"topology.kubernetes.io/region": getRandom(TopologyRegions),
				},
			},
		}
		ni := framework.NewNodeInfo()
		ni.SetNode(node)
		nodes = append(nodes, ni)
	}

	// 2. Create Existing Pods (Simulate running workloads)
	// We randomly assign these to nodes to create the "state" for anti-affinity
	for i := 0; i < numExistingPods; i++ {
		nodeIdx := rand.Intn(len(nodes))
		pod := makeBasePod(fmt.Sprintf("existing-pod-%d", i))
		pod.Spec.NodeName = nodes[nodeIdx].Node().Name

		// Add to the framework.NodeInfo
		nodes[nodeIdx].(*framework.NodeInfo).AddPod(pod)
	}

	// 3. Create Incoming Pods with Affinity/Anti-Affinity rules
	incoming := make([]fwk.PodInfo, 0, numIncomingPods)
	for i := 0; i < numIncomingPods; i++ {
		pod := makeBasePod(fmt.Sprintf("incoming-pod-%d", i))

		// Randomly decide to add affinity rules
		switch rand.Intn(3) {
		case 0:
			addPodAffinity(pod) // Wants to be near other apps
		case 1:
			addPodAntiAffinity(pod) // Wants to be away from other apps
		case 2:
			// No affinity (baseline)
		}

		pInfo, _ := framework.NewPodInfo(pod)
		incoming = append(incoming, pInfo)
	}

	return TestData{
		Nodes:        nodes,
		IncomingPods: incoming,
	}
}

// --- Helpers ---

func makeBasePod(name string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: getRandom(Namespaces),
			Labels: map[string]string{
				"app": getRandom(AppLabels),
			},
			UID: types.UID("uid-" + name), // UID is often required for scheduler cache
		},
		Spec: v1.PodSpec{
			// Basic container to pass validation
			Containers: []v1.Container{{Name: "ctr", Image: "pause"}},
		},
	}
}

// addPodAffinity adds a rule: "I must run on a node that has a pod with label app=[random]"
func addPodAffinity(pod *v1.Pod) {
	targetApp := getRandom(AppLabels)
	pod.Spec.Affinity = &v1.Affinity{
		PodAffinity: &v1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
				{
					LabelSelector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key:      "app",
								Operator: metav1.LabelSelectorOpIn,
								Values:   []string{targetApp},
							},
						},
					},
					TopologyKey: "kubernetes.io/hostname", // Co-location on same node
				},
			},
		},
	}
}

// addPodAntiAffinity adds a rule: "I must NOT run in a ZONE that has a pod with label app=[random]"
func addPodAntiAffinity(pod *v1.Pod) {
	targetApp := getRandom(AppLabels)
	pod.Spec.Affinity = &v1.Affinity{
		PodAntiAffinity: &v1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
				{
					LabelSelector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key:      "app",
								Operator: metav1.LabelSelectorOpIn,
								Values:   []string{targetApp},
							},
						},
					},
					TopologyKey: "topology.kubernetes.io/zone", // Spread across zones
				},
			},
		},
	}
}

func getRandom(options []string) string {
	return options[rand.Intn(len(options))]
}
