/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package nodedeclaredfeatures

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/version"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	ndf "k8s.io/component-helpers/nodedeclaredfeatures"
	ndftesting "k8s.io/component-helpers/nodedeclaredfeatures/testing"
	"k8s.io/klog/v2/ktesting"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/parallelize"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultbinder"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultpreemption"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	"k8s.io/kubernetes/pkg/scheduler/framework/preemption"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	internalcache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
	internalqueue "k8s.io/kubernetes/pkg/scheduler/backend/queue"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	tf "k8s.io/kubernetes/pkg/scheduler/testing/framework"
	imageutils "k8s.io/kubernetes/test/utils/image"
)

// createMockFeature is a helper function to create and configure a MockFeature.
func createMockFeature(t *testing.T, name string, infer bool, maxVersionStr string) *ndftesting.MockFeature {
	m := ndftesting.NewMockFeature(t)
	m.SetName(name)
	m.SetInferForScheduling(func(podInfo *ndf.PodInfo) bool { return infer })
	if maxVersionStr != "" {
		m.SetMaxVersion(version.MustParseSemantic(maxVersionStr))
	} else {
		m.SetMaxVersion(nil)
	}
	return m
}

func TestPreFilter(t *testing.T) {
	const (
		feature1 = "TestFeature1"
		feature2 = "TestFeature2"
	)
	mapper := ndf.NewFeatureMapper([]string{feature1, feature2})
	newFS := func(features ...string) ndf.FeatureSet {
		return mapper.MustMapSorted(features)
	}
	_, ctx := ktesting.NewTestContext(t)
	testCases := []struct {
		name              string
		pluginEnabled     bool
		pod               *v1.Pod
		nodeFeatures      []ndf.Feature
		expectedStatus    *fwk.Status
		expectedState     *preFilterState
		componenetVersion string
	}{
		{
			name:              "plugin disabled",
			pluginEnabled:     false,
			pod:               st.MakePod().Name("test-pod").Obj(),
			componenetVersion: "1.35.0",
			nodeFeatures: []ndf.Feature{
				createMockFeature(t, feature1, true, ""),
				createMockFeature(t, feature2, false, ""),
			},
			expectedStatus: fwk.NewStatus(fwk.Skip),
			expectedState:  nil,
		},
		{
			name:              "Pod with feature requirements",
			pluginEnabled:     true,
			pod:               st.MakePod().Name("test-pod").Obj(),
			componenetVersion: "1.35.0",
			nodeFeatures: []ndf.Feature{
				createMockFeature(t, feature1, true, ""),
				createMockFeature(t, feature2, false, ""),
			},
			expectedStatus: fwk.NewStatus(fwk.Success),
			expectedState:  &preFilterState{reqs: newFS(feature1)},
		},
		{
			name:              "Pod with multiple feature requirements",
			pluginEnabled:     true,
			pod:               st.MakePod().Name("test-pod").Obj(),
			componenetVersion: "1.35.0",
			nodeFeatures: []ndf.Feature{
				createMockFeature(t, feature1, true, "1.38.0"),
				createMockFeature(t, feature2, true, "1.38.0"),
			},
			expectedStatus: fwk.NewStatus(fwk.Success),
			expectedState:  &preFilterState{reqs: newFS(feature1, feature2)},
		},
		{
			name:              "Pod with no requirements",
			pluginEnabled:     true,
			pod:               st.MakePod().Name("test-pod").Obj(),
			componenetVersion: "1.35.0",
			nodeFeatures: []ndf.Feature{
				createMockFeature(t, feature1, false, ""),
				createMockFeature(t, feature2, false, ""),
			},
			expectedStatus: fwk.NewStatus(fwk.Skip),
			expectedState:  nil,
		},
		{
			name:              "Feature not required, version > MaxVersion",
			pluginEnabled:     true,
			pod:               st.MakePod().Name("test-pod").Obj(),
			componenetVersion: "1.34.0",
			nodeFeatures: []ndf.Feature{
				createMockFeature(t, feature1, true, "1.33.0"),
				createMockFeature(t, feature2, false, ""),
			},
			expectedStatus: fwk.NewStatus(fwk.Skip),
			expectedState:  nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ndfFramework := ndf.New(tc.nodeFeatures)

			plugin := &NodeDeclaredFeatures{
				ndfFramework: ndfFramework,
				version:      version.MustParseSemantic(tc.componenetVersion),
				enabled:      tc.pluginEnabled,
			}
			cycleState := framework.NewCycleState()
			result, status := plugin.PreFilter(ctx, cycleState, tc.pod, nil)

			if result != nil {
				t.Errorf("PreFilter should always return a nil for result")
			}

			if diff := cmp.Diff(tc.expectedStatus, status); diff != "" {
				t.Errorf("unexpected status (-want,+got):\n%s", diff)
			}

			if tc.expectedState != nil {
				state, err := getPreFilterState(cycleState)
				if err != nil {
					t.Fatalf("getPreFilterState returned unexpected error: %v", err)
				}
				if !tc.expectedState.reqs.Equal(state.reqs) {
					t.Errorf("unexpected preFilterState reqs: want %v, got %v", tc.expectedState.reqs, state.reqs)
				}
			} else {
				_, err := getPreFilterState(cycleState)
				if err == nil {
					t.Fatalf("get prefilter state: %v", err)
				}
			}
		})
	}
}

func TestFilter(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	const (
		featureA = "FeatureA"
		featureB = "FeatureB"
		featureC = "FeatureC"
	)
	f, _ := ndftesting.NewMockFramework(t, featureA, featureB, featureC)
	ndftesting.SetFrameworkDuringTest(t, f)
	newFS := func(features ...string) ndf.FeatureSet {
		return f.MustMapSorted(features)
	}

	testCases := []struct {
		name           string
		pluginEnabled  bool
		pod            *v1.Pod
		node           *v1.Node
		preFilterReqs  []string
		expectedStatus *fwk.Status
	}{
		{
			name:           "plugin disabled",
			pluginEnabled:  false,
			pod:            st.MakePod().Name("test-pod").Obj(),
			node:           st.MakeNode().Name("node-1").DeclaredFeatures([]string{featureA, featureB}).Obj(),
			preFilterReqs:  nil,
			expectedStatus: nil,
		},
		{
			name:           "Node matches requirements",
			pluginEnabled:  true,
			pod:            st.MakePod().Name("test-pod").Obj(),
			node:           st.MakeNode().Name("node-1").DeclaredFeatures([]string{featureA, featureB}).Obj(),
			preFilterReqs:  []string{featureA},
			expectedStatus: fwk.NewStatus(fwk.Success),
		},
		{
			name:           "Node does not match requirements",
			pluginEnabled:  true,
			pod:            st.MakePod().Name("test-pod").Obj(),
			node:           st.MakeNode().Name("node-1").DeclaredFeatures([]string{featureB}).Obj(),
			preFilterReqs:  []string{featureA},
			expectedStatus: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, errReasonUnsatisfiedRequirements),
		},
		{
			name:           "Node with multiple features, pod requires subset",
			pod:            st.MakePod().Name("test-pod").Obj(),
			node:           st.MakeNode().Name("node-multi").DeclaredFeatures([]string{featureA, featureB, featureC}).Obj(),
			preFilterReqs:  []string{featureA, featureC},
			expectedStatus: fwk.NewStatus(fwk.Success),
		},
		{
			name:           "Node has no declared features",
			pluginEnabled:  true,
			pod:            st.MakePod().Name("test-pod").Obj(),
			node:           st.MakeNode().Name("node-1").Obj(),
			preFilterReqs:  []string{featureA},
			expectedStatus: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, errReasonUnsatisfiedRequirements),
		},
		{
			name:           "Node with some but not all required features",
			pluginEnabled:  true,
			pod:            st.MakePod().Name("test-pod").Obj(),
			node:           st.MakeNode().Name("node-1").DeclaredFeatures([]string{featureA}).Obj(),
			preFilterReqs:  []string{featureA, featureB},
			expectedStatus: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, errReasonUnsatisfiedRequirements),
		},
		{
			name:           "Error getting pre-filter state",
			pluginEnabled:  true,
			pod:            st.MakePod().Name("test-pod").Obj(),
			node:           st.MakeNode().Name("node-1").Obj(),
			preFilterReqs:  nil, // This will cause getPreFilterState to fail
			expectedStatus: fwk.AsStatus(fmt.Errorf("error reading %q from cycle-state: %w", preFilterStateKey, fwk.ErrNotFound)),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if !tc.pluginEnabled {
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.36"))
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.NodeDeclaredFeatures, tc.pluginEnabled)
			}
			nodeInfo := framework.NewNodeInfo()
			nodeInfo.SetNode(tc.node)

			plugin := &NodeDeclaredFeatures{
				ndfFramework: ndf.DefaultFramework,
				version:      version.MustParseSemantic("1.35.0"),
				enabled:      tc.pluginEnabled,
			}
			cycleState := framework.NewCycleState()
			if tc.preFilterReqs != nil {
				cycleState.Write(preFilterStateKey, &preFilterState{reqs: newFS(tc.preFilterReqs...)})
			}

			status := plugin.Filter(ctx, cycleState, tc.pod, nodeInfo)
			if !status.IsSuccess() {
				if tc.expectedStatus.Code() != status.Code() {
					t.Errorf("unexpected status code: want %d, got %d", tc.expectedStatus.Code(), status.Code())
				}
				if tc.expectedStatus.Message() != status.Message() {
					t.Errorf("unexpected status message: want %q, got %q", tc.expectedStatus.Message(), status.Message())
				}
			} else if diff := cmp.Diff(tc.expectedStatus, status); diff != "" {
				t.Errorf("unexpected status (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestEnqueueExtensionsNodeUpdate(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	targetPodName := "test-pod"

	testCases := []struct {
		name          string
		pluginEnabled bool
		oldNode       *v1.Node
		newNode       *v1.Node
		expectedHint  fwk.QueueingHint
	}{
		{
			name:         "Node Add with feature",
			oldNode:      nil,
			newNode:      st.MakeNode().Name("node-1").DeclaredFeatures([]string{"FeatureA"}).Obj(),
			expectedHint: fwk.Queue,
		},
		{
			name:         "Node Update (Features Added)",
			oldNode:      st.MakeNode().Name("node-1").Obj(),
			newNode:      st.MakeNode().Name("node-1").DeclaredFeatures([]string{"FeatureA"}).Obj(),
			expectedHint: fwk.Queue,
		},
		{
			name:         "Node Update (Features Unchanged)",
			oldNode:      st.MakeNode().Name("node-1").DeclaredFeatures([]string{"FeatureA"}).Obj(),
			newNode:      st.MakeNode().Name("node-1").DeclaredFeatures([]string{"FeatureA"}).Obj(),
			expectedHint: fwk.QueueSkip,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ndfFramework := ndf.New([]ndf.Feature{})
			plugin := &NodeDeclaredFeatures{
				ndfFramework: ndfFramework,
				version:      version.MustParseSemantic("1.35.0"),
				enabled:      true,
			}

			hint, err := plugin.isSchedulableAfterNodeChange(logger, st.MakePod().Name(targetPodName).Obj(), tc.oldNode, tc.newNode)
			if err != nil {
				t.Fatalf("isSchedulableAfterNodeChange returned unexpected error: %v", err)
			}
			if tc.expectedHint != hint {
				t.Errorf("unexpected hint: want %v, got %v", tc.expectedHint, hint)
			}
		})
	}
}

func TestIsSchedulableAfterTargetPodUpdate(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)

	targetPodName := "test-pod"
	targetPodUID := "123"

	testCases := []struct {
		name              string
		oldPod            *v1.Pod
		newPod            *v1.Pod
		setupMock         func(m *ndftesting.MockFeature)
		nodeFeatures      []ndf.Feature
		expectedHint      fwk.QueueingHint
		componenetVersion *version.Version
		expectedErr       string
	}{
		{
			name:              "Pod Update adds requirement",
			oldPod:            st.MakePod().Name(targetPodName).UID(targetPodUID).Obj(),
			newPod:            st.MakePod().Name(targetPodName).UID(targetPodUID).Label("foo", "bar").Obj(),
			componenetVersion: version.MustParseSemantic("1.35.0"),
			setupMock: func(m *ndftesting.MockFeature) {
				i := 0
				m.SetInferForScheduling(func(podInfo *ndf.PodInfo) bool {
					switch i {
					case 0:
						i++
						return false
					case 1:
						i++
						return true
					default:
						panic("unexpected calls to SetInferForScheduling")
					}
				})
				m.SetName("TestFeature")
				m.SetMaxVersion(nil)
			},
			expectedHint: fwk.Queue,
		},
		{
			name:              "Pod Update removes requirement",
			oldPod:            st.MakePod().Name(targetPodName).UID(targetPodUID).Label("foo", "bar").Obj(),
			newPod:            st.MakePod().Name(targetPodName).UID(targetPodUID).Obj(),
			componenetVersion: version.MustParseSemantic("1.35.0"),
			setupMock: func(m *ndftesting.MockFeature) {
				i := 0
				m.SetInferForScheduling(func(podInfo *ndf.PodInfo) bool {
					switch i {
					case 0:
						i++
						return true
					case 1:
						i++
						return false
					default:
						panic("unexpected calls to SetInferForScheduling")
					}
				})
				m.SetName("TestFeature")
				m.SetMaxVersion(nil)
			},
			expectedHint: fwk.Queue,
		},
		{
			name:              "Pod Update with no change in requirements",
			oldPod:            st.MakePod().Name(targetPodName).UID(targetPodUID).Obj(),
			newPod:            st.MakePod().Name(targetPodName).UID(targetPodUID).Obj(),
			componenetVersion: version.MustParseSemantic("1.35.0"),
			setupMock: func(m *ndftesting.MockFeature) {
				m.SetInferForScheduling(func(podInfo *ndf.PodInfo) bool { return false })
				m.SetName("TestFeature")
				m.SetMaxVersion(nil)
			},
			expectedHint: fwk.QueueSkip,
		},
		{
			name:              "Infer returns error",
			oldPod:            st.MakePod().Name(targetPodName).UID(targetPodUID).Obj(),
			newPod:            st.MakePod().Name(targetPodName).UID(targetPodUID).Label("foo", "bar").Obj(),
			componenetVersion: nil,
			setupMock: func(m *ndftesting.MockFeature) {
				m.SetInferForScheduling(func(podInfo *ndf.PodInfo) bool { return true })
				m.SetName("TestFeature")
				m.SetMaxVersion(nil)
			},
			expectedHint: fwk.Queue, // Queued again in case of error
			expectedErr:  "target version cannot be nil",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockF := ndftesting.NewMockFeature(t)
			tc.setupMock(mockF)

			ndfFramework := ndf.New([]ndf.Feature{mockF})
			plugin := &NodeDeclaredFeatures{
				ndfFramework: ndfFramework,
				version:      tc.componenetVersion,
				enabled:      true,
			}
			hint, err := plugin.isSchedulableAfterTargetPodUpdate(logger, st.MakePod().Name(targetPodName).UID(targetPodUID).Obj(), tc.oldPod, tc.newPod)
			if tc.expectedErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.expectedErr)
				} else if !strings.Contains(err.Error(), tc.expectedErr) {
					t.Fatalf("expected error containing %q, got %v", tc.expectedErr, err)
				}
			} else if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			if tc.expectedHint != hint {
				t.Errorf("unexpected hint: want %v, got %v", tc.expectedHint, hint)
			}
		})
	}
}

func TestEventsToRegister(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	tests := []struct {
		name           string
		enabled        bool
		expectedLength int
	}{
		{
			name:           "plugin disabled",
			enabled:        false,
			expectedLength: 0,
		},
		{
			name:           "plugin enabled",
			enabled:        true,
			expectedLength: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := &NodeDeclaredFeatures{
				enabled: tt.enabled,
			}
			events, err := plugin.EventsToRegister(ctx)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(events) != tt.expectedLength {
				t.Errorf("expected %d events, got %d", tt.expectedLength, len(events))
			}
		})
	}
}

func TestNodeDeclaredFeatures_DeferredResizeSkipped(t *testing.T) {
	ctx := context.Background()
	pod := st.MakePod().Name("p").UID("p").Condition(v1.PodResizePending, v1.ConditionTrue, v1.PodReasonDeferred).Obj()
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(st.MakeNode().Name("node1").Obj())

	pl := &NodeDeclaredFeatures{enabled: true, enableInPlacePodVerticalScalingSchedulerPreemption: true}

	if preRes, preStatus := pl.PreFilter(ctx, nil, pod, nil); preStatus.Code() != fwk.Skip || preRes != nil {
		t.Errorf("PreFilter: got (res: %v, status: %v), want (nil, Skip)", preRes, preStatus.Code())
	}

	if filterStatus := pl.Filter(ctx, nil, pod, nodeInfo); filterStatus.Code() != fwk.Success {
		t.Errorf("Filter: got status %v, want Success (nil)", filterStatus.Code())
	}
}

type dryRunPreemptionCandidate struct {
	name    string
	victims *extenderv1.Victims
}

func TestDryRunPreemptionCandidateFiltering(t *testing.T) {
	metrics.Register()
	var (
		midPriority  = int32(500)
		highPriority = int32(1000)

		largeRes = map[v1.ResourceName]string{
			v1.ResourceCPU:    "1000m",
			v1.ResourceMemory: "1000Mi",
		}
		nodeRes = map[v1.ResourceName]string{
			v1.ResourceCPU:    "1000m",
			v1.ResourceMemory: "1000Mi",
			v1.ResourcePods:   "10",
		}
	)

	mockAVX512 := ndftesting.NewMockFeature(t)
	mockAVX512.SetName("intel.com/avx512")
	mockAVX512.SetInferForScheduling(func(podInfo *ndf.PodInfo) bool {
		return len(podInfo.Spec.Containers) > 0 && podInfo.Spec.Containers[0].Name == "container-req-avx512"
	})
	mockAVX512.SetMaxVersion(nil)
	mockAVX512.SetInferForUpdate(func(_, _ *ndf.PodInfo) bool { return false })
	mockAVX512.SetDiscover(func(*ndf.NodeConfiguration) bool { return false })

	mockSVE2 := ndftesting.NewMockFeature(t)
	mockSVE2.SetName("arm.com/sve2")
	mockSVE2.SetInferForScheduling(func(podInfo *ndf.PodInfo) bool {
		return len(podInfo.Spec.Containers) > 0 && podInfo.Spec.Containers[0].Name == "container-req-sve2"
	})
	mockSVE2.SetMaxVersion(nil)
	mockSVE2.SetInferForUpdate(func(_, _ *ndf.PodInfo) bool { return false })
	mockSVE2.SetDiscover(func(*ndf.NodeConfiguration) bool { return false })

	mockUnsatFeature := ndftesting.NewMockFeature(t)
	mockUnsatFeature.SetName("other.com/unsatisfied")
	mockUnsatFeature.SetInferForScheduling(func(podInfo *ndf.PodInfo) bool {
		return len(podInfo.Spec.Containers) > 0 && podInfo.Spec.Containers[0].Name == "container-req-unsat"
	})
	mockUnsatFeature.SetMaxVersion(nil)
	mockUnsatFeature.SetInferForUpdate(func(_, _ *ndf.PodInfo) bool { return false })
	mockUnsatFeature.SetDiscover(func(*ndf.NodeConfiguration) bool { return false })

	ndfFramework := ndf.New([]ndf.Feature{mockAVX512, mockSVE2, mockUnsatFeature})
	ndftesting.SetFrameworkDuringTest(t, *ndfFramework)

	nodes := []*v1.Node{
		st.MakeNode().Name("node-avx512").Capacity(nodeRes).DeclaredFeatures([]string{"intel.com/avx512"}).Obj(),
		st.MakeNode().Name("node-sve2").Capacity(nodeRes).DeclaredFeatures([]string{"arm.com/sve2"}).Obj(),
		st.MakeNode().Name("node-no-feature").Capacity(nodeRes).Obj(),
	}

	initPods := []*v1.Pod{
		st.MakePod().Name("victim-avx512").UID("victim-avx512").Node("node-avx512").Priority(midPriority).Req(largeRes).Obj(),
		st.MakePod().Name("victim-sve2").UID("victim-sve2").Node("node-sve2").Priority(midPriority).Req(largeRes).Obj(),
		st.MakePod().Name("victim-no-feature").UID("victim-no-feature").Node("node-no-feature").Priority(midPriority).Req(largeRes).Obj(),
	}

	tests := []struct {
		name      string
		fts       feature.Features
		preemptor *v1.Pod
		expected  []dryRunPreemptionCandidate
	}{
		{
			name: "preemptor requires AVX-512, only node-avx512 is preemption candidate",
			fts:  feature.Features{EnableNodeDeclaredFeatures: true},
			preemptor: st.MakePod().Name("preemptor-avx512").UID("preemptor-avx512").Priority(highPriority).
				Containers([]v1.Container{st.MakeContainer().Name("container-req-avx512").Image(imageutils.GetPauseImageName()).ResourceRequests(largeRes).Obj()}).Obj(),
			expected: []dryRunPreemptionCandidate{
				{
					name: "node-avx512",
					victims: &extenderv1.Victims{
						Pods: []*v1.Pod{st.MakePod().Name("victim-avx512").UID("victim-avx512").Node("node-avx512").Priority(midPriority).Req(largeRes).Obj()},
					},
				},
			},
		},
		{
			name: "preemptor requires SVE2, only node-sve2 is preemption candidate",
			fts:  feature.Features{EnableNodeDeclaredFeatures: true},
			preemptor: st.MakePod().Name("preemptor-sve2").UID("preemptor-sve2").Priority(highPriority).
				Containers([]v1.Container{st.MakeContainer().Name("container-req-sve2").Image(imageutils.GetPauseImageName()).ResourceRequests(largeRes).Obj()}).Obj(),
			expected: []dryRunPreemptionCandidate{
				{
					name: "node-sve2",
					victims: &extenderv1.Victims{
						Pods: []*v1.Pod{st.MakePod().Name("victim-sve2").UID("victim-sve2").Node("node-sve2").Priority(midPriority).Req(largeRes).Obj()},
					},
				},
			},
		},
		{
			name: "preemptor requires unsatisfied feature, no candidates returned",
			fts:  feature.Features{EnableNodeDeclaredFeatures: true},
			preemptor: st.MakePod().Name("preemptor-unsat").UID("preemptor-unsat").Priority(highPriority).
				Containers([]v1.Container{st.MakeContainer().Name("container-req-unsat").Image(imageutils.GetPauseImageName()).ResourceRequests(largeRes).Obj()}).Obj(),
			expected: nil,
		},
		{
			name: "preemptor has no feature requirements, all nodes are preemption candidates",
			fts:  feature.Features{EnableNodeDeclaredFeatures: true},
			preemptor: st.MakePod().Name("preemptor-generic").UID("preemptor-generic").Priority(highPriority).
				Containers([]v1.Container{st.MakeContainer().Name("generic-container").Image(imageutils.GetPauseImageName()).ResourceRequests(largeRes).Obj()}).Obj(),
			expected: []dryRunPreemptionCandidate{
				{
					name: "node-avx512",
					victims: &extenderv1.Victims{
						Pods: []*v1.Pod{st.MakePod().Name("victim-avx512").UID("victim-avx512").Node("node-avx512").Priority(midPriority).Req(largeRes).Obj()},
					},
				},
				{
					name: "node-no-feature",
					victims: &extenderv1.Victims{
						Pods: []*v1.Pod{st.MakePod().Name("victim-no-feature").UID("victim-no-feature").Node("node-no-feature").Priority(midPriority).Req(largeRes).Obj()},
					},
				},
				{
					name: "node-sve2",
					victims: &extenderv1.Victims{
						Pods: []*v1.Pod{st.MakePod().Name("victim-sve2").UID("victim-sve2").Node("node-sve2").Priority(midPriority).Req(largeRes).Obj()},
					},
				},
			},
		},
		{
			name: "feature gate disabled, feature requirements are ignored and all nodes are candidates",
			fts:  feature.Features{EnableNodeDeclaredFeatures: false},
			preemptor: st.MakePod().Name("preemptor-avx512").UID("preemptor-avx512").Priority(highPriority).
				Containers([]v1.Container{st.MakeContainer().Name("container-req-avx512").Image(imageutils.GetPauseImageName()).ResourceRequests(largeRes).Obj()}).Obj(),
			expected: []dryRunPreemptionCandidate{
				{
					name: "node-avx512",
					victims: &extenderv1.Victims{
						Pods: []*v1.Pod{st.MakePod().Name("victim-avx512").UID("victim-avx512").Node("node-avx512").Priority(midPriority).Req(largeRes).Obj()},
					},
				},
				{
					name: "node-no-feature",
					victims: &extenderv1.Victims{
						Pods: []*v1.Pod{st.MakePod().Name("victim-no-feature").UID("victim-no-feature").Node("node-no-feature").Priority(midPriority).Req(largeRes).Obj()},
					},
				},
				{
					name: "node-sve2",
					victims: &extenderv1.Victims{
						Pods: []*v1.Pod{st.MakePod().Name("victim-sve2").UID("victim-sve2").Node("node-sve2").Priority(midPriority).Req(largeRes).Obj()},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.fts.EnableNodeDeclaredFeatures {
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.36"))
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.NodeDeclaredFeatures, false)
			} else {
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.NodeDeclaredFeatures, true)
			}

			logger, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			registeredPlugins := []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
				tf.RegisterPluginAsExtensions(noderesources.Name, frameworkruntime.FactoryAdapter(tt.fts, noderesources.NewFit), "Filter", "PreFilter"),
				tf.RegisterPluginAsExtensions(Name, frameworkruntime.FactoryAdapter(tt.fts, New), "Filter", "PreFilter"),
			}

			var objs []runtime.Object
			objs = append(objs, tt.preemptor)
			for _, p := range initPods {
				objs = append(objs, p)
			}
			for _, n := range nodes {
				objs = append(objs, n)
			}

			informerFactory := informers.NewSharedInformerFactory(clientsetfake.NewClientset(objs...), 0)
			snapshot := internalcache.NewSnapshot(initPods, nodes)
			schedFwk, err := tf.NewFramework(
				ctx,
				registeredPlugins,
				"",
				frameworkruntime.WithPodNominator(internalqueue.NewSchedulingQueue(nil, informerFactory)),
				frameworkruntime.WithInformerFactory(informerFactory),
				frameworkruntime.WithParallelism(parallelize.DefaultParallelism),
				frameworkruntime.WithSnapshotSharedLister(snapshot),
				frameworkruntime.WithMutableSnapshotLister(snapshot),
				frameworkruntime.WithLogger(logger),
				frameworkruntime.WithPreemptionManager(func(fh fwk.Handle) fwk.PreemptionManager {
					return preemption.NewPreemptionManager(fh, tt.fts)
				}),
			)
			if err != nil {
				t.Fatalf("Failed to create framework: %v", err)
			}

			informerFactory.Start(ctx.Done())
			informerFactory.WaitForCacheSync(ctx.Done())

			dpArgs := &config.DefaultPreemptionArgs{MinCandidateNodesPercentage: 100, MinCandidateNodesAbsolute: 100}
			dpPlugin, err := defaultpreemption.New(ctx, dpArgs, schedFwk, tt.fts)
			if err != nil {
				t.Fatalf("Failed to create DefaultPreemption plugin: %v", err)
			}

			nodeInfos, err := snapshot.NodeInfos().List()
			if err != nil {
				t.Fatalf("Failed to list nodeInfos: %v", err)
			}

			state := framework.NewCycleState()
			if _, status, _ := schedFwk.RunPreFilterPlugins(ctx, state, tt.preemptor); !status.IsSuccess() {
				t.Fatalf("Unexpected PreFilter status: %v", status)
			}

			got, _, err := dpPlugin.Evaluator.DryRunPreemption(ctx, state, tt.preemptor, nodeInfos, nil, 0, int32(len(nodeInfos)))
			if err != nil {
				t.Fatalf("DryRunPreemption failed: %v", err)
			}

			for i := range got {
				victims := got[i].Victims().Pods
				sort.Slice(victims, func(a, b int) bool {
					return victims[a].Name < victims[b].Name
				})
			}
			sort.Slice(got, func(a, b int) bool {
				return got[a].Name() < got[b].Name()
			})

			var candidates []dryRunPreemptionCandidate
			for _, c := range got {
				candidates = append(candidates, dryRunPreemptionCandidate{
					name:    c.Name(),
					victims: c.Victims(),
				})
			}

			if diff := cmp.Diff(tt.expected, candidates, cmp.AllowUnexported(dryRunPreemptionCandidate{})); diff != "" {
				t.Errorf("Unexpected preemption candidates (-want, +got):\n%s", diff)
			}
		})
	}
}
