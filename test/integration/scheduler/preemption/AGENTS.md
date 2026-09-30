# AGENTS.md: Developer & Agent Guide for `test/integration/scheduler/preemption`

This document provides AI agents and human contributors with an architectural guide, harness details, execution lifecycles, and verification practices for the Kubernetes scheduler preemption integration test suite located under `test/integration/scheduler/preemption`.

---

## 1. High-Level Overview & Suite Purpose

The preemption integration test suite executes end-to-end scheduler preemption flows against a real in-memory `kube-apiserver` backed by `etcd` and a live `kube-scheduler` instance. It verifies candidate evaluation, victim selection, reprieve mechanics, PDB compliance, storage volume constraints, asynchronous actuation, in-place vertical scaling, and recent KEP enhancements (pod-level resources, node declared features, opportunistic batching signatures, and structured DRA topology).

---

## 2. Directory Layout & Test Suite Organization

```
test/integration/scheduler/preemption/
├── AGENTS.md                            # This developer and agent guide
├── main_test.go                         # Global test setup and etcd verification
├── preemption_test.go                   # Standard preemption flows, priority tiers, and basic candidate selection
├── extender_preemption_test.go          # HTTP scheduling extender integration with preemption
├── async_preemption_failures_test.go    # Asynchronous preemption error handling, timeout, and pre-bind conflict tests
├── deferred_resize_preemption_test.go   # In-place resource scaling (KEP-1287) and resize preemption interactions
├── asyncframework/                      # Asynchronous preemption execution lifecycle tests
├── nominatednodename/                   # NominatedNodeName propagation, clearing, and scheduler queue nomination
├── podgroup/                            # Gang and workload preemption across multi-node pod groups (KEP-4832)
└── misc/
    └── miscpreemption_test.go           # Advanced and edge-case preemption suites (PDBs, RWOP volumes, KEP-2837, KEP-4818, KEP-6072)
```

---

## 3. Test Harness Architecture & Execution Lifecycle

### 3.1. Embedded Test Context (`initTest`)
Each test function initializes an isolated test environment via `initTest(t, "test-prefix")`:
1. **Control Plane Spin-Up**: Initializes an in-process `kube-apiserver` backed by a local `etcd` server.
2. **Scheduler Launch**: Starts a standard `kube-scheduler` configured with DefaultPreemption and all in-tree plugins.
3. **Namespace Isolation**: Generates a dedicated namespace using the given prefix (note: namespace names must remain under 63 characters including the generated UUID suffix).
4. **Context Management**: Creates a root test context (`testCtx.Ctx`) that automatically tears down all background controllers upon test completion.

### 3.2. Common Test Helpers & Patterns
- `createNode(cs, node)` / `st.MakeNode()`: Registers nodes with custom allocatable capacities and status features.
- `runPausePod(cs, pod)` / `initPausePod(cfg)`: Runs a pause container pod to simulate existing cluster workloads or victims.
- `simulateVictimDeletion(ctx, cs, ns, podName)`: Monitors for the scheduler setting `DeletionTimestamp` or patches zero grace period deletion to simulate victim termination by the kubelet.
- `waitForPodToScheduleWithTimeout(ctx, cs, pod, timeout)`: Blocks until `pod.Spec.NodeName` is populated by the scheduler.
- `waitForPodUnschedulable(ctx, cs, pod)`: Blocks until the pod is placed into the unschedulable queue with failure events recorded.
- `featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, ...)`: Temporarily enables or disables specific feature gates for the duration of a test.

---

## 4. Key Preemption Integration Suites

### 4.1. Pod-Level Resource Preemption (`TestPodLevelResourcePreemption` - KEP-2837)
- **Preemptor with Pod-Level Resources & Overhead**: Verifies that when a high-priority pod declares pod-level requests (`spec.resources.requests`) and runtime overhead (`spec.overhead` via `RuntimeClass`), the candidate evaluation accounts for the combined request and triggers eviction of lower-priority victims.
- **Victim with Pod-Level Resources**: Verifies that evicting a victim that specifies pod-level requests frees up the full pod-level resource amount, allowing an incoming preemptor requiring those resources to schedule.

### 4.2. Node Declared Features Preemption (`TestNodeDeclaredFeaturesPreemption` - KEP-4818)
- **Unresolvable Status Exclusion**: Tests that when a preemptor requires a declared node feature (such as `UserNamespacesHostNetworkSupport` inferred from `HostNetwork: true` and `HostUsers: false`), nodes lacking that feature return `UnschedulableAndUnresolvable`.
- **Zero Disruption Invariant**: Ensures lower-priority victim pods running on incompatible nodes are never marked for deletion or disrupted during candidate evaluation.

### 4.3. Structured DRA & NUMA Topology Preemption (`TestDRAStructuredNUMATopologyPreemption` - KEP-6072)
- **DeviceClass & CEL Match**: Creates `DeviceClass` with CEL selectors and `ResourceSlice` instances bound to specific nodes.
- **Topology Alignment**: Ensures preemption only evicts compute victims on nodes that also have matching structured DRA device slices available.

### 4.4. Storage & Access Mode Preemption (`TestVolumeRestrictionsPreemption`)
- **ReadWriteOncePod (RWOP)**: Validates that preemption obeys single-node attachment constraints for RWOP volumes, preventing conflicting multi-node evictions.

---

## 5. Prerequisites & Test Execution

### 5.1. Toolchain & Dependencies
- **Go Toolchain**: Go 1.27+ with `GOTOOLCHAIN=auto`.
- **`etcd` Binary**: Required in system `PATH` (typically `/usr/local/bin/etcd`). Integration tests fail to bootstrap the API server if `etcd` is missing.

### 5.2. Running Tests
```bash
# Run all preemption integration tests
cd /home/debian/work && GOTOOLCHAIN=auto go test -v ./test/integration/scheduler/preemption/...

# Run the misc preemption suite (KEP-2837, KEP-4818, KEP-6072)
cd /home/debian/work && GOTOOLCHAIN=auto go test -v ./test/integration/scheduler/preemption/misc/

# Run a specific integration test
cd /home/debian/work && GOTOOLCHAIN=auto go test -v -run TestNodeDeclaredFeaturesPreemption ./test/integration/scheduler/preemption/misc/
```
