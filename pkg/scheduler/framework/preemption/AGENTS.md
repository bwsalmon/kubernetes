# AGENTS.md: Preemption Framework Architecture & Implementation Guide

This guide provides an in-depth architectural overview, interface specifications, execution lifecycles, candidate finding and victim selection algorithms, reprieve mechanics, PDB partitioning ($V_{respect}$ vs $V_{violate}$), synchronization semantics, and developer invariants for the Kubernetes scheduler preemption subsystem located under `pkg/scheduler/framework/preemption`.

---

## 1. High-Level Overview & KEP Foundations

The preemption subsystem (`pkg/scheduler/framework/preemption`) provides the foundational algorithms, candidate evaluation engines, and actuation pipelines for evicting lower-priority pods when an incoming high-priority pod (or gang-scheduled pod group) cannot find sufficient resources on any cluster node during the filtering phase.

### Core KEP Foundations:
1. **Core Pod Priority and Preemption (KEP-562)**:
   - Priority-driven preemption ensuring high-priority workloads can schedule without deadlock or starvation.
   - `PreemptionPolicy` support (`PreemptLowerPriority` vs `PreemptNever`).
   - Nominated node tracking (`status.nominatedNodeName`) and nomination shadowing prevention via two-pass conservative filtering.
   - Minimal victim eviction via speculative removal and importance-based reprieval.
2. **Respect PodDisruptionBudget in Preemption (KEP-3838)**:
   - Partitions candidate victims into PDB-respecting ($V_{respect}$) and PDB-violating ($V_{violate}$) sets.
   - Two-pass reprieve algorithm: reprieves $V_{violate}$ pods before $V_{respect}$ pods to minimize budget disruption.
   - Zero-disruption optimization: early short-circuiting in dry-run preemption when non-violating candidates satisfy search quotas.
   - Tier-1 scoring preference in candidate selection for nodes with minimum PDB violations.
3. **Asynchronous Preemption (KEP-4832)**:
   - Non-blocking actuation in background goroutines with `PreEnqueue` gating (`IsPodRunningPreemption`).
   - In-memory eviction for waiting (`Permit`) and pre-binding (`PreBind`) pods.

---

## 2. Directory Architecture & Subsystem Layout

```
pkg/scheduler/framework/preemption/
├── types.go                   # Core types (Victim, DomainVictim, candidate, candidateList, ExecutorPreemptor)
├── types_test.go              # Unit tests for victim sorting, domain victim creation, and candidate lists
├── util.go                    # Helper functions (MoreImportantVictim, PDB filters, hierarchy traversal, priorities)
├── util_test.go               # Unit tests for PDB filtering, victim importance ranking, and tree traversal
├── manager.go                 # defaultPreemptionManager and DomainVictim ordering
├── manager_test.go            # Unit tests for PreemptionManager victim generation and preemption policies
├── executor.go                # PreemptionExecutor (sync & async eviction, waiting pod cancellation, status patch)
├── executor_test.go           # Unit tests for Executor sync/async execution, metrics, and nominations
├── preemption.go              # Evaluator engine, findCandidates, DryRunPreemption, SelectCandidate, node scoring
├── preemption_test.go         # Unit tests for Evaluator lifecycle, extenders, dry-run, and candidate selection
├── podgrouppreemption.go      # PodGroupEvaluator for gang/workload preemption across the cluster snapshot
├── podgrouppreemption_test.go # Comprehensive unit tests for PodGroupEvaluator and reprieve loops
└── AGENTS.md                  # This agent guide
```

---

## 3. Core Types & Domain Models

### 3.1. `Victim` and `DomainVictim` (`types.go`)

```go
type Victim interface {
    Priority() int32
    Pods() []fwk.PodInfo
    EarliestStartTime() *metav1.Time
    Type() fwk.EntityKeyType
}
```

- **`Victim`**: Represents an atomic preemption unit. For standalone pods, it wraps a single `PodInfo`. For gang-scheduled workloads (PodGroups with `DisruptionModeAll`), it bundles all scheduled member pods across the cluster so they are evaluated and evicted as an indivisible unit.
- **`DomainVictim`**: Enriches a `Victim` with the `affectedNodes map[string]fwk.NodeInfo` from the snapshot. This allows the evaluator to assess the multi-node blast radius of preempting a distributed pod group during single-node or cluster-wide dry runs.
- **`ViolatingVictim[T Victim]`**: Wraps a `Victim` with `ViolateCount int`, indicating how many of its constituent pods violate an active `PodDisruptionBudget` upon eviction.

### 3.2. `PreemptionCandidate` and `candidateList` (`types.go`)

- **`candidate`**: Implements `fwk.PreemptionCandidate`. Bundles the target node name, the `extenderv1.Victims` (slice of victim pods and PDB violation count), and the count of disrupted pod groups (`numPodGroupDisruptions`).
- **`candidateList`**: Thread-safe bounded container storing up to `candidatesNum` candidates discovered during parallel dry-run passes. Provides `add(*candidate)`, `size()`, and `get()`.

---

## 4. Single-Pod Preemption Lifecycle & Algorithm

When all nodes fail `FilterPlugins` during a scheduling cycle, the scheduler enters the `PostFilter` extension point. The `DefaultPreemption` plugin invokes `Evaluator.Preempt()`, executing the following workflow:

```
[ PostFilter Triggered (0 Feasible Nodes) ]
                     │
                     ▼
[ Step 0: Fetch Latest Preemptor Pod ]
   └── Query PodLister to prevent operating on stale pod spec/status
                     │
                     ▼
[ Step 1: Preemption Eligibility Check ]
   ├── Pod.Spec.PreemptionPolicy == PreemptNever -> Abort (Unschedulable)
   └── Nominated node has terminating pods -> Wait (Abort cycle)
                     │
                     ▼
[ Step 2: Concurrently Find Candidates (findCandidates / DryRunPreemption) ]
   ├── Collect Unschedulable nodes from NodeToStatusReader
   ├── Compute random offset & candidate limit (MinCandidateNodesPercentage/Absolute)
   └── Run checkNode() in parallel via Parallelizer.Until:
         ├── Discover DomainVictims on candidate node (GetVictimsOnNode)
         ├── Run SelectVictimsOnNode on cloned CycleState & snapshot copy
         └── Collect non-violating & violating candidates into candidateList
                     │
                     ▼
[ Step 3: Extender Verification (callExtenders) ]
   └── Invoke extender.ProcessPreemption() for preempt-capable HTTP extenders
                     │
                     ▼
[ Step 4: Pick Best Candidate (SelectCandidate / pickOneNodeForPreemption) ]
   └── Execute 6-stage tie-breaking cascade to pick optimal node
                     │
                     ▼
[ Step 5: Actuate Preemption (Executor.ActuatePodPreemption) ]
   ├── Synchronous Path: Delete pods in parallel, clear lower-priority nominations
   └── Asynchronous Path (KEP-4832): Dispatch background goroutine, set PreEnqueue gate
                     │
                     ▼
[ Return PostFilterResult with NominatedNodeName ]
```

### Detailed Algorithm Steps:

#### Step 0: Retrieve Latest Preemptor
Fetches the updated `*v1.Pod` from `ev.PodLister` to avoid operating on stale annotations or scheduling gates.

#### Step 1: Preemption Eligibility (`PodEligibleToPreemptOthers`)
Evaluates whether the preemptor should be permitted to preempt:
1. **`PreemptionPolicy`**: If `Spec.PreemptionPolicy == PreemptNever`, preemption is immediately aborted with status `Unschedulable`.
2. **Victims in Flight**: If the preemptor already has a `Status.NominatedNodeName`, the evaluator checks whether any terminating pods on that node were marked with scheduler preemption (`PodTerminatingByPreemption`). If terminating pods exist, the scheduler returns `Unschedulable` to allow them to complete their graceful termination period without prematurely evicting additional workloads. (Exception: if the node was marked `UnschedulableAndUnresolvable` by filters, re-preemption is allowed).

#### Step 2: Parallel Dry Run (`findCandidates` / `DryRunPreemption`)
1. Filters cluster nodes using `NodeToStatusReader.NodesForStatusCode(..., fwk.Unschedulable)`. Nodes marked `UnschedulableAndUnresolvable` (e.g. node selector / architecture mismatch) are completely bypassed.
2. Calculates candidate search window:
   - `offset = rand(len(potentialNodes))`
   - `candidatesNum = max(MinCandidateNodesAbsolute, (len(potentialNodes) * MinCandidateNodesPercentage) / 100)`
3. Executes `checkNode` across worker goroutines via `fh.Parallelizer().Until`:
   - Evaluates node at `(offset + i) % len(potentialNodes)`.
   - Calls `GetVictimsOnNode(nodeInfo)` to build `[]*DomainVictim`.
   - Clones `CycleState` via `state.Clone()` and snapshots `nodeInfo.Snapshot()`.
   - Invokes `SelectVictimsOnNode`.
   - Stores candidate in `nonViolatingCandidates` (0 PDB violations) or `violatingCandidates`.
   - **Zero-Disruption Short-Circuiting**: Once `nonViolatingCandidates.size() > 0` and `nonViolatingCandidates.size() + violatingCandidates.size() >= candidatesNum`, context is canceled early (`cancel()`) to avoid unneeded dry-run passes.

#### Step 3: Scheduling Extender Filtering (`callExtenders`)
For clusters with HTTP scheduling extenders:
- Converts candidates to `map[string]*extenderv1.Victims`.
- Passes map to `extender.ProcessPreemption()`.
- Extenders can drop nodes or add/remove victim pods. Placeholder nodes with empty victim lists are preserved for downstream extenders.

#### Step 4: Candidate Selection Cascade (`pickOneNodeForPreemption`)
When multiple candidate nodes pass dry-run preemption, `pickOneNodeForPreemption` executes a deterministic 6-tier scoring cascade:

| Tier | Evaluation Metric | Preference Rule | Rationale |
|---|---|---|---|
| **1** | `minNumPDBViolatingScoreFunc` | Minimum PDB violations | Preserves application availability budgets (KEP-3838). |
| **2** | `minHighestPriorityScoreFunc` | Minimum highest victim priority | Avoids preempting higher-tier workloads (KEP-562). |
| **3** | `minSumPrioritiesScoreFunc` | Minimum sum of victim priorities | Minimizes total priority displacement (normalized by `+ MaxInt32 + 1`). |
| **4** | `minNumPodsScoreFunc` | Minimum count of victim pods | Minimizes churn and restart overhead. |
| **5** | `latestStartTimeScoreFunc` | Latest start time of highest priority victims | Enforces "first-come, first-served" by protecting older, longer-running jobs. |
| **6** | Tie-Breaker | First node in list | Deterministic tie-breaker. |

---

## 5. Victim Selection & Reprieve Algorithm (`SelectVictimsOnNode`)

`SelectVictimsOnNode` finds the minimal subset of lower-priority pods on a node to evict in order to fit the preemptor while respecting PDB constraints.

```
[ Input: All DomainVictims on Target Node ]
                     │
                     ▼
[ 1. Eligibility Filter ]
   └── Discard victims with Priority >= Preemptor or failing IsEligiblePod
                     │
                     ▼
[ 2. Complete Removal Pass ]
   ├── Mutate mainNode.RemovePod()
   ├── Run PreFilterExtensionRemovePod() for main & remote nodes
   └── RunFilterPluginsWithNominatedPods()
         ├── If FAIL -> Node cannot fit preemptor even if ALL victims evicted (ABORT)
         └── If PASS -> Proceed to Reprieve Loop
                     │
                     ▼
[ 3. Sort Potential Victims by Importance ]
   └── Order descending using MoreImportantVictim()
                     │
                     ▼
[ 4. Partition Potential Victims via FilterVictimsWithPDBViolation ]
   ├── $V_{violate}$: Victims whose eviction violates active PDB ($pdbsAllowed < 0$)
   └── $V_{respect}$: Victims whose eviction respects PDB ($pdbsAllowed \ge 0$)
                     │
                     ▼
[ 5. Phase A: Reprieve PDB-Violating Victims ($V_{violate}$) ]
   └── Iterate descending: Add victim back -> RunFilterPluginsWithNominatedPods()
         ├── Fits? -> Keep in node (Reprieved!)
         └── Fails? -> Remove victim again & mark as Definite Victim (increment numViolatingVictims)
                     │
                     ▼
[ 6. Phase B: Reprieve Non-Violating Victims ($V_{respect}$) ]
   └── Iterate descending: Add victim back -> RunFilterPluginsWithNominatedPods()
         ├── Fits? -> Keep in node (Reprieved!)
         └── Fails? -> Remove victim again & mark as Definite Victim
                     │
                     ▼
[ 7. Return Final Minimal Victim List & PDB Violation Count ]
```

### 5.1. PDB Budget Accounting Formulation
Given a set of active PDBs $\mathcal{P} = \{pdb_1, \dots, pdb_k\}$ with available budgets $b_i = pdb_i.Status.DisruptionsAllowed$:
1. For each victim pod $p$ matching $pdb_i$ (where $p.Name \notin pdb_i.Status.DisruptedPods$):
   $$b_i \leftarrow b_i - 1$$
2. If any matching PDB reaches $b_i < 0$, pod $p$ is marked as violating.
3. For gang `Victim` objects: if **any member pod** violates a PDB, the **entire Victim** is classified into $V_{violate}$.

### 5.2. Victim Importance Ranking (`MoreImportantVictim`):
When comparing two preemption units $v_1$ and $v_2$:
1. **Priority**: Higher priority is more important ($P(v_1) > P(v_2)$).
2. **Workload Type**: `CompositePodGroup` (rank 3) > `PodGroup` (rank 2) > `Pod` (rank 1).
3. **Runtime / Start Time (Single Pods)**: Pod with earlier start time (longer runtime) is more important ("first-come, first-served").
4. **Group Size (PodGroups)**: Larger group size is more important (avoids high rescheduling cost of large jobs).
5. **Group Start Time**: Group with older earliest start time is more important.
6. **UID Determinism**: `podIdentityLess` (string comparison of representative pod's UID: `v1.Pods()[0].UID < v2.Pods()[0].UID`) guarantees stable, reproducible ordering.

---

## 6. Nomination Shadowing & Two-Pass Conservative Filtering

### 6.1. The Nomination Mechanism
When a pod preempts victims on a node, it receives `Status.NominatedNodeName = node.Name`. This reserves the node in future scheduling cycles until the victims finish terminating.

### 6.2. Two-Pass Filter Verification (`RunFilterPluginsWithNominatedPods`)
To prevent nomination shadowing and race conditions:
1. **Pass 1 (With Nominated Pods)**: Nominated pods with priority $\ge$ preemptor's priority are added to `NodeInfo` and `CycleState` via `addGENominatedPods`. `RunFilterPlugins` is executed. This ensures the preemptor does not take space or port allocations reserved for higher-priority pods.
2. **Pass 2 (Without Nominated Pods)**: If Pass 1 succeeds and nominated pods were present, `RunFilterPlugins` is run a second time *without* the nominated pods. This ensures filters like inter-pod affinity do not pass solely on the assumption that nominated pods (which are not yet bound) are present.
3. **Lower-Priority Nomination Eviction**: When a higher-priority pod preempts on a node, `Executor` clears `Status.NominatedNodeName` on any lower-priority pods previously nominated on that node, preventing deadlocks.

---

## 7. Actuation Subsystem (`Executor`)

`Executor` implements `fwk.PreemptionExecutor` to perform the actual eviction of chosen victim pods.

### 7.1. Pod Preemption Mechanisms (`PreemptPod`)
When evicting a victim pod:
1. **Waiting Pod Cancellation**: If the victim is currently waiting in the `Permit` phase (`fh.GetWaitingPod(victim.UID)`), invokes `waitingPod.Preempt(pluginName, "preempted")`. The victim is rejected in memory and returns to the backoff queue without issuing an API delete call.
2. **Pre-Bind Cancellation**: If the victim is in the asynchronous pre-bind phase (`fh.GetPodInPreBind(victim.UID)`), invokes `podInPreBind.CancelPod(...)`.
3. **API Eviction**:
   - Patches `PodStatus` with condition `DisruptionTarget` (`Status = True`, `Reason = PodReasonPreemptionByScheduler`, `Message = "...: preempting to accommodate a higher priority ..."`).
   - Issues `DeletePod` API call via client-go.
   - Publishes a `Preempted` Normal event to the event recorder.

### 7.2. Synchronous vs. Asynchronous Preemption (KEP-4832)

```
                       [ ActuatePodPreemption ]
                                   │
              ┌────────────────────┴────────────────────┐
              ▼                                         ▼
   [ Synchronous Execution ]                 [ Asynchronous Execution ]
   (EnableAsyncPreemption = false)           (EnableAsyncPreemption = true)
              │                                         │
   ├── Parallelize Until() for all           ├── Create detached context.Background()
   │   c.Victims().Pods                      ├── Insert preemptor.UID into preempting map
   ├── PreemptPod() for each                 ├── Launch background goroutine:
   ├── Clear NominatedNodeName on            │     ├── Parallelize Until() for N-1 victims
   │   lower-priority pods on target         │     ├── Set lastVictimsPendingPreemption
   └── Return status to scheduling cycle     │     ├── Preempt last victim Pod
                                             │     ├── Clear lower-priority nominations
                                             │     ├── Delete from preempting map
                                             │     └── If in-memory/error: fh.Activate()
                                             └── Return nil immediately to scheduling cycle
```

---

## 8. Extender & Policy Utilities (`util.go`)

### 8.1. Pod Disruption Budget Evaluation (`FilterVictimsWithPDBViolation`)
- Inspects active `PodDisruptionBudget` resources across the cluster.
- Decrements `pdb.Status.DisruptionsAllowed` as matching pods are evaluated.
- **Victim Atomicity**: If **any single pod** inside a gang `Victim` violates a PDB on eviction, the **entire Victim** is classified as violating. Partial eviction of gang workloads is strictly forbidden.

### 8.2. Hierarchy Traversal (`traverseHierarchyUp`)
- Uses Go 1.23+ range-over-func iterator (`iter.Seq[*fwk.GenericPodGroup]`) to walk upward from a leaf PodGroup through CompositePodGroups up to `WorkloadMaxTreeDepth` (10).
- Used to compute root group priorities (`getPodPriority`) and locate highest ancestors with `DisruptionModeAll`.

---

## 9. Advanced Resource, Topology & Batched Preemption Interactions (KEPs)

### 9.1 KEP-2837: Pod-Level Resource Specification & Overhead Accounting in Preemption
- **Resource Computation Model**:
  - The `NodeResourcesFit` plugin invokes `computePodResourceRequest(pod, opts)` (backed by `resourcehelper.PodRequests(pod, opts)`).
  - Effective requests are computed as:
    $$\text{EffectiveRequest}[R] = \text{Spec.Overhead}[R] + \max\left(\text{Spec.Resources.Requests}[R], \max_{c \in \text{Containers}}(\text{Container.Requests}[R])\right)$$
  - Under in-place resource scaling (KEP-1287), container `AllocatedResources` and resize status adjustments are layered on top of pod-level definitions.
- **Preemption Evaluation Lifecycle**:
  - During candidate dry-run evaluation (`DryRunPreemption` -> `SelectVictimsOnNode`), when candidate victim pods are speculatively removed via `nodeInfo.RemovePod(victim)`, the node's `Requested` resource footprint is reduced by the victim's exact effective request (accounting for pod-level requests, overheads, and resize states).
  - In `fitsRequest(podRequest)`, the preemptor's aggregate demand (inclusive of `Spec.Resources.Requests` and `Spec.Overhead`) is compared against the node's allocatable capacity minus remaining pods' effective allocations.
  - In the reprieve phase (`reprievePod`), candidate victims are added back sequentially to ensure the minimal subset of victims is evicted without over-evicting capacity.

### 9.2 KEP-4818: Node Declared Features Matching & `UnschedulableAndUnresolvable` Safety
- **Feature Inference & Filtering**:
  - `NodeDeclaredFeatures` plugin matches features required by the pod (inferred via `InferForPodScheduling(pod.Spec)`) against `nodeInfo.GetNodeDeclaredFeatures()`.
  - Inferred features include kernel/container runtime capabilities such as `UserNamespacesHostNetworkSupport` (inferred when `HostNetwork: true` and `HostUsers: false`).
- **Preemption Resolvability Invariant**:
  - If a node lacks a required declared feature, `NodeDeclaredFeatures.Filter` returns `fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ...)`.
  - In `DryRunPreemption`, candidate node discovery runs parallel `checkNode` workers across candidate nodes. Any node returning `UnschedulableAndUnresolvable` is immediately disqualified and skipped before any victim eviction simulation or PDB analysis.
  - **Safety Guarantee**: Unrelated lower-priority workloads on feature-incompatible nodes are never disrupted or evicted, preventing useless thrashing and ensuring preemption exclusively targets feature-compatible nodes.

### 9.3 KEP-5598: Opportunistic Batching, Pod Signature Hashing & Preemption Decision Caching
- **Pod Signature Hashing (`SignPod`)**:
  - The framework computes a canonical JSON hash `PodSignature` for queue items by invoking `SignPlugin.SignPod` across registered plugins (`NodeResourcesFit`, `NodeDeclaredFeatures`, `TaintToleration`, `DynamicResources`, etc.).
  - Signature fragments serialize scheduling criteria (tolerations, affinity, feature requirements, effective resource requests, priority level).
- **Batch Cache & Preemption Boundary**:
  - `OpportunisticBatch.GetNodeHint` accelerates scheduling of consecutive, homogeneous pods by reusing filter and score decisions.
  - When a pod in a batch fails filter and enters `PostFilter` / preemption, `SignPod` invariants guarantee that:
    1. Preemption decisions and nominations are computed specifically for the nominated preemptor.
    2. Dynamic resource claim pods (`len(pod.Spec.ResourceClaims) > 0`) return `Unschedulable` status during `SignPod`, completely bypassing batch caching due to node-specific claim and device states.
    3. Once preemption nominates a node, subsequent pods in the scheduling queue re-evaluate the nominated node taking the pending eviction into account.

### 9.4 KEP-6072: Structured Dynamic Resource Allocation (DRA) & NUMA Topology Preemption
- **Structured Resource Allocation**:
  - The `DynamicResources` plugin interfaces with the DRA API (`resource.k8s.io/v1`), evaluating `DeviceClass`, `ResourceClaim`, and `ResourceSlice` objects.
  - CEL device selectors filter devices by driver attributes, model identifiers, and topology locations (e.g. NUMA nodes, PCIe switches).
- **Candidate Evaluation across Topology Boundaries**:
  - In `PostFilter` / `Preemption`, if a preemptor pod requires structured dynamic devices, candidate evaluation checks both compute capacity (`NodeResourcesFit`) and device availability (`DynamicResources`).
  - Nodes lacking available devices matching the claim's CEL selectors or NUMA topology return unresolvable statuses in `DynamicResources.PreFilter` / `Filter`.
  - Preemption ensures that lower-priority compute victims are only evicted on nodes that can simultaneously satisfy both the compute requests and the structured DRA device/topology allocations.

---

## 10. Prometheus Metrics & Observability

| Metric Name | Type | Labels | Description |
|---|---|---|---|
| `scheduler_preemption_attempts_total` | Counter | - | Total number of preemption attempts initiated in `PostFilter`. |
| `scheduler_preemption_victims` | Histogram | - | Number of victim pods selected per pod preemption attempt. |
| `scheduler_workload_preemption_victims` | Histogram | - | Number of victim pods selected per workload/gang preemption attempt. |
| `scheduler_preemption_evaluation_duration_seconds` | Histogram | `preemptor_type`, `status_code` | Latency of preemption candidate evaluation passes. |
| `scheduler_preemption_execution_duration_seconds` | Histogram | `preemptor_type`, `result` | Duration of synchronous or asynchronous preemption execution. |
| `scheduler_preemption_goroutines_duration_seconds` | Histogram | `result` | Wall-clock execution time of async preemption background goroutines. |
| `scheduler_preemption_goroutines_execution_total` | Counter | `result` | Number of async preemption goroutines completed (`success` / `error`). |
| `scheduler_preemption_pdb_violations_total` | Counter | `preemptor_type` | Total number of PDB violations incurred during preemption. |
| `scheduler_preemption_workload_disruptions` | Histogram | `preemptor_type` | Number of atomic pod groups disrupted during preemption. |

---

## 11. Critical Developer & Agent Invariants

1. **CycleState Isolation during Dry Run**:
   - `checkNode` in `DryRunPreemption` evaluates candidate nodes concurrently. It **MUST** pass `state.Clone()` into `SelectVictimsOnNode`. Never share mutable `CycleState` between parallel evaluation workers.
2. **Main Node vs Remote Node Mutation Boundary**:
   - Within `SelectVictimsOnNode`, only the main candidate node's `NodeInfo` is mutated via `RemovePod` / `AddPodInfo`. Remote nodes of distributed pod groups are updated exclusively via `PreFilterExtensionRemovePod` / `AddPod` hooks.
3. **PDB Reprieval Ordering**:
   - PDB-violating victims ($V_{violate}$) MUST always be reprieved before non-violating victims ($V_{respect}$) to uphold KEP-3838 disruption minimization.
4. **Disruption Target Status Patching**:
   - Victim pods MUST have their status patched with condition `DisruptionTarget` before deletion, preserving attribution and pod lifecycle observability.
5. **Context Detachment in Async Eviction**:
   - `prepareCandidateAsync` MUST create a new context (`context.Background()`) detached from the scheduling cycle, as the scheduling cycle context is canceled immediately upon returning from `scheduleOne`.
6. **Resolvability Status Enforcement**:
   - Any plugin returning `fwk.UnschedulableAndUnresolvable` during Filter MUST halt victim discovery on that node immediately. Never attempt preemption on nodes with unresolvable mismatches (such as missing declared hardware/kernel features or invalid volume access modes).

---

## 12. Testing Patterns & Verification

```bash
# Run all preemption unit tests with race detection
GOTOOLCHAIN=auto go test -v -race ./pkg/scheduler/framework/preemption/...

# Run DefaultPreemption plugin tests
GOTOOLCHAIN=auto go test -v -race ./pkg/scheduler/framework/plugins/defaultpreemption/...

# Run scheduler integration preemption tests (including KEP-2837, KEP-4818, KEP-5598, KEP-6072 suites)
GOTOOLCHAIN=auto go test -v ./test/integration/scheduler/preemption/misc/
```
