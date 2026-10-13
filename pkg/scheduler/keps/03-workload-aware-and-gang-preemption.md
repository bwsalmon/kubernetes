# Domain 3: Workload-Aware & Gang Preemption

**Governing KEPs:**
- **KEP-5710**: *Workload-Aware Preemption (WAP) / Gang Scheduling*
- **KEP-6012**: *Composite Pod Groups & Hierarchical Workload Preemption*

**Primary Packages:**
- `pkg/scheduler/schedule_one_podgroup.go`
- `pkg/scheduler/framework/preemption/` (`podgrouppreemption.go`, `executor.go`, `manager.go`, `types.go`, `util.go`, `preemption.go`)
- `pkg/scheduler/framework/plugins/defaultpreemption/`
- `pkg/scheduler/framework/plugins/gangscheduling/`
- `pkg/scheduler/framework/plugins/helper/podgroup.go`
- `pkg/apis/scheduling/` (`types.go`, `v1alpha3`, `v1beta1`)
- `pkg/scheduler/backend/queue/` (`workload_forest.go`, `pod_group_member_pods.go`)

---

## 1. Executive Summary & Architectural Motivation

Distributed artificial intelligence, machine learning training (e.g., PyTorch Distributed Data Parallel / DDP, Megatron-LM, DeepSpeed, Ray, TensorFlow Horovod, JAX), high-performance computing (MPI), and distributed analytics pipelines require **all-or-nothing (gang) atomic scheduling**. In a gang workload requiring $N$ cooperating pods (e.g., 64 distributed GPU worker pods), the job cannot make forward progress unless all $N$ (or at least `minMember`) pods are concurrently scheduled, bound, and executing across the cluster. Scheduling any subset $M < N$ pods while the remaining pods remain unscheduled wastes cluster compute and accelerator resources, as the $M$ active pods idle while waiting for communication synchronization barriers.

### 1.1 Deficiencies of Legacy Single-Pod Preemption (KEP-562)

Under Kubernetes' classical single-pod preemption mechanism (KEP-562 / Domain 1):
1. **Partial Gang Allocation Waste & Starvation**: The scheduler evaluates pods independently in single-pod `scheduleOne` iterations. If Pod 1 of Gang $A$ preempts running lower-priority pods on Node 1, but subsequent Pods $2 \dots N$ fail to find placement or preemption candidates on other nodes, the preempted workloads on Node 1 are needlessly destroyed. Meanwhile, Pod 1 holds resources on Node 1 until its permit timeout expires, stalling cluster progress and causing cascading thrashing.
2. **Inter-Gang Deadlock (Split-Brain Preemption)**: When two high-priority gangs (Gang $A$ and Gang $B$) arrive concurrently, Pods of Gang $A$ may preempt victims on Nodes 1..4, while Pods of Gang $B$ preempt victims on Nodes 5..8. Neither gang achieves its `minMember` quorum across the cluster, leading to reciprocal starvation and timeout rejection cycles.
3. **Hierarchical Workload Structure Blindness**: Complex ML training and data engineering architectures comprise multi-tier topologies (e.g., Parameter Server tier + Worker tier + Evaluator / Checkpoint driver). Single-pod and flat gang models cannot coordinate preemption priority inheritance, group-level dependencies, or selective cascading evictions across parent-child composite structures.
4. **Zombie Resource Holding from Partial Eviction**: If one worker of an existing gang is preempted by an external high-priority pod, the surviving workers of the victim gang continue running even though their collective job is broken and cannot complete.

### 1.2 The WAP & Composite Pod Group Solution

**KEP-5710** (*Workload-Aware Preemption*) and **KEP-6012** (*Composite Pod Groups*) resolve these foundational challenges by introducing:
- **`scheduleOnePodGroup`**: Atomic scheduling loop evaluating entire PodGroups and CompositePodGroup hierarchies in a single scheduling transaction.
- **Cluster-Wide Multi-Node Preemption Simulation**: Isolated simulation across `MutableSnapshotSharedLister` that hypothetically clears potential victims across all cluster nodes, validates gang placement feasibility, and executes global victim reprieval.
- **Zero-Victim-Leakage Invariant**: Strict all-or-nothing preemption guarantees where zero victim pods are disrupted unless the entire preemptor gang is guaranteed to achieve placement quorum.
- **`CompositePodGroup` Hierarchical Trees**: Recursive priority resolution (`traverseHierarchyUp`, `getPodPriority`) and configurable `DisruptionMode` cascading semantics (`Single` vs `All`).
- **Permit Phase Synchronization & Rollback Lifecycles**: Coordinated permit waits, waiting pod preemption in scheduler memory, and clean rollback mechanics upon timeout or scheduling cancellation.

---

## 2. API Specifications & Data Structures

The Workload-Aware Gang Scheduling and Preemption APIs are declared in `pkg/apis/scheduling/` and versioned in `scheduling.k8s.io/v1alpha3` and `scheduling.k8s.io/v1beta1`.

### 2.1 PodGroup API (`scheduling.k8s.io/v1beta1`)

A `PodGroup` represents a concrete runtime grouping of member pods that share scheduling and disruption policies:

```yaml
apiVersion: scheduling.k8s.io/v1beta1
kind: PodGroup
metadata:
  name: distributed-training-worker-group
  namespace: ml-training
spec:
  # Optional reference to parent CompositePodGroup in hierarchical topologies
  parentCompositePodGroupName: distributed-training-root
  
  # Scheduling policy defining gang parameters
  schedulingPolicy:
    gang:
      minMember: 16
      scheduleTimeoutSeconds: 300
      
  # Disruption mode specifying victim blast radius
  disruptionMode:
    all: {} # Options: all | single
    
  # Priority class name resolved to 32-bit integer priority
  priorityClassName: high-priority-ml
  
  # Preemption policy: PreemptLowerPriority (default) | PreemptNever
  preemptionPolicy: PreemptLowerPriority
  
  # Optional topology constraints
  schedulingConstraints:
    topology:
      domain: topology.kubernetes.io/zone
```

#### Core Field Semantics:
- **`spec.schedulingPolicy.gang.minMember`**: Minimum number of member pods that must be schedulable concurrently for the gang cycle to succeed. If fewer than `minMember` pods can be placed across the cluster, the entire gang is rejected or routed to `PodGroupPostFilter`.
- **`spec.schedulingPolicy.gang.scheduleTimeoutSeconds`**: Maximum duration (in seconds) member pods wait in the `Permit` phase while victim pods terminate or resource claims bind.
- **`spec.disruptionMode`**:
  - **`All` (`AllDisruptionMode`)**: Atomic disruption. If any member of this group is chosen as a preemption victim, all other running/scheduled member pods in this group across the cluster are aggregated into a single atomic `Victim` unit and evicted simultaneously.
  - **`Single` (`SingleDisruptionMode`)**: Granular disruption. Individual member pods can be preempted independently without triggering group-wide evictions.
- **`spec.preemptionPolicy`**:
  - `PreemptLowerPriority`: PodGroup is eligible to preempt lower-priority victims across the cluster.
  - `PreemptNever`: PodGroup cannot preempt any running workloads; pods remain in queue until capacity becomes available naturally.

### 2.2 CompositePodGroup API (`scheduling.k8s.io/v1beta1` / KEP-6012)

A `CompositePodGroup` establishes a hierarchical tree structure containing child `PodGroup` and nested `CompositePodGroup` entities:

```yaml
apiVersion: scheduling.k8s.io/v1beta1
kind: CompositePodGroup
metadata:
  name: distributed-training-root
  namespace: ml-training
spec:
  # Optional reference to ancestor CompositePodGroup (up to WorkloadMaxTreeDepth = 8)
  parentCompositePodGroupName: null
  
  # Scheduling policy defining inter-group scheduling strategy
  schedulingPolicy:
    simultaneous:
      minGroupCount: 2 # Both PS and Worker tiers required
      
  # Cascading disruption mode for the entire subtree
  disruptionMode:
    all: {} # Options: all | single
    
  # Priority class applied to the root
  priorityClassName: critical-training-job
  preemptionPolicy: PreemptLowerPriority
```

#### Hierarchy Limits and Tree Invariants:
- **`WorkloadMaxTreeDepth = 8`**: Maximum allowable depth of parent-child `CompositePodGroup` chains to guard against cyclic references and unbounded recursion.
- **`WorkloadMaxPodGroupTemplates = 8`**: Maximum number of child templates per composite workload.
- **`Strategy` (`Simultaneous` vs `StrictOrdering`)**: Defines whether child groups are scheduled concurrently or sequentially.

### 2.3 Condition State Machine & Observability

`PodGroupStatus` and `CompositePodGroupStatus` track scheduling progression via well-defined conditions:

| Condition Type | Reason | Meaning |
| :--- | :--- | :--- |
| `PodGroupInitiallyScheduled` | `Scheduled` | All required `minMember` pods successfully placed and bound. |
| `PodGroupInitiallyScheduled` | `Unschedulable` | Insufficient capacity, placement constraint mismatch, or preemption inability. |
| `PodGroupInitiallyScheduled` | `SchedulerError` | Internal scheduler error or plugin execution failure. |
| `PodGroupInitiallyScheduled` | `PodGroupError` | Invalid group layout, cyclic dependency, or configuration divergence. |
| `DisruptionTarget` | `PreemptionByScheduler` | PodGroup or CompositePodGroup is marked for eviction by higher-priority preemptor. |

---

## 3. Workload-Aware Gang Scheduling Architecture (`schedule_one_podgroup.go`)

The core scheduling pipeline for gang workloads is implemented in `pkg/scheduler/schedule_one_podgroup.go`.

```
               +-------------------------------------------------------------+
               |              scheduleOnePodGroup(podGroupInfo)              |
               +-------------------------------------------------------------+
                                              |
                                     [Validate Hierarchy]
                               (validatePodGroup / Hierarchy)
                                              |
                                  [Reconcile with Snapshot]
                                (reconcilePodGroupWithSnapshot)
                                              |
                                 [Run Scheduling Algorithm]
                               (runRootSchedulingAlgorithm)
                                /                           \
      (Single PodGroup)        /                             \ (CompositePodGroup)
      podGroupSchedulingAlgorithm                   compositePodGroupSchedulingAlgorithm
             |                                              |
      [Evaluate Placements]                         [Evaluate Subtree Placements]
             |                                              |
      Did >= minMember Fit?                         Did Subtree Satisfy Quorum?
            /       \                                      /       \
     (Yes) /         \ (No)                         (Yes) /         \ (No)
          /           \                                  /           \
   [Submit Bind]  [PodGroupPostFilter]            [Submit Bind]  [PodGroupPostFilter]
                        |                                              |
               +--------v----------------------------------------------v--------+
               |            PodGroupEvaluator.Preempt (Gang Preemption)         |
               +----------------------------------------------------------------+
                        |
            Preemption Successful?
                   /          \
            (Yes) /            \ (No)
                 /              \
         [Set Nominated]     [Update Unschedulable Condition]
         [Actuate Async]     [Zero Victims Evicted]
```

### 3.1 Scheduling Cycle Stages

1. **Hierarchy Validation (`validatePodGroup`, `validatePodGroupHierarchy`)**:
   - Inspects tree integrity up to `WorkloadMaxTreeDepth`.
   - Validates that child `PodGroups` within a hierarchy do not specify divergent or conflicting `PreemptionPolicy` settings (e.g., child with `PreemptNever` under root with `PreemptLowerPriority`).
   - Ensures parent-child links form an acyclic directed graph.
2. **Snapshot Reconciliation (`reconcilePodGroupWithSnapshot`)**:
   - Verifies the state of already-scheduled or assumed pods belonging to the group within `CacheSnapshot`.
3. **Multi-Pod Leaf Placement (`podGroupPodSchedulingAlgorithm`)**:
   - Iterates through unscheduled member pods in topological or indexed order.
   - Executes `PreFilter`, `Filter`, `PreScore`, `Score`, `Reserve`, and `Permit` plugins for each member in the gang against the simulated snapshot.
   - Temporarily assumes each successfully placed pod in the snapshot so subsequent members in the same gang recognize resource reservations of earlier members.
4. **All-or-Nothing Quorum Verification**:
   - If the count of schedulable pods is $\ge \text{minMember}$, the placement is marked successful and committed.
   - If fewer than $\text{minMember}$ pods can fit, all temporary snapshot reservations for the gang are rolled back via `revertFns`, and the scheduler invokes `schedFwk.RunPodGroupPostFilterPlugins`.

---

## 4. Gang Preemption Engine (`pkg/scheduler/framework/preemption/`)

When gang placement fails, the scheduler delegates to `PodGroupEvaluator` (`pkg/scheduler/framework/preemption/podgrouppreemption.go`).

### 4.1 Cluster-Wide Candidate Simulation Pipeline

```
+-------------------------------------------------------------------------------------------+
|                              PodGroupEvaluator.Preempt()                                  |
+-------------------------------------------------------------------------------------------+
                                              |
                           [Check Ongoing Preemption]
                    IsPodGroupWaitingForVictims(pgInfo)?
                                  /            \
                           (Yes) /              \ (No)
                                /                \
                     [Return Nominated]    [GenerateVictims]
                                                 |
                                     [selectVictimsOnDomain]
                                                 |
                   +-----------------------------v-----------------------------+
                   | 1. Hypothetical Removal:                                  |
                   |    mutableLister.RemovePod() for ALL potential victims   |
                   +-----------------------------------------------------------+
                                                 |
                   +-----------------------------v-----------------------------+
                   | 2. Feasibility Evaluation:                                |
                   |    Run podGroupSchedulingFunc(ctx) on cleared cluster    |
                   +-----------------------------------------------------------+
                                                 |
                                      Did Gang Quorum Fit?
                                        /            \
                                 (Yes) /              \ (No)
                                      /                \
          +--------------------------v--+     +--------v----------------------+
          | 3. Global Victim Reprieval: |     | Abort Gang Preemption:        |
          |    Iterate potential victims|     | - Return Unschedulable Status |
          |    in DESCENDING priority;  |     | - ZERO victims evicted        |
          |    Add back & re-run Filter;|     | - Zero state modified         |
          |    Reprieve if gang fits!   |     +-------------------------------+
          +-----------------------------+
                         |
          +--------------v--------------+
          | 4. Multi-Node Actuation:    |
          |    ActuatePodGroupPreemption|
          |    - Evict final victims    |
          |    - Set NominatedNodeNames |
          +-----------------------------+
```

### 4.2 Detailed Algorithm Steps

#### Step 1: Preemption Eligibility & Ongoing Check
- Evaluates `getPreemptionPolicy(pgInfo)`. If `PreemptNever`, preemption immediately terminates with `Unschedulable`.
- Checks `preemptionManager.Executor().IsPodGroupWaitingForVictims(pgInfo)`. If the gang already has nominated nodes with victims undergoing asynchronous termination, it returns current nominating infos without recalculating.

#### Step 2: Global Victim Discovery (`GenerateVictims` / `getWorkloadPreemptionVictims`)
- Scans all nodes in `SharedLister`.
- Identifies running pods whose effective priority is lower than `pgInfo.GetPriority()`.
- Grouping:
  - If a victim pod belongs to a `PodGroup` or `CompositePodGroup` where any ancestor specifies `DisruptionMode=All`, all scheduled member pods of that entire hierarchy across all nodes are aggregated into a single atomic `Victim` struct via `searchCrossNodesVictimPods`.
  - If `DisruptionMode=Single` or the pod is standalone, it becomes an individual `PodVictim`.
- Sorts victims in descending order of priority using `prepareDomainVictims`:
  1. Priority order (lower priority victims considered first for removal).
  2. PDB compliance: Non-violating victims precede PDB-violating victims.

#### Step 3: Global Cluster Simulation & Hypothetical Eviction
- Invokes `removePods(victim)` on `MutableSnapshotSharedLister` for every potential victim across all nodes.
- Executes `podGroupSchedulingFunc(ctx)`:
  - Runs full scheduling cycle for preemptor gang pods against the emptied snapshot.
  - If the gang *cannot* fit even when all potential lower-priority victims in the cluster are removed, the preemption process aborts immediately. **Zero victims are evicted**.

#### Step 4: Monotonic Global Victim Reprieval
- If the gang successfully placed all `minMember` pods with assignments on nodes $\{N_1, N_2, \dots, N_k\}$, the scheduler minimizes collateral disruption via global reprieval:
  - Iterates over potential victims in reverse order (`slices.Backward(potentialVictims)`), testing highest-priority victims first.
  - For each victim $V$:
    1. Simulates adding $V$'s pods back to the snapshot via `addVictimPodsWithPreFilter`.
    2. Runs `PreFilterExtensionAddPod` and `RunFilterPluginsWithNominatedPods` for all preemptor pods on their assigned nodes.
    3. If all preemptor pods still fit on their assigned nodes, $V$ is **reprieved** (kept running).
    4. If any preemptor pod fails filters, $V$ cannot be reprieved and is marked in `victimsToPreempt`.

#### Step 5: Multi-Node Actuation & Nomination
- Constructs `selectVictimsResult`:
  - `nominatedNodeNames`: Maps each assigned preemptor pod to its target nominated node.
  - `victims`: Final list of unreprieved victims across all involved nodes.
- Calls `preemptionManager.Executor().ActuatePodGroupPreemption`:
  - Marks victim pods with `DisruptionTarget` (`PodReasonPreemptionByScheduler`).
  - Emits Kubernetes Events (`Preempted` / `Preempting`).
  - Issues asynchronous `DELETE` requests to `kube-apiserver`.
  - Updates in-memory preemption tracking sets (`preempting`, `lastVictimsPendingPreemption`).

---

## 5. Hierarchical Priority & Disruption Rules (KEP-6012)

### 5.1 Root Priority Inheritance (`getPodPriority` & `traverseHierarchyUp`)

In a multi-tier `CompositePodGroup`, priority resolution prevents priority inversion across tree levels:

```
                  +----------------------------------+
                  |  Root CompositePodGroup (P=1000) |
                  +----------------------------------+
                                   |
                  +----------------------------------+
                  |  Child CompositePodGroup (P=800) |
                  +----------------------------------+
                                   |
                  +----------------------------------+
                  |    Leaf PodGroup (P=500)         |
                  +----------------------------------+
                                   |
                          [Member Pod (P=100)]
```

When evaluating preemption:
1. `traverseHierarchyUp` traverses parent pointers from the leaf `PodGroup` up to the root `CompositePodGroup` (capped at `WorkloadMaxTreeDepth = 8`).
2. `getPodPriority` resolves the effective preemption priority to the **root ancestor's priority** ($P=1000$).
3. This ensures that subordinate pods of a mission-critical composite workload (e.g., helper loggers or metric sidecars with lower local priority) inherit the protection of the root workload and can preempt lower-priority batch jobs.

### 5.2 DisruptionMode Cascading Resolution (`getHighestAllAncestor`)

When a running pod is identified as a candidate victim:
1. `getHighestAllAncestor` traverses upwards from the candidate pod's `PodGroup`.
2. If any ancestor in the chain has `DisruptionMode = All`, the search identifies the highest `All` ancestor.
3. `searchCrossNodesVictimPods` queries `PodGroupStates` in the cache snapshot for all leaf `PodGroups` under that highest ancestor.
4. All running pods across all nodes belonging to those groups are bundled into a single atomic `Victim`.
5. **Atomic Eviction**: If reprieval fails, all pods across all nodes in that composite subtree are evicted together. This prevents "zombie" gang pods from consuming cluster capacity when their peer pods are killed.

---

## 6. Deadlock Prevention, Permit Timeout & Rollback Lifecycles

### 6.1 Inter-Gang Deadlock Prevention
- **Strict Queue Ordering**: Gangs in `activeQ` are ordered by Priority and creation timestamp (`PrioritySort`). Only the highest-priority gang at the head of the queue undergoes scheduling and preemption evaluation at any given time.
- **Transactional Snapshot Isolation**: Preemption simulations run against an isolated `MutableSnapshotSharedLister` transaction. Intermediate preemption states do not mutate cluster state until all-or-nothing feasibility is mathematically proven.

### 6.2 Permit Phase Timeout & Garbage Collection
1. When member pods of a gang are scheduled onto nodes with terminating victims, they enter the `Permit` phase with `scheduleTimeoutSeconds`.
2. If victims terminate before timeout:
   - Node capacity frees up.
   - Preemptor pods unblock from `Permit` and proceed to `PreBind` and `Bind`.
3. If victim termination stalls (e.g., slow graceful termination or webhook delays) and `scheduleTimeoutSeconds` expires:
   - The scheduler's `WaitingPodsMap` timeout handler fires.
   - All waiting pods in the gang are rejected simultaneously.
   - Temporary nominations and reservations are cleared.
   - Pods are returned to `activeQ` with exponential backoff, allowing other workloads to schedule.

### 6.3 In-Memory Waiting Pod & PreBind Preemption
To avoid deadlocks where a preemptor targets a victim pod that is currently in the `Permit` (`WaitingPod`) or `PreBind` phase:
- `Executor.PreemptPod` checks `fh.GetWaitingPod(victim.UID)`. If present, it executes `waitingPod.Preempt(...)` directly in scheduler memory without waiting for an API server round-trip.
- If the victim is in `PreBind`, `fh.GetPodInPreBind(victim.UID).CancelPod(...)` immediately aborts binding.

---

## 7. Verification & Test Matrix

The implementation of KEP-5710 and KEP-6012 is validated by comprehensive unit and integration test suites:

### 7.1 Integration Test Suites (`test/integration/scheduler/preemption/podgroup/`)

| Test Function | Target Scenario |
| :--- | :--- |
| `TestPodGroupPreemption` | Core all-or-nothing preemption, single vs multi-node victims, zero-victim abortion on insufficient capacity. |
| `TestPodGroupPreemptionStatus` | Verification of `PodGroupInitiallyScheduled` condition transitions (`Scheduled`, `Unschedulable`, `SchedulerError`). |
| `TestPodGroupPreemption_NominatedNodeNameRespected` | Preservation of `nominatedNodeName` across multi-node placements during asynchronous victim eviction. |
| `TestCompositePodGroupPreemption` | Hierarchical preemption across multi-tier composite trees, verifying root priority inheritance. |
| `TestCompositePodGroupPreemption_MixedDisruptionModes` | Verification of `DisruptionMode=All` vs `DisruptionMode=Single` cascading eviction boundaries. |
| `TestMultiTierCompositePodGroupPreemption_NestedDisruptionModes` | Deeply nested 3-tier composite trees with alternating disruption modes. |
| `TestPartialGangPreemption_PodGroupAndCompositePodGroup` | Quorum verification ensuring partial gang preemption is prevented. |
| `TestCompositePodGroupPreemption_CyclicReferencesAndEdgeCases` | Cyclic hierarchy detection, validation error assertions, and orphan child handling. |
| `TestPodGroupPreemption_MixedPreemptionPolicies` | Mixed `PreemptNever` and `PreemptLowerPriority` enforcement across preemptors and victims. |
| `TestCompositePodGroupPreemption_DivergentHierarchyValidation` | Rejection of composite hierarchies with conflicting child preemption policies. |

### 7.2 Unit Test Suites (`pkg/scheduler/framework/preemption/` & `pkg/scheduler/`)

- `TestPodGroupEvaluator_Preempt_Victims`: Comprehensive simulation of priority mixing, PDB violation ordering, and disruption mode grouping.
- `TestPodGroupCycle_PodGroupPostFilter`: Integration between `scheduleOnePodGroup` and `PodGroupPostFilter` plugin invocations.
- `TestPodGroupPreemptionEvaluationDurationMetric`: Observability metrics telemetry verification.
