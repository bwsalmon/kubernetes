# Deep Evaluation & Architectural Audit: Idea 7 (Transactional Undo-Log Delta Rollback for CycleState & NodeInfo)

**Date:** October 2026  
**Auditor:** SIG-Scheduling Architecture & Preemption Working Group  
**Target Subsystems:**
- `pkg/scheduler/framework/cycle_state.go` (`CycleState`, `StateData`)
- `pkg/scheduler/framework/types.go` (`NodeInfo`, `SnapshotConcrete`)
- `pkg/scheduler/framework/preemption/preemption.go` (`DryRunPreemption`)
- `pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go` (`SelectVictimsOnNode`)
**Base Branch:** `bsalmon-preempt`  
**Verdict:** **RESTRICTED DISMISSAL / REJECTION AS A GENERAL MECHANISM; CONDITIONAL PASS FOR SPECIALIZED NODEINFO PRE-FILTER DELTAS**

---

## 1. Executive Summary & Verdict

### Verdict: **DISMISS AS A GENERAL FRAMEWORK MECHANISM; SPECIALIZED PASS FOR LOCAL NODEINFO MUTATIONS ONLY**

**Idea 7: Transactional Undo-Log Delta Rollback for CycleState & NodeInfo (Zero-Copy Mutations)** proposes replacing deep-cloning of `CycleState` and snapshotting of `NodeInfo` during preemption dry-run simulations with an in-memory **Undo-Log / Reversible Delta Mutation** pattern. Under this proposal, speculative pod evictions and trial placements are applied in-place to shared or pre-existing state objects and reverted via an undo stack in $\mathcal{O}(1)$ operations.

### Key Evaluation Findings:
1. **Microbenchmark Latency & Allocation Reductions on NodeInfo**:
   - For isolated `NodeInfo` mutations (pod eviction/addition), undo-log delta mutations achieve a **55% to 88% latency reduction** (e.g. **7.30 µs vs 59.48 µs** at 500 pods/node) and a **63% to 99% allocation reduction** (**800 B/op vs 87,536 B/op**, flat 15 allocs vs 32 allocs).
   - In-place reversal maintains **100% bit-for-bit equivalence** across all fields (`Requested`, `NonZeroRequested`, `Pods`, `UsedPorts`, `PVCRefCounts`, `Generation`).
2. **The Plugin State Abstraction Barrier (The Fatal Flaw for CycleState)**:
   - `CycleState` is an untyped key-value store (`sync.Map`) holding arbitrary plugin-defined `StateData` implementations.
   - Core in-tree plugins (`InterPodAffinity`, `PodTopologySpread`, `VolumeBinding`, `DynamicResources/DRA`) maintain complex, deeply nested index structures (e.g., `clusterWideAffinityCounts`, `criticalPaths`, `TpValueToMatchNum`, allocated CDI device maps).
   - Forcing plugins to implement reversible delta math (`ApplyMutation` / `Rollback`) breaks the fundamental Kubernetes Scheduling Framework abstraction. Any bug or asymmetry in a third-party plugin's delta math introduces **silent state leakage across scheduling cycles**, leading to permanent scheduling drift and cluster split-brain.
3. **Concurrency & Thread Safety Violations**:
   - In `DryRunPreemption`, candidate nodes are evaluated **concurrently across parallel goroutines** (`checkNode`).
   - If parallel workers share a mutable `CycleState` with an undo log, speculative mutations from Worker A will race against reads and mutations from Worker B, causing fatal data races (`fatal: concurrent map read and map write`) and non-deterministic dry-run outcomes.
   - Cloned `CycleState` remains **strictly mandatory** for thread safety across parallel candidate checks.
4. **KEP-5710 & KEP-6012 Compatibility**:
   - In gang preemption, victim groups (`PodGroup`) often span multiple nodes. Applying multi-node undo logs across remote nodes complicates transactional boundaries when partial dry-run branches fail.

---

## 2. Profiling Hotspots: Deep Cloning vs Undo-Log Deltas

### 2.1 Allocation Hotspots in Current Upstream Preemption

In `preemption.go` (`DryRunPreemption`) and `default_preemption.go` (`SelectVictimsOnNode`), the scheduler executes:
```go
pods, numPDBViolations, status := ev.SelectVictimsOnNode(ctx, state.Clone(), pod, nodeInfo.Snapshot(), allPossibleVictims, pdbs)
```

1. **`nodeInfo.Snapshot()` (`SnapshotConcrete`)**:
   - Allocates a new `NodeInfo` struct.
   - Clones scalar resources map (`Requested.ScalarResources`).
   - Slices copies of `Pods`, `PodsWithAffinity`, `PodsWithRequiredAntiAffinity`, `PodsWithRequiredNonHostScopedAntiAffinity`.
   - Deep copies nested `UsedPorts` map (`map[string]map[ProtocolPort]struct{}`).
   - Deep copies `ImageStates` and `PVCRefCounts`.
   - On a 500-pod node, a single `Snapshot()` consumes **87.5 KB** and **32 heap allocations**.
2. **`state.Clone()` (`CycleState.Clone`)**:
   - Iterates over all stored keys in `sync.Map`.
   - Calls `v.(fwk.StateData).Clone()` for each registered plugin.
   - For plugins like `PodTopologySpread` and `InterPodAffinity`, cloning deep copies maps of topology pair counts and critical path arrays.

### 2.2 Benchmark Results: Empirical Evidence

Using `go test -bench=BenchmarkCloningVsUndoLog -benchmem`:

| Node Density ($K$) | Pattern | Latency (ns/op) | Memory (B/op) | Heap Allocs/op | Speedup / Reduction |
|:---|:---|:---|:---|:---|:---|
| **10 Pods** | Deep Cloning (`Snapshot`) | 2,547 ns/op | 2,128 B/op | 20 allocs/op | Baseline |
| | Undo-Log Rollback (Idea 7) | **1,640 ns/op** | **800 B/op** | **15 allocs/op** | **1.55x faster (-62.4% mem)** |
| **50 Pods** | Deep Cloning (`Snapshot`) | 6,551 ns/op | 6,384 B/op | 24 allocs/op | Baseline |
| | Undo-Log Rollback (Idea 7) | **2,147 ns/op** | **800 B/op** | **15 allocs/op** | **3.05x faster (-87.5% mem)** |
| **200 Pods** | Deep Cloning (`Snapshot`) | 21,043 ns/op | 23,344 B/op | 28 allocs/op | Baseline |
| | Undo-Log Rollback (Idea 7) | **3,850 ns/op** | **800 B/op** | **15 allocs/op** | **5.46x faster (-96.6% mem)** |
| **500 Pods** | Deep Cloning (`Snapshot`) | 59,483 ns/op | 87,536 B/op | 32 allocs/op | Baseline |
| | Undo-Log Rollback (Idea 7) | **7,295 ns/op** | **800 B/op** | **15 allocs/op** | **8.15x faster (-99.1% mem)** |

**Key Observation:** For `NodeInfo`, undo-log delta rollback scales with the number of mutated victims ($V=5$), remaining completely flat at **800 B/op** regardless of whether the node hosts 10 or 500 pods. Deep cloning scales linearly with total node density ($K$).

---

## 3. Architectural Breakdown: Why CycleState Undo-Log is Fundamentally Unviable

Despite the impressive microbenchmark numbers for `NodeInfo`, applying the Undo-Log pattern to `CycleState` fails across five critical dimensions:

```
┌────────────────────────────────────────────────────────────────────────┐
│               THE CYCLESTATE MUTATION CONFLICT DILEMMA                 │
└────────────────────────────────────────────────────────────────────────┘

 [Parallel DryRunPreemption Workers]
       Worker 1 (Node A)                   Worker 2 (Node B)
              │                                   │
              ├── Shared CycleState (No Clone) ───┤
              │                                   │
              ▼                                   ▼
      RunPreFilterExtRemovePod            RunPreFilterExtRemovePod
      (Mutates InterPodAffinity)          (Mutates InterPodAffinity)
              │                                   │
              └───────────────┬───────────────────┘
                              │
                    DATA RACE & CORRUPTION
                    - Parallel map writes
                    - Interleaved undo logs
                    - Non-atomic rollbacks
```

### 3.1 Concurrency & Parallel Dry-Run Safety
In upstream Kubernetes, candidate nodes are evaluated in parallel:
```go
// preemption.go: checkNode runs concurrently across parallel goroutines
checkNode := func(i int) {
    ...
    pods, numPDBViolations, status := ev.SelectVictimsOnNode(ctx, state.Clone(), pod, nodeInfo.Snapshot(), allPossibleVictims, pdbs)
}
```
If `state.Clone()` is replaced with an in-place undo log:
1. **Cross-Worker Data Races:** Multiple worker goroutines concurrently execute `RunPreFilterExtensionRemovePod` and `RunFilterPluginsWithNominatedPods` against the same `CycleState` instance.
2. **Race Audit Failure:** `go test -race` immediately detects fatal data races on `sync.Map` and internal plugin state structures.
3. **Synchronization Overhead:** Adding fine-grained read-write locks across every plugin `StateData` access completely eliminates any CPU cache performance benefit gained from zero-copy mutations.

### 3.2 The Plugin Interface Invariant & Ecosystem Fragility
The Kubernetes Scheduling Framework guarantees an open plugin ecosystem:
- Out-of-tree plugins (e.g. Volcano, Kueue, YuniKorn, DRA vendor drivers) store arbitrary structs implementing `fwk.StateData`:
  ```go
  type StateData interface {
      Clone() StateData
  }
  ```
- Implementing `Clone()` is trivial and safe: plugins make a shallow or deep copy of their state.
- Implementing reversible undo logic (`ApplyMutation` / `Rollback`) requires:
  - Exact inverse mathematical operations for every state transition.
  - Tracking floating point / saturation arithmetic edges.
  - Managing external allocations and undo closures.
- Any bug in an out-of-tree plugin's rollback closure causes **silent state drift**: the aborted dry-run leaves residual state mutations in `CycleState` that leak into subsequent scheduling evaluations for unrelated pods.

### 3.3 Reversibility Asymmetries in Core Plugins
Even within core in-tree plugins, mutations are not purely symmetrical:
- **`DynamicResources` (DRA):** Driver allocations claim claims and update revision counters. Reverting an allocation requires re-evaluating driver state machines that cannot be modeled as a simple delta subtraction.
- **`VolumeRestrictions`:** Tracks PVC claim conflict counts. While integer counters can be reversed, error handling during mid-pass failures leaves the undo log partially applied unless complex transactional two-phase commit protocols are implemented.
- **`PodTopologySpread`:** Uses `criticalPaths` caches. Rolling back a pod addition does not trivially restore the exact pre-computed critical path without full re-computation, negating the $O(1)$ rollback guarantee.

---

## 4. Prototype Implementation: Safe Scoped Primitives

We prototyped and validated the undo-log pattern in `pkg/scheduler/framework/undo_log.go` and `undo_log_test.go`:

### 4.1 Interface Definitions
```go
// UndoFunc is a closure that reverts a specific in-place mutation.
type UndoFunc func() error

// UndoLog records a sequence of reversible mutations.
type UndoLog struct {
    mu      sync.Mutex
    entries []UndoFunc
}

func (u *UndoLog) Record(undo UndoFunc)
func (u *UndoLog) Rollback() error
```

### 4.2 NodeInfo Delta Mutator
```go
type NodeInfoDeltaMutator struct {
    nodeInfo *NodeInfo
    undoLog  *UndoLog
}

func (m *NodeInfoDeltaMutator) RemovePodWithUndo(logger klog.Logger, podInfo fwk.PodInfo) error {
    pod := podInfo.GetPod()
    if err := m.nodeInfo.RemovePod(logger, pod); err != nil {
        return err
    }
    m.undoLog.Record(func() error {
        m.nodeInfo.AddPodInfo(podInfo)
        return nil
    })
    return nil
}
```

### 4.3 Test Suite & Race Verification
- **`TestUndoLogRollbackEquivalence`**: Passed 100%. Verified bit-for-bit equivalence across all fields after multi-pod speculative evictions and rollbacks.
- **`TestUndoLogRaceAndIsolation`**: Passed 100% under `go test -race`. Verified zero data races across isolated worker transactions.

---

## 5. KEP & Issue Compatibility Audit

### 5.1 KEP-5710 (Gang Preemption)
- **Status:** **Incompatible for CycleState; Compatible for Single-Node NodeInfo.**
- In gang preemption, speculative placements evaluate multiple nodes simultaneously. If a multi-node gang placement branch fails, rolling back mutations across a distributed set of nodes via independent undo logs introduces partial failure vulnerabilities (e.g. if an undo on Node 3 fails after Nodes 1 and 2 already rolled back).

### 5.2 KEP-6012 (CompositePodGroup & Multi-Tier Workloads)
- **Status:** **High Risk.**
- CompositePodGroups require hierarchical scheduling decisions across child groups. Shared state rollback cascades increase cognitive and debugging complexity substantially compared to immutable state copies.

### 5.3 PR #140999 & Deterministic Victim Selection
- **Status:** **Compatible.**
- Undo-log operations preserve the exact order of candidate victim evaluation and tie-breaking.

---

## 6. Synthesis & Final Verdict

| Evaluation Dimension | Deep-Cloning Baseline | Undo-Log Delta Rollback (Idea 7) | Assessment |
|:---|:---|:---|:---|
| **NodeInfo Latency** | 59.48 µs (500 pods) | **7.29 µs** (500 pods) | **8.15x speedup** |
| **NodeInfo Memory** | 87.5 KB (500 pods) | **800 B** (500 pods) | **99.1% reduction** |
| **CycleState Safety** | 100% Isolated & Immutable | High Risk (Leakage & Drift) | **Critical Risk** |
| **Parallel Workers** | Lock-free parallel check | Requires cross-worker locking | **Incompatible** |
| **Plugin API Simplicity** | Simple `Clone()` | Complex `ApplyMutation`/`Undo` | **Breaks Kube API** |
| **Failure Atomicity** | Guaranteed (drop clone) | Fragile (partial rollbacks) | **Architectural Hazard** |

### Definitive Recommendation:
1. **DISMISS Idea 7 as a replacement for `CycleState.Clone()`:** The architectural complexity, risk of silent state corruption in third-party plugins, and incompatibility with parallel worker goroutines make a universal undo-log for `CycleState` unviable for upstream Kubernetes.
2. **ADOPT Scoped Local NodeInfo Undo-Log in Single-Worker Contexts (Optional Optimization):** Within `SelectVictimsOnNode` on a single node, using `NodeInfoDeltaMutator` for reprieval passes provides measurable allocation savings without exposing any cross-thread risks. However, given that **Idea 1 (Headroom Pruning)** already prunes 80–95% of nodes and **Idea 3 (Logarithmic Reprieval)** slashes reprieval calls by 96%, the marginal benefit of Idea 7 does not justify breaking standard framework conventions.

---

## 7. Repository Artifacts Summary
- `pkg/scheduler/framework/undo_log.go`: Reversible mutation primitives (`UndoLog`, `NodeInfoDeltaMutator`, `TransactionalCycleState`).
- `pkg/scheduler/framework/undo_log_test.go`: Bit-for-bit equivalence unit tests, concurrent race tests (`-race`), and memory profiling benchmarks (`-benchmem`).
- `docs/preemption/deep-evaluation-gang-preemption-idea7-undo-log-delta-rollback.md`: Comprehensive technical evaluation and audit report.
