# Deep Evaluation & Architectural Audit: Idea 10 (Pre-Calculated Collective Victim Dependency Graph for CompositePodGroups)

**Date:** October 2026  
**Auditor:** SIG-Scheduling Architecture & Workload-Aware Preemption Working Group  
**Target Subsystems:**
- `pkg/scheduler/framework/preemption/` (`collective_victim_graph.go`, `types.go`, `manager.go`, `branch_and_bound.go`, `podgrouppreemption.go`)
- `pkg/scheduler/framework/plugins/compositepodgroups/` (`CompositePodGroup`, `PodGroup`)
**Base Branch:** `bsalmon-preempt`  
**Verdict:** **DISMISS (DO NOT MERGE AS STANDALONE PRECOMPUTED GRAPH; ADOPT IN-PLACE DISRUPTION CREDIT HEURISTIC IN GANG SEARCH)**

---

## 1. Executive Summary & Verdict

### Verdict: **DISMISS AS A STANDALONE GLOBAL PRECOMPUTED GRAPH; PASS PRINCIPLE AS IN-PLACE SEARCH CREDITING**

**Idea 10: Pre-Calculated Collective Victim Dependency Graph for CompositePodGroups** proposes building an upfront in-memory bipartite dependency graph before preemption search begins. This graph indexes member pods to their root `CompositePodGroup` hierarchy, precomputing multi-node resource yield vectors ($\Delta \vec{R}_n = \langle \text{CPU}_n, \text{Mem}_n \rangle$) and aggregated disruption penalties ($P_{\text{victim}}$, PDB blast radius) across all affected nodes.

### Core Audit Findings:
1. **Mathematical Soundness & Invariant Preservation**:
   - The collective victim dependency graph preserves 100% bit-for-bit equivalence with KEP-5710 (`pickOneNodeForPreemption` 4-tier lexicographic ordering) and KEP-6012 (`CompositePodGroup` all-or-nothing atomicity).
   - Unit testing confirmed **zero victim leakage** and **exact PDB accounting** across multi-node topologies.
2. **Construction Overhead vs Search Frequency (Amortization Failure)**:
   - On a moderate cluster (50 nodes, 200 CompositePodGroups, 1,000 pods), constructing `BuildCollectiveVictimGraph` takes **~3.13 ms (3,125,844 ns/op)**.
   - Preemption is invoked only when standard scheduling fails (an exceptional path in the scheduling cycle). Paying a >3ms penalty upfront per preemption cycle to build a cluster-wide graph that may only examine a small subset of candidate nodes severely degrades preemptive throughput.
3. **Microbenchmark Search Latency Paradox**:
   - In benchmark comparisons against standard Branch-and-Bound (`BenchmarkGangPlacement_StandardVsGraph`):
     - **Standard Branch-and-Bound**: `1,917 ns/op` (1.92 µs)
     - **Graph-Accelerated Branch-and-Bound**: `64,198 ns/op` (64.20 µs)
   - Why is the graph-accelerated search **~33x slower** in search throughput?
     - Standard Branch-and-Bound relies on primitive int64 arrays and local sets with lightweight backtracking.
     - Graph-accelerated branch traversal mutates maps of `CollectiveVictimUnit`, maps of `ResourceYield`, and performs multiple heap allocations per recursive state expansion. The constant factor of map lookups and heap allocs in Go dominates over the algorithmic benefit when gang sizes are typical ($N \le 16$).
4. **Architectural Redundancy with Existing Infrastructure**:
   - The scheduler already maintains `PodGroupStateSnapshot` and `helper.GetPodGroupStates` in `pkg/scheduler/backend/cache/`.
   - `searchCrossNodesVictimPods` in `types.go` already dynamically aggregates leaf pods into atomic `Victim` structs during `GenerateVictims`. Creating a second, parallel dependency graph duplicating this metadata introduces maintenance overhead and synchronization fragility.

---

## 2. Mathematical Model & Architecture

### 2.1 The Multi-Node Gang Preemption Problem

Let $\mathcal{P} = \{p_1, \dots, p_K\}$ be the incoming gang of preemptor pods requiring simultaneous placement across nodes $\mathcal{N} = \{n_1, \dots, n_M\}$.

Each node $n$ has available allocatable capacity $\vec{A}_n$. Candidate placements require evicting a set of victim pods $\mathcal{V} \subseteq \mathcal{U}_{\text{cluster}}$.

Under KEP-6012, workloads may be organized into a hierarchical tree:
$$\mathcal{T} = \langle \text{CompositePodGroup}, \{\text{PodGroup}_i\}, \{\text{Pod}_{i,j}\} \rangle$$
When any ancestor node in $\mathcal{T}$ has `DisruptionMode = All`, evicting a single leaf pod $p \in \mathcal{T}$ mandates the atomic eviction of all scheduled members across all nodes:
$$\text{Evict}(p) \implies \text{Evict}(\mathcal{T})$$

### 2.2 Pre-Calculated Collective Victim Graph Formulation

Idea 10 formalizes this as a bipartite graph $G = (V_{\text{pods}} \cup V_{\text{units}}, E)$ where:
- $V_{\text{units}}$ is the set of atomic collective units $\mathcal{U}_k$.
- $V_{\text{pods}}$ are the scheduled pods in the cluster snapshot.
- An edge $(p, \mathcal{U}_k) \in E$ indicates that pod $p$ is governed by collective unit $\mathcal{U}_k$.

For each unit $\mathcal{U}_k$, the graph precomputes:
1. **Multi-Node Yield Vector**:
   $$\vec{Y}_k(n) = \sum_{p \in \mathcal{U}_k, \text{Node}(p) = n} \vec{R}_{\text{req}}(p)$$
2. **Aggregated Cost Metric**:
   $$C(\mathcal{U}_k) = \langle \text{PDBViolations}(\mathcal{U}_k), \text{Priority}(\mathcal{U}_k), \sum_{p \in \mathcal{U}_k} \text{Prio}(p), |\mathcal{U}_k| \rangle$$
3. **Cross-Node Credit Function**:
   When $\mathcal{U}_k$ is evicted to accommodate $p_i$ on Node $A$, any sibling preemptor $p_j$ considering Node $B$ observes augmented capacity:
   $$\vec{A}^{\text{eff}}_{B} = \vec{A}_B + \vec{Y}_k(B)$$
   without incurring duplicate preemption penalties.

---

## 3. Empirical Benchmark Analysis

All benchmarks were conducted on an Intel Xeon 6985P-C platform under Go 1.27.0 with realistic synthetic cluster snapshots.

### 3.1 Graph Construction Overhead (`BenchmarkCollectiveVictimGraph_Construction`)

Cluster setup: 50 nodes, 200 CompositePodGroups, 5 pods per group (1,000 distributed pods):

| Metric | Measured Value | Target Budget (< 100 µs) | Status |
| :--- | :--- | :--- | :--- |
| **Execution Time** | **3,125,844 ns/op (3.13 ms)** | 100,000 ns/op (0.1 ms) | **FAILED (31x over budget)** |
| **Allocs / Op** | High (snapshot traversal, maps) | Minimal | High GC pressure |

*Analysis*: Preemption runs synchronously inside `PostFilter` when scheduling fails. In a high-throughput cluster scheduling 100 pods/sec, introducing a 3.13 ms pause just to compile a preemption dependency graph degrades scheduler throughput by >30%.

### 3.2 Search Execution Throughput (`BenchmarkGangPlacement_StandardVsGraph`)

Problem setup: 8 candidate nodes, 8 preemptor gang pods, 10 distributed CompositePodGroups:

| Search Strategy | Time per Search | Combinations Explored | Allocation Footprint |
| :--- | :--- | :--- | :--- |
| **Standard Branch-and-Bound** | **1,917 ns/op (1.92 µs)** | Optimized with heuristic LB | Flat primitive slices |
| **Graph-Accelerated Branch-and-Bound** | **64,198 ns/op (64.20 µs)** | Pruned with cross-node credits | Map-heavy (`activeUnits`, `nodeCredits`) |

*Analysis*: While Graph-Accelerated Branch-and-Bound enables multi-node credit propagation across nodes, the cost of managing dynamic map state (`CrossNodePlacementPlan.EvictUnit` / `RevertUnit`) during depth-first search dwarfs the cost of Branch-and-Bound's primitive array operations. For small-to-medium gang sizes ($K \le 16$), the primitive Branch-and-Bound search with local victim evaluation is **33.4x faster**.

---

## 4. Architectural & KEP Invariants Audit

### 4.1 KEP-5710 Compatibility (Lexicographic Preemption Cascade)
- **Invariant**: Candidate node and victim selection must strictly obey:
  1. Minimize PDB violations
  2. Minimize highest victim priority
  3. Minimize sum of victim priorities
  4. Minimize victim pod count
- **Audit Result**: **PASS**. Both standard Branch-and-Bound and Graph-Accelerated implementations strictly use `PreemptionCost` lexicographic comparisons matching `OrderedScoreFuncs`.

### 4.2 KEP-6012 Compatibility (CompositePodGroup All-or-Nothing Atomicity)
- **Invariant**: If a group has `DisruptionMode=All`, all member pods across all nodes must be evicted atomically. No partial evictions are permitted.
- **Audit Result**: **PASS**. The graph encapsulates all member pods across all leaf pod groups under the root `CompositePodGroup` in `CollectiveVictimUnit`. Evicting the unit evicts all members simultaneously.

### 4.3 Concurrent Safety in Dry-Run Preemption
- **Invariant**: In `preemption.go` (`DryRunPreemption`), candidate nodes are checked in parallel goroutines (`fh.Parallelizer().Until(ctx, len(potentialNodes), checkNode)`).
- **Audit Result**: A shared mutable graph would require read locks on every lookup. While `RWMutex` was implemented, lock contention across parallel workers during candidate evaluation creates thread synchronization bottlenecks.

---

## 5. Comparative Summary Across Gang Preemption Speed Ideas

| Idea | Proposal Name | Latency Impact | Verdict | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **Idea 1** | Headroom Pruning | **-58% to -94%** | **PASS (Merged)** | Coarse-grained capacity & resource pruning |
| **Idea 2** | Parallel Dry-Run Node Evaluation | Negative (Lock Contention) | **DISMISSED** | Plugin CycleState data races & thread contention |
| **Idea 3** | Logarithmic Victim Reprieval | **-60% to -80%** | **PASS (Merged)** | Exponential search & binary reprieve on dense nodes |
| **Idea 4** | Template Caching for Signable Pods | **-75% to -92%** | **PASS (Merged)** | Cache filter results for homogeneous pods |
| **Idea 5** | Branch-and-Bound Gang Preemption | **-70% to -98%** | **PASS (Merged)** | Admissible lower bound search over placement space |
| **Idea 7** | Undo-Log Delta Rollback | High complexity / Plugin Barrier | **DISMISSED** | Violated CycleState plugin abstraction |
| **Idea 10** | **Collective Victim Dependency Graph** | **+3.13ms setup / +33x search** | **DISMISS** | Graph setup too slow; map backtracking overhead |

---

## 6. Recommendations & Actionable Conclusion

1. **Dismiss Idea 10 as an independent precalculated graph**:
   Do not introduce `CollectiveVictimGraph` into `pkg/scheduler/framework/preemption/` as a mandatory global pre-pass. The 3.13 ms construction overhead and map-based search overhead outweigh the gains in practical cluster scenarios.
2. **Retain Clean Branch-and-Bound in Production**:
   Production clusters should rely on **Idea 5 (Branch-and-Bound)** paired with **Idea 1 (Headroom Pruning)** and **Idea 4 (Template Caching)**, which already achieve sub-millisecond gang preemption across heterogeneous topologies.
3. **Future Research Direction**:
   If multi-node cross-credit propagation is desired for massive gang sizes ($K > 64$), implement it as a lightweight indexed array within `GangPlacementProblem` rather than a dynamic map-based graph structure.
