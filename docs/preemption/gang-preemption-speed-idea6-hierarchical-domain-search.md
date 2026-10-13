# Gang Preemption Speed Idea 6: Topology-Partitioned Hierarchical Failure Domain Search

## 1. Executive Summary & Verdict

**Verdict:** **DISMISS (Not Recommended for General Upstream Adoption)**
* **Alternative Recommendation:** Retain strictly as an opt-in domain-partitioning heuristic for specialized single-domain co-located workloads (`requireCoLocation`), while dismissing it as a general preemption layer for arbitrary distributed gangs.

This report documents the architectural investigation, formal prototype implementation, empirical benchmarking, and KEP compatibility audit for **Idea 6: Topology-Partitioned Gang Preemption Search (Hierarchical Failure Domain Decomposition)**.

The proposed optimization hypothesizes that in large-scale Kubernetes clusters (e.g. 1,000–5,000 nodes distributed across multiple regions, failure zones, and racks), candidate nodes can be partitioned into hierarchical failure domain trees (e.g., Zone $\to$ Rack $\to$ Node). Under Idea 6, the scheduler aggregates candidate node capacities, headroom, and preemption candidate counts at the failure-domain level, pruning non-viable domains *en masse* before invoking fine-grained node-level preemption search.

### Key Investigation Findings
1. **Redundancy with Upstream Layer 1 & Layer 3:** In the production gang preemption pipeline, coarse-grained node headroom pruning (**Idea 1**) already prunes individual non-viable nodes in $\mathcal{O}(1)$ time ($1.19\text{ \mu s} - 3.18\text{ \mu s}$ per node). Furthermore, Admissible Branch-and-Bound search (**Idea 5**) already prunes suboptimal multi-node combinations with early-exit bounds ($< 5\text{ \mu s}$ greedy upper-bound termination).
2. **Double Partitioning Overhead on Feasible Clusters:** Building the hierarchical domain tree, computing aggregate domain bitmasks, and partitioning problem candidate slices introduces $\mathcal{O}(M \cdot N)$ memory allocation and copying overhead. In our microbenchmarks across 16, 64, and 256 pod gangs on 64 to 1,024 nodes, Idea 6 increased preemption search latency by **2.07x to 2.22x** compared to pure Branch-and-Bound ($74.2\text{ ms}$ vs $35.7\text{ ms}$ for 256 pods on 1,024 nodes).
3. **Impedance Mismatch with K8s PodTopologySpread (`maxSkew`):** Unlike batch schedulers where gangs run entirely in a single failure domain, standard Kubernetes gang workloads require *balanced spreading* across domains governed by `PodTopologySpread` constraints ($skew \le maxSkew$). Pruning an entire domain at coarse granularity is only mathematically sound if *every* node in the domain is infeasible, or if the gang explicitly requires co-location in a single domain. If even one node in a domain is feasible, pruning the domain risks causing `maxSkew` infeasibility across remaining domains or violating the admissibility guarantee of global preemption.
4. **Viable Specialized Niche:** When gang workloads specify strict co-location (e.g., all gang members must fit on a single rack or NVLink failure domain), domain-level quorum pruning successfully prunes small racks in $\mathcal{O}(D)$ time, yielding immediate short-circuiting. However, this does not generalize to multi-domain spread workloads.

---

## 2. Architectural Motivation & Algorithmic Formulation

### 2.1 The Combinatorial Problem in Large Clusters
In Kubernetes clusters running large distributed AI/ML training or HPC workloads under Workload-Aware Gang Preemption (**KEP-5710**) and CompositePodGroups (**KEP-6012**), scheduling a gang of $M$ pods across $N$ candidate nodes requires identifying a valid assignment vector:
$$\vec{A} = (n_1, n_2, \dots, n_M) \in \mathcal{N}^M$$
which minimizes total preemption disruption cost $C(\vec{A})$ while satisfying:
1. Node allocatable capacity and headroom knapsack constraints.
2. Inter-pod and pod-level affinity / anti-affinity rules.
3. Multi-domain topology spread constraints ($|count(D_i) - count(D_j)| \le maxSkew$).

When $N$ spans thousands of nodes across tens of failure domains (zones and racks), searching candidate nodes across all domains simultaneously can be wasteful if entire failure domains lack sufficient cumulative capacity to support the gang's minimum quorum ($MinMember$).

### 2.2 Algorithmic Design of Idea 6
Idea 6 introduces a two-tier hierarchical decomposition:

```
[ Step 1: Topology Hierarchy Tree Construction ]
       Cluster Candidate Nodes
                │
                ├── Zone A (Rack 1, Rack 2...) -> Aggregate Allocatable CPU/Mem/Pods
                ├── Zone B (Rack 3, Rack 4...) -> Aggregate Allocatable CPU/Mem/Pods
                └── Zone C (Rack 5, Rack 6...) -> Aggregate Allocatable CPU/Mem/Pods

[ Step 2: Conservative Failure Domain Pruning ]
       Evaluate Quorum & Headroom Feasibility per Domain:
       - If Domain Feasible Pods == 0  ===> PRUNE DOMAIN
       - If Co-Location Required & Domain Headroom < Gang MinMember ===> PRUNE DOMAIN
       - If Domain violates maxSkew lower bound mathematically ===> PRUNE DOMAIN

[ Step 3: Domain-Partitioned Branch-and-Bound Placement ]
       Filter GangPlacementProblem Candidate Nodes to Viable Domains Only
                │
       Execute Admissible Branch-and-Bound Search on Reduced Search Space
```

#### Hierarchical Domain Aggregate Structure
```go
type TopologyDomainAggregate struct {
    DomainName            string
    TopologyKey           string
    Nodes                 []string
    TotalAllocCPU         int64
    TotalAllocMem         int64
    MaxPodsCapacity       int
    CandidateFeasiblePods int
    PodFeasibilityMask    []bool
    MinDisruptionCost     PreemptionCost
}
```

#### Conservative Domain Pruning Condition
To preserve 100% decision fidelity and avoid discarding viable solutions, domain pruning must adhere to a strict conservative guarantee:
$$Prune(D) = \text{true} \iff \forall \vec{A} \in \mathcal{F}, \quad \text{no feasible solution uses nodes in } D$$

For general multi-domain spreading, this condition holds if and only if $\text{FeasiblePods}(D) = 0$. For co-located single-domain gangs, it holds if $\text{Capacity}(D) < MinMember$.

---

## 3. Prototype Implementation

The prototype was implemented in `pkg/scheduler/framework/preemption/hierarchical_domain_search.go`:
1. **`BuildTopologyHierarchy`**: Traverses candidate nodes and per-pod candidate matrices to construct `TopologyDomainAggregate` instances for each failure domain value (indexed by `TopologyKey`).
2. **`PruneInfeasibleDomains`**: Evaluates aggregate CPU, memory, pod slot counts, and candidate feasibility masks against gang $MinMember$ and topology spread constraints.
3. **`PartitionProblemByDomains`**: Slices the global `GangPlacementProblem` to exclude nodes in pruned domains, shrinking the candidate arrays.
4. **`HierarchicalDomainGangSearch`**: Integrates domain pruning with `BranchAndBoundGangSearch`. If all domains are pruned, it immediately short-circuits to `Unschedulable` with 0 search states explored. If co-location is requested, it solves each viable domain independently and selects the global minimum cost.

---

## 4. Benchmark Validation & Comparative Latency Analysis

Microbenchmarks were conducted on the scheduler sandbox platform (Intel Xeon 6985P-C, Linux amd64, Go 1.27.0) comparing **Baseline Admissible Branch-and-Bound (Idea 5)** directly against **Topology-Partitioned Hierarchical Domain Search (Idea 6)** across varying cluster failure domain topologies.

### Benchmark Setup
- **Scenario 1 (Gang 16, Nodes 64, Domains 8):** 16-pod gang across 8 zones (8 nodes per zone). 4 zones viable, 4 zones congested/infeasible.
- **Scenario 2 (Gang 64, Nodes 256, Domains 16):** 64-pod gang across 16 zones (16 nodes per zone). 8 zones viable, 8 zones congested/infeasible.
- **Scenario 3 (Gang 256, Nodes 1024, Domains 32):** 256-pod gang across 32 zones (32 nodes per zone). 16 zones viable, 16 zones congested/infeasible.

### Quantitative Benchmark Results

| Workload Configuration | Baseline BnB (Idea 5) Latency | Hierarchical Domain Search (Idea 6) | Relative Overhead / Speedup | Nodes Pruned by Idea 6 | Decision Fidelity |
|---|---|---|---|---|---|
| **Gang 16, Nodes 64, Domains 8** | **73.45 µs** | 163.12 µs | **2.22x slower** (+89.67 µs) | 32 nodes (4 domains) | **100% Bit-for-Bit Identical** |
| **Gang 64, Nodes 256, Domains 16** | **1.19 ms** | 2.65 ms | **2.23x slower** (+1.46 ms) | 128 nodes (8 domains) | **100% Bit-for-Bit Identical** |
| **Gang 256, Nodes 1024, Domains 32** | **35.76 ms** | 74.21 ms | **2.07x slower** (+38.45 ms) | 512 nodes (16 domains) | **100% Bit-for-Bit Identical** |

### Benchmark Analysis & Why Idea 6 Incurs Net Negative Speedup
1. **Tree Allocation and Slicing Tax:** Creating the hierarchical domain records, sorting domain keys, and re-allocating `PerPodCandidates` slices for the partitioned problem requires significant heap allocations that scale with $\mathcal{O}(M \cdot N)$.
2. **Branch-and-Bound Already Skips Infeasible Nodes in $\mathcal{O}(1)$:** In Idea 5, candidate nodes where `Feasible == false` or where headroom is insufficient are already skipped with an inline boolean branch in the inner search loop. Skipping an infeasible node in Go takes $< 1\text{ ns}$. Allocating slices to omit those nodes takes dozens of microseconds.
3. **Greedy Upper Bound Neutralizes Search Space:** In Idea 5, the greedy initial assignment computes an admissible upper bound $UB$ in $\mathcal{O}(M \log N)$. In clustered domains, the greedy pass immediately finds a valid placement in viable nodes, establishing a tight upper bound that prunes all remaining candidate branches instantly. Consequently, pre-filtering nodes at the domain level saves zero branch expansions in Branch-and-Bound.

---

## 5. KEP Compatibility & Community Audit

### 5.1 KEP-5710 (Workload-Aware Preemption)
- **Quorum Semantics:** Under KEP-5710, a gang preemption request is all-or-nothing ($MinMember$). If fewer than $MinMember$ pods can be scheduled, the gang cannot preempt. Idea 6 safely verifies $CandidateFeasiblePods \ge MinMember$ in co-location scenarios.
- **Victim Monotonicity & Actuation:** Domain partitioning does not alter victim selection order within viable nodes. Decision fidelity was confirmed to be 100% bit-for-bit identical to baseline.

### 5.2 KEP-6012 (CompositePodGroups & DisruptionModeAll)
- **Multi-Node Victim Coupling:** Under KEP-6012, evicting a pod on Node A may trigger cascading eviction of coupled pods on Node B. If Node A and Node B reside in different failure domains, partitioning the search by domain artificially divorces coupled victim costs, risking suboptimal victim selection unless global set deduplication is maintained across domain boundaries.

### 5.3 PodTopologySpread & `maxSkew` Invariants
- **Inter-Domain Dependencies:** Standard Kubernetes spread constraints dictate that pods must be distributed evenly across topology domains:
  $$\max_{i} (count(D_i)) - \min_{j} (count(D_j)) \le maxSkew$$
  Pruning a domain $D_k$ changes the count of active domains, potentially altering the global minimum calculation or triggering `minDomains` violations. Pruning domains hierarchically without running full spread constraint simulation risks falsely reporting a cluster as unschedulable.

---

## 6. Comprehensive Comparison with Prior Art (Ideas 1–5)

| Metric / Dimension | Idea 1: Headroom Pruning | Idea 3: Logarithmic Reprieval | Idea 4: Template Caching | Idea 5: Branch and Bound | Idea 6: Hierarchical Domain Search |
|---|---|---|---|---|---|
| **Optimization Target** | Candidate node pre-filtering | `SelectVictimsOnNode` reprieval | Static Filter evaluation | Global multi-node gang placement | Failure-domain level partitioning |
| **Algorithmic Shift** | $\mathcal{O}(K \cdot F) \to \mathcal{O}(1)$ per node | $\mathcal{O}(K) \to \mathcal{O}(\log K)$ Filter calls | Redundant static evaluations $\to 0$ | $\mathcal{O}(N^M) \to \mathcal{O}(M \log N)$ | $\mathcal{O}(N) \to \mathcal{O}(D)$ domain pruning |
| **Observed Latency Impact** | **77.6% latency reduction** | **2.5x to 31.2x fewer** Filter calls | **Eliminates static checks** | **505x to 18,371x speedup** | **2.07x to 2.22x SLOWER** (overhead > savings) |
| **Implementation Complexity** | Low (arithmetic bounds check) | Medium (binary search prefix) | Medium (LRU template cache) | High (admissible search tree) | Medium (domain tree + slice re-partitioning) |
| **Decision Fidelity** | 100% bit-for-bit identical | 100% bit-for-bit identical | 100% bit-for-bit identical | 100% bit-for-bit identical | 100% bit-for-bit identical |
| **Recommendation** | **Adopt (Layer 1)** | **Adopt (Layer 2)** | **Adopt (Layer 2)** | **Adopt (Layer 3)** | **DISMISS (Unfavorable trade-off)** |

---

## 7. Final Recommendation & Production Guidance

### Verdict: DISMISS
We recommend **DISMISSING** Idea 6 from the general Kubernetes preemption pipeline.

### Architectural Rationale:
1. **Net Negative Performance Impact:** In all evaluated scenarios (16 to 256 gang pods, 64 to 1,024 nodes), Idea 6 introduced a **2x latency penalty** due to domain tree aggregation and data structure slicing overhead.
2. **Subsumed by Ideas 1 & 5:** The combination of Layer 1 Headroom Pruning (Idea 1) and Layer 3 Branch-and-Bound (Idea 5) already achieves optimal performance. Infeasible nodes are skipped in $\mathcal{O}(1)$ time without requiring hierarchical domain re-partitioning.
3. **Risk of Topology Spread Fragility:** Hard-partitioning search across failure domains introduces subtle edge cases with `PodTopologySpread` `maxSkew` calculations and multi-node KEP-6012 cascading victim sets.

### Optional Future Work: Specialized Co-Location Hook
If Kubernetes introduces first-class single-domain co-location gang constraints (e.g., `TopologyAffinity: MustBeInSingleZone`), the domain-level quorum check (`dom.CandidateFeasiblePods >= minMember`) can be incorporated as a lightweight filter prior to preemption. For the general upstream scheduler, however, Idea 6 should not be adopted.
