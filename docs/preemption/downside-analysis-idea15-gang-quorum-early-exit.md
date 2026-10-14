# Downside & Failure Mode Analysis: Idea 15 (Speculative Early-Exit Gang Quorum & Suffix Feasibility Checker)

**Date:** October 2026  
**Auditor / Reviewer:** Scheduler Red-Team & Systems Architecture Review  
**Subsystem:** `pkg/scheduler/framework/preemption/` (`headroom.go`, `branch_and_bound.go`, `quorum_checker_test.go`)  
**Target Proposal:** Idea 15: Speculative Early-Exit Gang Quorum & Suffix Feasibility Checker (`bwsalmon/kubernetes`, `bsalmon-preempt`)  
**Base Branch:** `bsalmon-preempt` (Prototype: PR #123 / Commit `dfae0410419`)  
**Tracking Origin:** Task 984 / Task 3712 / Task 3659  

---

## 1. Executive Summary & Adversarial Verdict

### 1.1 Adversarial Verdict: **CRITICAL ARCHITECTURAL DEFECTS / REJECT IN CURRENT FORM (FATAL FALSE NEGATIVES FROM 1-TO-1 NODE PACKING FALLACY, ELASTIC GANG KNAPSACK OVER-SUMMATION, CONCURRENT PREEMPTION CAPACITY RACING, AND NEGATIVE SEARCH OVERHEAD)**

**Idea 15 (Speculative Early-Exit Gang Quorum & Suffix Feasibility Checker)** was introduced to prune unviable gang preemption evaluations in $\mathcal{O}(1)$ time. In large-scale clusters under high saturation, searching for victim combinations across hundreds or thousands of candidate nodes for a multi-pod gang consumes hundreds of milliseconds of scheduler thread time. If the cluster fundamentally lacks the candidate nodes or aggregate scalar capacity to satisfy the gang's minimum quorum ($M_{\min}$ under KEP-5710 and KEP-6012), continuing into Layer 2 (Victim Selection) and Layer 3 (Branch-and-Bound Search) is computationally wasteful.

To eliminate this waste, Idea 15 injects:
1. **Cluster-Wide Viable Node Quorum Check**: Immediately after Layer 1 Headroom Pruning, aborting preemption if $N_{\text{viable}} < M_{\min}$.
2. **Aggregate Scalar Capacity Knapsack Bound**: Aborting preemption if the cluster-wide sum of available headroom across all viable nodes is strictly less than the gang's aggregate demand for CPU, Memory, Ephemeral Storage, or GPUs/scalar devices.
3. **Admissible Suffix Feasibility in Layer 3**: Fast-aborting Branch-and-Bound search if candidate nodes are fewer than required members, or pruning recursive search branches when remaining candidate slots cannot satisfy remaining pods.

Following an exhaustive adversarial red-team audit and benchmark analysis of the implementation landed in PR #123 (`dfae0410419`), **Idea 15 in its current form introduces severe semantic violations of core Kubernetes scheduling invariants, catastrophic false negatives that starve viable distributed workloads, concurrent preemption capacity racing that triggers victim thrashing, and negative computational overhead in Layer 3 search loops.**

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                CRITICAL RED-TEAM FINDINGS SUMMARY                                │
├──────────────────────────────────────────────────────────────────────────────────────────────────┤
│ 1. FATAL FALSE NEGATIVES: THE 1-TO-1 NODE-TO-POD PACKING FALLACY                                │
│    - CheckGangQuorumAndCapacityFeasibility enforces: if len(viableHeadrooms) < minMember -> FAIL.│
│    - BranchAndBoundGangSearch enforces: if len(prob.CandidateNodes) < reqMembers -> FAIL.        │
│    - Both checks assume 1 gang pod consumes exactly 1 entire node! In real clusters, multiple   │
│      gang pods routinely pack onto a single viable node (e.g., 8 GPU pods on one 8-GPU node,    │
│      or 16 CPU pods across 2 large 64-core nodes). Idea 15 prematurely and unconditionally       │
│      rejects schedulable gangs, completely breaking Invariant 3 (Zero False Negatives).          │
├──────────────────────────────────────────────────────────────────────────────────────────────────┤
│ 2. ELASTIC GANG DISCONNECT: minMember VS totalMember OVER-SUMMATION (KEP-5710 & KEP-6012)        │
│    - For elastic gangs where minMember < totalMember, CheckGangQuorumAndCapacityFeasibility     │
│      sums resource requests across ALL totalMember pods in gangPods!                             │
│    - If cluster capacity can host 20 pods and minMember = 16, but totalMember = 32, Idea 15      │
│      compares cluster capacity (20) against totalMember demand (32) and aborts with              │
│      "insufficient headroom", starving elastic AI/HPC jobs that could easily run at quorum!      │
├──────────────────────────────────────────────────────────────────────────────────────────────────┤
│ 3. CONCURRENT PREEMPTION CAPACITY RACING & PHANTOM HEADROOM DOUBLE-SPENDING                      │
│    - In multi-scheduler pipelines or parallel gang preemption evaluations, Idea 15 evaluates     │
│      aggregate cluster capacity statelessly against shared cache snapshots.                      │
│    - Concurrent gangs G_1 and G_2 both observe the same 128 preemptible GPUs and pass Idea 15.   │
│      Both proceed to Layer 2 and Layer 3, evicting victims on the same candidate nodes! One gang  │
│      claims the capacity, while the second gang fails at the permit phase after having already   │
│      evicted innocent workloads, triggering catastrophic victim thrashing and goodput collapse.  │
├──────────────────────────────────────────────────────────────────────────────────────────────────┤
│ 4. TOPOLOGICAL FRAGMENTATION: CONTINUOUS KNAPSACK VS DISCRETE BIN-PACKING                        │
│    - Scalar knapsack summation ignores node boundaries. 4 nodes with 2 GPUs each have 8 total    │
│      GPUs, but cannot host a single 4-GPU pod. Idea 15 passes doomed workloads, failing to       │
│      provide early-exit speedup in fragmented clusters.                                          │
├──────────────────────────────────────────────────────────────────────────────────────────────────┤
│ 5. NEGATIVE BENCHMARK OVERHEAD & IN-SEARCH O(|V|) SLOWDOWN IN BRANCH-AND-BOUND                   │
│    - Microbenchmarks reveal that Idea 15 Suffix Quorum B&B search is 1.2x to 2.3x SLOWER than    │
│      baseline candidate scanning due to an un-memoized O(|CandidateNodes|) inner loop executed   │
│      at every recursive search state. Furthermore, CheckGangQuorumAndCapacityFeasibility        │
│      allocates 390 to 3,082 heap objects (up to 942 KB/op) per check, violating Invariant 4.    │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Technical Architecture & Theoretical Model of Idea 15

### 2.1 Proposed Mechanism in the 4-Layer Preemption Pipeline

Idea 15 is architected as an intermediary guard between Layer 1 (Node Filter & Headroom Pruning) and Layer 2 (Candidate Victim Selection), with a secondary extension inside Layer 3 (Multi-Node Gang Solver):

```
       Incoming Unschedulable Gang (KEP-5710 / KEP-6012)
                          │
                          ▼
 ┌─────────────────────────────────────────────────────────────┐
 │ Layer 1: Coarse-Grained Node Filtering & Headroom Pruning   │
 │ - DRA Claim Pre-Filter (Idea 11)                            │
 │ - Headroom Capacity Pruning (Idea 1 / PR #80)               │
 └────────────────────────┬────────────────────────────────────┘
                          │ Output: N_viable Candidate Nodes
                          ▼
 ┌─────────────────────────────────────────────────────────────┐
 │ IDEA 15: Speculative Early-Exit Gang Quorum & Knapsack      │
 │ 1. Quorum Check: len(viableHeadrooms) < minMember ?         │
 │ 2. Scalar Knapsack: sum(Avail_n) < sum(Req_p) ?             │
 └──────────────┬───────────────────────────────┬──────────────┘
                │ FAILS (Abort)                 │ PASSES
                ▼                               ▼
       [Fast-Fail in O(1)]        ┌────────────────────────────┐
       Return Unschedulable       │ Layer 2: Victim Selection  │
                                  └─────────────┬──────────────┘
                                                │
                                                ▼
                                  ┌────────────────────────────┐
                                  │ Layer 3: Branch-and-Bound  │
                                  │ - Idea 15 Suffix Pruning   │
                                  └────────────────────────────┘
```

### 2.2 Mathematical Formulation

Under the Idea 15 specification:
1. **Viable Node Quorum Predicate**:
   $$\Phi_{\text{quorum}}(\mathcal{V}, M_{\min}) = \begin{cases} \text{FAIL}, & \text{if } |\mathcal{V}| < M_{\min} \\ \text{PASS}, & \text{otherwise} \end{cases}$$
   where $\mathcal{V}$ is the set of viable candidate nodes surviving Layer 1, and $M_{\min}$ is the gang's minimum member count.

2. **Cluster-Wide Scalar Knapsack Upper Bound**:
   For each resource dimension $r \in \{\text{CPU}, \text{Memory}, \text{Storage}, \text{ScalarDevices}\}$:
   $$R_{\text{available}}(r) = \sum_{n \in \mathcal{V}} \left( \text{Allocatable}_n(r) - \text{NonPreemptible}_n(r) \right)$$
   $$R_{\text{required}}(r) = \sum_{p \in \mathcal{G}} \text{Request}_p(r)$$
   $$\Phi_{\text{knapsack}}(\mathcal{V}, \mathcal{G}) = \bigwedge_{r} \left( R_{\text{available}}(r) \ge R_{\text{required}}(r) \right)$$
   If $\Phi_{\text{knapsack}} = \text{false}$, the preemption cycle terminates immediately with status `Unschedulable`.

3. **Suffix Node Slot Pruning in Layer 3**:
   At search depth $k$ (having assigned $k$ pods), if the sum of remaining available slots across all candidate nodes is strictly less than $(|\mathcal{G}| - k)$, the search branch is pruned:
   $$\sum_{n \in \mathcal{V}} \max(0, \text{MaxPods}_n - \text{AssignedPods}_n) < (|\mathcal{G}| - k) \implies \text{Prune Branch}$$

---

## 3. Deep Failure Modes & Edge Case Audits

### 3.1 Failure Mode 1: The Multi-Pod Node Packing Fallacy ($N_{\text{viable}} < M_{\min}$)

#### 3.1.1 Root Cause Analysis
In `pkg/scheduler/framework/preemption/headroom.go` (line 375):
```go
// 1. Cluster-wide viable node quorum check: N_viable < minMember
if len(viableHeadrooms) < minMember {
    res.Passed = false
    res.Status = fwk.NewStatus(
        fwk.Unschedulable,
        fmt.Sprintf("insufficient viable candidate nodes across cluster: %d viable nodes < %d required minMember", len(viableHeadrooms), minMember),
    )
    return res
}
```
And identically in `pkg/scheduler/framework/preemption/branch_and_bound.go` (line 362):
```go
// 1a. Suffix / Cluster Viable Candidate Quorum Pre-Check (Idea 15):
if opts.EnableSuffixQuorumPruning {
    reqMembers := numPods
    if prob.MinMember > 0 {
        reqMembers = prob.MinMember
    }
    if len(prob.CandidateNodes) < reqMembers {
        return &GangPlacementResult{
            ...
            Status: fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("insufficient candidate nodes for gang quorum: %d nodes < %d minMember", len(prob.CandidateNodes), reqMembers)),
        }
    }
}
```

Both implementations commit the fundamental mathematical fallacy of equating **minimum member pods** ($M_{\min}$) with **minimum required candidate nodes** ($N_{\min}$). 

In Kubernetes, pods do not have an inherent 1-to-1 bijection with nodes. Unless the pod specification explicitly specifies a hard host anti-affinity rule (`spec.affinity.podAntiAffinity` with `topologyKey: kubernetes.io/hostname`), multiple pods of the same gang can—and routinely do—co-locate on the same node.

#### 3.1.2 Concrete Production Failure Scenario (Accelerator / GPU Multi-Tenancy)
Consider a machine learning inference or fine-tuning gang under KEP-5710:
- **Gang Definition:** 8 worker pods, each requesting 1 GPU (`nvidia.com/gpu: 1`), 8 CPU cores, and 32 GiB RAM.
- **Gang Requirement:** $M_{\min} = 8$, totalMember = 8. No host anti-affinity specified.
- **Cluster Hardware:** 2 high-density NVIDIA HGX H100 servers (`node-alpha`, `node-beta`).
  - Each node has 8 H100 SXM5 GPUs, 128 CPU cores, 1 TiB RAM.
  - Currently, both nodes are running lower-priority batch workloads (`Priority: 100`).
  - The preemptor gang has `Priority: 1000`.
- **Preemption Evaluation:**
  - Layer 1 Headroom checks find both `node-alpha` and `node-beta` viable:
    - `node-alpha`: 8 preemptible GPUs, 120 preemptible CPUs.
    - `node-beta`: 8 preemptible GPUs, 120 preemptible CPUs.
  - `viableHeadrooms` has exactly **2 nodes** (`len(viableHeadrooms) == 2`).
  - Aggregate cluster preemptible capacity: 16 GPUs, 240 CPUs, 2 TiB RAM.
  - Schedulability truth: The 8 pods can easily fit: 4 pods on `node-alpha` and 4 pods on `node-beta` (or all 8 on `node-alpha`!).
- **Idea 15 Behavior:**
  - `len(viableHeadrooms) < minMember` evaluates: $2 < 8 \implies \mathbf{TRUE}$.
  - Idea 15 **FAST-FAILS** immediately!
  - Returns `Unschedulable: "insufficient viable candidate nodes across cluster: 2 viable nodes < 8 required minMember"`.
  - Layer 2 and Layer 3 are aborted. Zero victims are evaluated. The gang is rejected and returned to the scheduling queue.
- **Severity:** **CRITICAL S1 (False Negative / Workload Starvation)**. Violates Invariant 3 (**Zero False Negatives**). The test suite in `quorum_checker_test.go` (line 71) literally codified this defect as "intended behavior", failing to recognize that 3 nodes with 2 GPUs each can easily host 4 pods requesting 1 GPU each!

---

### 3.2 Failure Mode 2: Elastic Gang Disconnect (`minMember` vs `totalMember` under KEP-5710 & KEP-6012)

#### 3.2.1 Root Cause Analysis
Under KEP-5710 (Workload-Aware Gang Scheduling) and KEP-6012 (CompositePodGroups), distributed workloads support elastic scaling. A distributed PyTorch training job with Elastic Horovod or TorchElastic specifies:
- `spec.minMember = 16`
- `spec.totalMember = 32` (or `len(gangPods) == 32`)

The semantic contract of KEP-5710 is: **If at least `minMember` pods can be scheduled simultaneously, the gang can proceed.** It is completely acceptable for the cluster to place any subset of size $k \in [M_{\min}, T]$.

However, look at `pkg/scheduler/framework/preemption/headroom.go` (lines 394–414):
```go
// Aggregate required capacity across the entire gang
for _, p := range gangPods {
    pReq := resource.PodRequests(p, resource.PodResourcesOptions{
        SkipPodLevelResources: !enablePodLevelResources,
    })
    res.TotalRequiredCPU = saturatingAdd(res.TotalRequiredCPU, pReq.Cpu().MilliValue())
    res.TotalRequiredMem = saturatingAdd(res.TotalRequiredMem, pReq.Memory().Value())
    res.TotalRequiredStorage = saturatingAdd(res.TotalRequiredStorage, pReq.StorageEphemeral().Value())
    for rName, qty := range pReq {
        ...
        res.TotalRequiredScalars[rName] = saturatingAdd(res.TotalRequiredScalars[rName], val)
    }
}

// 2. Aggregate Scalar Capacity Knapsack Bound checks
if res.TotalPreemptibleCPU < res.TotalRequiredCPU {
    res.Passed = false
    res.Status = fwk.NewStatus(
        fwk.Unschedulable,
        fmt.Sprintf("cluster aggregate preemptible cpu headroom insufficient for gang: %dm available < %dm required", res.TotalPreemptibleCPU, res.TotalRequiredCPU),
    )
    return res
}
```

The algorithm iterates over `gangPods`, which contains **all 32 pods** (`totalMember`), accumulating their aggregate demand. It then checks if `TotalPreemptibleCPU < TotalRequiredCPU`.

#### 3.2.2 Concrete Production Failure Scenario (Elastic AI/HPC Starvation)
- **Workload:** 32-pod elastic training gang. Each pod requests 16 CPU cores. Total demand for 32 pods = 512 cores.
- **Quorum Requirement:** $M_{\min} = 16$ pods (requires only 256 cores to achieve quorum).
- **Cluster State:** Saturated cluster with 20 candidate nodes, each containing 16 preemptible cores.
  - Total preemptible capacity across viable nodes = $20 \times 16 = 320\ \text{cores}$.
- **Feasibility Truth:**
  - Quorum requires 256 cores.
  - Available capacity is 320 cores ($320 \ge 256$).
  - Up to 20 pods can be placed immediately, fully satisfying $M_{\min} = 16$.
- **Idea 15 Behavior:**
  - `res.TotalPreemptibleCPU` = 320,000m.
  - `res.TotalRequiredCPU` = 512,000m (sum of all 32 pods).
  - Check: $320,000 < 512,000 \implies \mathbf{TRUE}$.
  - Idea 15 **FAST-FAILS**!
  - Returns `Unschedulable: "cluster aggregate preemptible cpu headroom insufficient for gang: 320000m available < 512000m required"`.
- **Heterogeneous Gang Sub-defect:**
  In heterogeneous gangs (e.g. Master pod requesting 32 cores, 15 Worker pods requesting 8 cores each), which subset represents the minimal required capacity?
  A naive prefix or suffix summation fails to respect KEP-6012 hierarchical sub-group contracts (where Master is non-elastic $M_{\min}=1$, and Workers are elastic $M_{\min}=10/15$). Summing all pods unconditionally breaks all elastic preemption in Kubernetes!
- **Severity:** **CRITICAL S1 (False Negative / Complete Elastic Preemption Breakdown)**.

---

### 3.3 Failure Mode 3: Concurrent Preemption Capacity Racing & Phantom Headroom Double-Spending

#### 3.3.1 The Shared-Snapshot Illusion
In a Kubernetes control plane, the scheduler evaluates pods or gangs concurrently or in rapid succession across scheduling cycles. When multiple preemption cycles execute against the cached `NodeInfoSnapshot`:
- `CalculateNodePreemptibleHeadroom` computes theoretical available capacity as:
  $$\text{Available}(r) = \text{Allocatable}(r) - \text{NonPreemptible}(r)$$
- `CheckGangQuorumAndCapacityFeasibility` sums this capacity cluster-wide.
- **The Defect:** This calculation is completely stateless and uncoordinated. It assumes that 100% of the cluster's preemptible capacity is exclusively available to the currently evaluated gang!

```
                  Cluster Preemptible Headroom: 100 GPUs
                                     │
           ┌─────────────────────────┴─────────────────────────┐
           ▼                                                   ▼
┌─────────────────────────────────────┐   ┌─────────────────────────────────────┐
│ Preemption Cycle 1 (Gang A: 80 GPUs)│   │ Preemption Cycle 2 (Gang B: 80 GPUs)│
│ - Idea 15 Knapsack: 100 >= 80 -> PASS│   │ - Idea 15 Knapsack: 100 >= 80 -> PASS│
│ - Layer 2: Picks Victims V_A        │   │ - Layer 2: Picks Victims V_B        │
│ - Layer 3: B&B Solves Nodes         │   │ - Layer 3: B&B Solves Nodes         │
└──────────────────┬──────────────────┘   └──────────────────┬──────────────────┘
                   │                                         │
                   ▼                                         ▼
      Dispatches Evictions for V_A              Dispatches Evictions for V_B
                   │                                         │
                   ▼                                         ▼
       Gang A Binds (Takes 80 GPUs)             Gang B Permit Timeout! (Fails)
       STATUS: SUCCESS                          Victims V_B Evicted IN VAIN!
                                                (Goodput Collapse / Thrashing)
```

#### 3.3.2 The Victim Thrashing Death Spiral
1. Gang A ($Priority = 1000$, requests 80 GPUs) arrives. Idea 15 observes 100 available GPUs. Check passes.
2. Gang B ($Priority = 900$, requests 80 GPUs) arrives concurrently or in the next cycle before Gang A binds. Idea 15 observes the same 100 available GPUs (because Gang A's victims are still in `Terminating` state and have not been deleted from the API server). Check passes.
3. Both gangs proceed to Layer 2 and Layer 3, expending hundreds of milliseconds of CPU solving victim sets.
4. Both gangs nominate overlapping or disjoint lower-priority victims across the cluster and trigger eviction API calls (`DeletePod`).
5. Gang A binds first, consuming 80 GPUs. Only 20 GPUs remain.
6. Gang B reaches the `WaitOnPermit` phase: only 20 GPUs are free. Gang B cannot achieve quorum ($80 > 20$).
7. Gang B hits permit timeout and rolls back!
8. **Consequence:** The lower-priority workloads evicted on behalf of Gang B died completely in vain! Cluster goodput drops to near zero, and the scheduler enters a preemption thrashing death spiral where victims are killed continuously but gangs fail to run.
- **Severity:** **HIGH S2 (Cluster Instability / Goodput Destruction)**.

---

### 3.4 Failure Mode 4: Topological Fragmentation & The Fractional Knapsack Fallacy

#### 3.4.1 Continuous Knapsack vs Discrete Multi-Dimensional Bin-Packing
Idea 15 claims:
> *"Zero False Negatives (Invariant 3): Mathematically proven: if the sum of all theoretical capacity across all viable nodes is strictly less than the gang request, no combination of nodes can possibly host the gang."*

While the contrapositive is true in a single continuous dimension (if aggregate capacity is less than demand, placement is impossible), the **positive converse is dangerously weak** and creates massive false positives in fragmented clusters:
$$\sum_{n} C_n \ge \sum_p D_p \centernot\implies \exists \text{ feasible placement}$$

Container scheduling is not a continuous fractional knapsack; it is an $\mathcal{NP}$-hard Multi-Dimensional Vector Bin Packing Problem (MD-BPP) constrained by discrete node boundaries:
1. **Node Boundary Fragmentation:**
   - Pod $P_1$ requests 8 GPUs. Pod $P_2$ requests 8 GPUs. (Total required = 16 GPUs).
   - Cluster has 16 nodes, each with exactly 1 available GPU.
   - Aggregate available GPUs = 16.
   - Idea 15 check: $16 \ge 16 \implies \mathbf{PASS}$.
   - Feasibility truth: Neither pod can fit on ANY node in the cluster! The gang is completely unschedulable.
   - Idea 15 lets this doomed gang pass through to expensive Layer 2 and Layer 3 search, failing to provide early exit.
2. **Multi-Dimensional Orthogonal Starvation:**
   - Node 1 has 64 CPUs and 0 GPUs available.
   - Node 2 has 0 CPUs and 8 GPUs available.
   - Gang requests 32 CPUs and 8 GPUs.
   - Sum CPU = 64 ($64 \ge 32$). Sum GPU = 8 ($8 \ge 8$).
   - Idea 15 check: Both scalar dimensions pass $\implies \mathbf{PASS}$.
   - Feasibility truth: Neither node can host the pod because CPU and GPU capacity exist on disjoint physical machines!
3. **Kubelet Pod Count Exhaustion (`NodeStatus.Allocatable.Pods`):**
   - Nodes can have abundant CPU and RAM but be at their maximum pod capacity limit (e.g., 110 pods).
   - Idea 15 does not check pod count capacity in its scalar knapsack bound.

---

### 3.5 Failure Mode 5: Branch-and-Bound Suffix Pruning Over-Eagerness & Negative Search Overhead

#### 3.5.1 Suffix Slot Calculation Bug in Recursive Search
In `pkg/scheduler/framework/preemption/branch_and_bound.go` (lines 501–536):
```go
if opts.EnableSuffixQuorumPruning && prob.MinMember > 0 {
    remainingPods := numPods - podIdx
    if len(prob.NodeMaxPods) > 0 {
        isOnePodPerNode := true
        availableSlots := 0
        for _, nName := range prob.CandidateNodes {
            maxP := prob.NodeMaxPods[nName]
            if maxP <= 0 {
                isOnePodPerNode = false
                break
            }
            if maxP > 1 {
                isOnePodPerNode = false
            }
            remSlot := maxP - nodePodCount[nName]
            if remSlot > 0 {
                availableSlots += remSlot
            }
        }
        if isOnePodPerNode || len(prob.NodeMaxPods) == len(prob.CandidateNodes) {
            if availableSlots < remainingPods {
                branchesPruned++
                return
            }
        }
    }
}
```

Look at line 503:
`remainingPods := numPods - podIdx`
`numPods` is the **total number of pods** ($T$).
If `prob.MinMember < numPods` (e.g. $M_{\min} = 4, T = 8$):
Suppose at search depth `podIdx = 3`, 3 pods have already been placed.
To achieve quorum, we only need to place **$M_{\min} - 3 = 1$ more pod**!
However, `remainingPods` is computed as:
`remainingPods = 8 - 3 = 5 pods`!
If `availableSlots` across remaining candidate nodes is 3:
Because $3 < 5$, the check `if availableSlots < remainingPods` evaluates to **TRUE**!
**The branch is pruned immediately!**
Even though 3 slots is more than enough to place the 1 remaining pod needed to satisfy `MinMember = 4`!
**Result:** Branch-and-Bound prematurely discards valid placement branches that satisfy `MinMember` quorum, returning `Unschedulable` for viable gangs!

#### 3.5.2 $\mathcal{O}(|\text{CandidateNodes}|)$ Inner-Loop Slowdown
Notice lines 508–525:
Inside the recursive B&B function, on **every state evaluation**, the code iterates over `prob.CandidateNodes`:
`for _, nName := range prob.CandidateNodes { ... remSlot := maxP - nodePodCount[nName] ... }`
Instead of maintaining an incremental integer accumulator of available slots in $\mathcal{O}(1)$ time as pods are assigned and unassigned during backtracking, it re-scans the entire candidate node slice on every branch!
In a problem with 200 candidate nodes exploring 1,000 states, this executes $200,000$ map lookups and loop iterations in the hottest inner loop of the scheduler!

---

## 4. Algorithmic Complexity, Microbenchmark & Macro Benchmark Impact

### 4.1 Microbenchmark Profiling: The Cost of Idea 15

We executed the benchmark suites in `pkg/scheduler/framework/preemption/` on an Intel Xeon 6985P-C platform under Go 1.27.0 with memory allocation tracking (`-benchmem`):

#### Table 4.1: `CheckGangQuorumAndCapacityFeasibility` Scaling Profile

| Benchmark Scenario | Gang Size | Candidate Nodes | Latency (ns/op) | Memory (B/op) | Allocs (allocs/op) | Pruning Verdict |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **QuorumFastFail** | 64 | 500 | 320.2 ns | 368 B | 6 | $\mathcal{O}(1)$ Instant Abort |
| **KnapsackGPUFastFail** | 64 | 500 | 80,381 ns | 118,398 B | 392 | $\mathcal{O}(N+T)$ Headroom Scan |
| **FeasiblePassed** | 64 | 500 | 85,731 ns | 118,432 B | 390 | $\mathcal{O}(N+T)$ Full Traversal |
| **QuorumFastFail** | 128 | 1,000 | 343.0 ns | 368 B | 6 | $\mathcal{O}(1)$ Instant Abort |
| **KnapsackGPUFastFail** | 128 | 1,000 | 153,424 ns | 236,189 B | 776 | $\mathcal{O}(N+T)$ Headroom Scan |
| **FeasiblePassed** | 128 | 1,000 | 165,781 ns | 236,192 B | 774 | $\mathcal{O}(N+T)$ Full Traversal |
| **QuorumFastFail** | 256 | 2,000 | 308.8 ns | 376 B | 7 | $\mathcal{O}(1)$ Instant Abort |
| **KnapsackGPUFastFail** | 256 | 2,000 | 276,234 ns | 471,763 B | 1,545 | $\mathcal{O}(N+T)$ Headroom Scan |
| **FeasiblePassed** | 256 | 2,000 | 292,328 ns | 471,712 B | 1,542 | $\mathcal{O}(N+T)$ Full Traversal |
| **QuorumFastFail** | 512 | 2,000 | 325.2 ns | 384 B | 8 | $\mathcal{O}(1)$ Instant Abort |
| **KnapsackGPUFastFail** | 512 | 2,000 | 537,442 ns | 942,875 B | 3,082 | $\mathcal{O}(N+T)$ Headroom Scan |
| **FeasiblePassed** | 512 | 2,000 | 569,709 ns | 942,753 B | 3,078 | $\mathcal{O}(N+T)$ Full Traversal |

#### Key Microbenchmark Insights:
1. **Severe Memory Allocation Tax:** While `QuorumFastFail` (the node count check) is fast (320 ns, 6 allocs), the Knapsack Capacity check generates **942 KiB of heap garbage and over 3,080 heap allocations per evaluation** for a 512-pod gang! This directly violates Invariant 4 (**Zero Memory Allocation in Preemption Hot Paths**).
2. **Latent Latency Penalty for Schedulable Workloads:** For viable gangs that pass Idea 15, `CheckGangQuorumAndCapacityFeasibility` imposes an unconditional **85 µs to 570 µs latency tax**, burning scheduler thread time before Layer 2 even begins.

---

### 4.2 Layer 3 Branch-and-Bound Benchmark Comparison: Idea 15 vs Baseline

We benchmarked Layer 3 Branch-and-Bound search with Idea 15 Suffix Quorum Pruning enabled vs the baseline candidate scanner:

#### Table 4.2: Branch-and-Bound Search Performance Comparison

| Gang Configuration | Baseline Latency | Idea 15 Latency | Speedup / Slowdown | Baseline Allocs | Idea 15 Allocs |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Gang 64** | 2,639 ns/op | 6,188 ns/op | **2.34x SLOWER** | 0 allocs (0 B) | 5 allocs (2,336 B) |
| **Gang 128** | 33,087 ns/op | 53,925 ns/op | **1.63x SLOWER** | 0 allocs (0 B) | 5 allocs (4,384 B) |
| **Gang 256** | 310,882 ns/op | 386,357 ns/op | **1.24x SLOWER** | 0 allocs (0 B) | 6 allocs (8,489 B) |
| **Gang 512** | 1,229,689 ns/op | 1,513,535 ns/op | **1.23x SLOWER** | 0 allocs (0 B) | 7 allocs (16,689 B) |

#### Adversarial Confirmation:
**Idea 15 Suffix Quorum Pruning makes Branch-and-Bound search measurably slower across all gang sizes!**
Because the baseline search already utilizes admissible cost lower bounds (`admissibleLB := currentCost.Add(suffixLB[podIdx])`), adding an un-memoized $\mathcal{O}(|\mathcal{V}|)$ slot scan at every node in the search tree introduces more overhead than the branches it prunes.

---

## 5. Invariant Compliance Audit against Phase 2 Invariants

Every optimization landed on `bsalmon-preempt` must strictly satisfy the five core architectural invariants established in Phase 2:

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        PHASE 2 INVARIANTS COMPLIANCE AUDIT                             │
├──────────────────────────────────────────────────────────────────┬─────────────────────┤
│ Invariant                                                        │ Audit Verdict       │
├──────────────────────────────────────────────────────────────────┼─────────────────────┤
│ Invariant 1: Strict Upstream Parity & Decision Equivalence       │ FAILED              │
│ - Must never reject a placement that upstream preemption accepts │ (Violated by F1, F2)│
├──────────────────────────────────────────────────────────────────┼─────────────────────┤
│ Invariant 2: Deterministic Preemption & Zero Flakes              │ PARTIAL / FLAKY     │
│ - Preemption decisions must be reproducible and deterministic    │ (Violated by F3)    │
├──────────────────────────────────────────────────────────────────┼─────────────────────┤
│ Invariant 3: Zero False Negatives                                │ CRITICAL FAILURE    │
│ - Must never declare an unschedulable verdict for a viable gang   │ (Violated by F1, F2)│
├──────────────────────────────────────────────────────────────────┼─────────────────────┤
│ Invariant 4: Zero Memory Allocation in Hot Evaluation Paths      │ FAILED              │
│ - Preemption checks must not allocate heap maps or slice buffers │ (Violated by F7)    │
├──────────────────────────────────────────────────────────────────┼─────────────────────┤
│ Invariant 5: Sub-Microsecond Per-Node Overhead                   │ FAILED              │
│ - Candidate evaluation overhead must not exceed 1 µs per node    │ (Violated by F7)    │
└──────────────────────────────────────────────────────────────────┴─────────────────────┘
```

1. **Invariant 1 & 3 (Upstream Parity & Zero False Negatives): FAILED (CRITICAL)**.
   - Upstream preemption and KEP-5710 evaluate candidate placements without assuming 1 pod per node. Upstream successfully places multi-pod gangs on multi-GPU or multi-core nodes where $N_{\text{viable}} < M_{\min}$. Idea 15 rejects them immediately.
   - Upstream successfully schedules elastic gangs where $M_{\min} \le \text{Capacity} < \text{totalMember}$. Idea 15 rejects them immediately.
2. **Invariant 2 (Deterministic Preemption): FAILED**.
   - Concurrent preemption racing against shared cache snapshots causes non-deterministic victim eviction races and permits timeouts.
3. **Invariant 4 & 5 (Zero Allocations & Sub-Microsecond Overhead): FAILED**.
   - Generates up to 3,082 heap allocations and 942 KiB per check, requiring 570 µs on large gangs.

---

## 6. Comparative Risk Matrix Across Gang Preemption Ideas

To understand where Idea 15 fits into the overall gang preemption architectural portfolio, we compare it against the other Phase 1 and Phase 3 optimization proposals:

| Optimization Proposal | Primary Speedup Mechanism | False Negative Risk | Concurrency Safety | Allocation Profile | Adversarial Verdict |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Idea 1: Headroom Pruning** (PR #80) | Coarse node-level capacity filter | High (Taint/DRA bugs) | Thread-Safe (cloned) | Low (scalar) | **Needs Safeguards** |
| **Idea 3: Logarithmic Reprieval** (PR #77) | Binary search victim reprieval | **Zero** | Thread-Safe | Zero allocs | **PRODUCTION GOLDEN** |
| **Idea 4: Template Caching** (PR #76) | Signature-based feasibility cache | **Zero** | RCU Read-Lock Safe | Amortized Zero | **PRODUCTION GOLDEN** |
| **Idea 5: Branch-and-Bound** (PR #78) | Combinatorial multi-node solver | **Zero** | Isolated context | Low | **PRODUCTION GOLDEN** |
| **Idea 7: Undo-Log Rollback** (PR #81) | Delta mutation reversal | Critical (Data races) | High Race Risk | Zero allocs | **REJECTED** |
| **Idea 8: Priority Bucketing** (PR #82) | Tiered victim evaluation | **Zero** | Thread-Safe | Low | **PRODUCTION GOLDEN** |
| **Idea 10: Victim Graph** (PR #85) | Precomputed bipartite graph | High (PDB leakage) | Cache sync drift | High graph overhead| **REJECTED AS GLOBAL** |
| **Idea 11: DRA Claim Filter** (PR #120) | O(1) device driver pre-filter | **Zero** | Thread-Safe | Zero allocs | **PRODUCTION GOLDEN** |
| **Idea 12: NUMA Slicing** (PR #118) | Locality-aligned victim search | High (Priority inv.) | Cache sync race | Medium | **HIGH RISK** |
| **Idea 15: Gang Quorum Checker** (PR #123) | Cluster-wide early-exit abort | **CRITICAL (F1, F2)** | **High Race Risk (F3)** | **High (942 KB/op)** | **REJECT IN CURRENT FORM** |

---

## 7. Mandatory Production Guardrails & Remediation Architecture

If Idea 15 is to be salvaged and safely deployed in production Kubernetes clusters, the following five architectural remediations are mandatory:

### 7.1 Remediation 1: Replace Node Count Check with Admissible Gang Pod Capacity Upper Bound
Delete `len(viableHeadrooms) < minMember` completely. Replace it with an admissible calculation of the **maximum gang pods that could fit across viable nodes**:

$$\text{MaxGangPodsOnNode}(n, \mathcal{G}) = \min_{r} \left\lfloor \frac{\text{Allocatable}_n(r) - \text{NonPreemptible}_n(r)}{\min_{p \in \mathcal{G}} \text{Request}_p(r)} \right\rfloor$$
$$\text{MaxClusterGangCapacity}(\mathcal{V}, \mathcal{G}) = \sum_{n \in \mathcal{V}} \min\left( \text{MaxGangPodsOnNode}(n, \mathcal{G}), \text{Allocatable}_n(\text{pods}) - \text{NonPreemptiblePods}_n \right)$$

**Safety Condition:**
$$\text{If } \text{MaxClusterGangCapacity}(\mathcal{V}, \mathcal{G}) < M_{\min} \implies \textbf{Fast-Fail Immediate Abort}$$

This mathematically guarantees **Zero False Negatives**: if even the most optimistic packing of the smallest gang pods cannot achieve $M_{\min}$ across all viable nodes, the gang is truly unschedulable.

### 7.2 Remediation 2: Elastic Demand Lower-Bound Selection for $M_{\min}$
When $M_{\min} < \text{len}(\text{gangPods})$, do NOT sum all pods in `gangPods`. 
Instead, extract the **optimistic minimal resource vector** of size $M_{\min}$:
1. For each scalar resource $r$, sort the pod requests in ascending order:
   $$R_1(r) \le R_2(r) \le \dots \le R_T(r)$$
2. Sum only the smallest $M_{\min}$ requests:
   $$R_{\min\text{-required}}(r) = \sum_{i=1}^{M_{\min}} R_i(r)$$
3. Fast-fail if and only if:
   $$R_{\text{available}}(r) < R_{\min\text{-required}}(r)$$

For KEP-6012 hierarchical composite gangs, partition the sum into mandatory sub-groups ($M_{\text{mandatory}} = \text{totalMember}$) plus elastic worker sub-groups.

### 7.3 Remediation 3: CycleState Reservation & Optimistic Concurrency Control
To prevent concurrent preemption capacity racing (Failure Mode 3):
1. Introduce a cycle-scoped `PreemptionReservationTracker` in `CycleState`.
2. When Gang A passes Idea 15 and enters Layer 2/3, record a soft reservation on candidate node capacities.
3. Subsequent preemption cycles in parallel workers must deduct active soft reservations from available headroom:
   $$\text{EffectiveAvailable}_n(r) = \text{Available}_n(r) - \text{ReservedPreemption}_n(r)$$
4. If a gang hits a permit timeout or fails binding, release reservations immediately.

### 7.4 Remediation 4: Zero-Allocation Scalar Array Accumulators
Eliminate dynamic map allocations (`make(map[v1.ResourceName]int64)`):
1. Pre-map well-known scalar resources (CPU, Memory, Ephemeral Storage, `nvidia.com/gpu`) to fixed integer indices in an inline array `[8]int64`.
2. Compute pod requests once during gang intake and cache them on the gang struct, rather than invoking `resource.PodRequests(p)` repeatedly inside the preemption cycle.

### 7.5 Remediation 5: $\mathcal{O}(1)$ Incremental Slot Tracking in Branch-and-Bound
In `BranchAndBoundGangSearch`:
1. Remove the $\mathcal{O}(|\mathcal{V}|)$ candidate scanning loop from the recursive search function.
2. Initialize `totalAvailableSlots` before starting search recursion.
3. When assigning a pod to node $n$, decrement `totalAvailableSlots` by 1.
4. Check suffix feasibility in $\mathcal{O}(1)$ time:
   $$\text{if } \text{totalAvailableSlots} < (M_{\min} - \text{placedPods}) \implies \text{Prune Branch}$$

---

## 8. Summary Audit Conclusion & Final Sign-Off

### 8.1 Final Verdict
**REJECT IN CURRENT FORM (PR #123) / MUST BE GATED BEHIND FEATURE FLAG `FeatureGateGangQuorumEarlyExit=false` UNTIL REMEDIATIONS 1 THROUGH 5 ARE IMPLEMENTED.**

While the high-level concept of pruning hopelessly unschedulable gangs in $\mathcal{O}(1)$ time is theoretically sound, the PR #123 implementation of Idea 15 is critically flawed. By naively assuming a 1-to-1 relationship between pods and nodes, it introduces severe false negatives that immediately break multi-tenant GPU packing and container co-location. By summing all pods in elastic gangs, it breaks KEP-5710 and KEP-6012. By lacking concurrency isolation, it exacerbates victim thrashing under cluster load. And by executing an un-memoized loop inside Branch-and-Bound, it degrades search performance by up to 2.34x.

Until Remediations 1 through 5 are fully implemented and verified against the integration test suite, Idea 15 must not be enabled in production environments.

---

**Report Filed By:** Task 3712 / Auditor Red-Team  
**Origin:** Task 984  
**Target Repository:** `bwsalmon/kubernetes` (`bsalmon-preempt`)  
**Commit Reference:** `dfae0410419` / PR #123
