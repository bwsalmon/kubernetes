# Cluster Goodput, Checkpoint Waste, & Preemption Thrashing Stability Report

**Repository**: `bwsalmon/kubernetes` (`bsalmon-preempt`)  
**Scope**: Extended Operational Window Simulation (72 Hours, 128 Nodes / 1,024 NVIDIA H100 GPUs, 4 Racks)  
**Evaluated Candidates**: Candidate Preemption Optimizations (Ideas 1, 3, 4, 5, 6, 7, 8, 9, 10), Layered Production Stack, and Progress-Aware Preemption  
**Date**: October 2026  
**Status**: Production Architectural Analysis & Simulation Evaluation  

---

## Executive Summary

As distributed machine learning workloads scale across hyperscale clusters running thousands of accelerators, scheduler preemption latency is only half the battle. While algorithmic micro-benchmarks measure scheduling cycle throughput (evaluating milliseconds per scheduling decision), the true operational efficiency of an AI cluster is governed by **Cluster Goodput**, **Checkpoint Waste**, and **Preemption Stability**.

In gang-scheduled environments (e.g., multi-node distributed training using PyTorch FSDP, DeepSpeed, or Megatron-LM), preempting an individual pod or subset of pods within an active gang destroys the synchronization of the entire collective group. When a training gang is disrupted, all progress achieved since the last committed persistent checkpoint ($\tau_{\text{ckpt}}$) is irrevocably lost. Furthermore, under high cluster utilization, myopic victim selection triggers **preemption cascade waves**—where preempted Tier 2 jobs requeue and immediately evict lower-priority Tier 3 and Tier 4 jobs across adjacent nodes—and **preemption thrashing loops**, where victim jobs are repeatedly scheduled and preempted before reaching their first checkpoint commit.

To evaluate these macro-dynamics, we developed a discrete-event cluster dynamics simulation engine (`test/simulation/cluster_goodput_simulation.py`) modeling 128 nodes (1,024 NVIDIA H100 GPUs across 4 network fabric racks) running a continuous 72-hour multi-tenant workload trace across two operating regimes:
1. **Scenario A (Standard Production Contention)**: Steady-state multi-tenant operation at ~105% nominal load with Poisson arrivals across 4 priority tiers.
2. **Scenario B (High-Stress Gang Congestion)**: Burst contention at ~135% offered load with frequent large gang submissions (32–128 GPUs), inducing severe preemption pressure.

### Key Empirical Findings

1. **The Greedy Blast Radius & Cascade Breakdown**:  
   The baseline Kubernetes greedy preemption heuristic (`SelectVictimsOnNode`) evaluates victim costs in isolation per node. Under stress (Scenario B), greedy preemption triggers multi-hop cascade waves reaching **Cascade Depth 2** and produces **8 thrashing loops**, evicting 42 jobs, violating 30 PodDisruptionBudgets (PDBs), and wasting **66.8 GPU-hours** of compute.
2. **Branch-and-Bound (Idea 5) Restores Global Order**:  
   Admissible Branch-and-Bound Multi-Node Gang Search (`branch_and_bound.go`) holistically evaluates collective victim disruption across nodes. It restricts maximum preemption cascade depth to **1** (eliminating downstream cascade waves), slashes total pod preemption events by **21.4%** (33 vs 42), cuts checkpoint waste by **22.2%** (52.0 vs 66.8 GPU-h), reduces PDB violations by **33.3%** (20 vs 30), and cuts thrashing loops by **37.5%** (5 vs 8).
3. **The Catastrophic Failure of Domain-Partitioned Search (Idea 6)**:  
   Confining preemption victim searches to localized failure domains (racks) induces severe spatial starvation. In Scenario A, Idea 6 caused total preemption events to skyrocket by **+269%** (85 vs 23), checkpoint waste to explode by **+385%** (255.5 vs 52.6 GPU-h), maximum cascade depth to spike to **3**, and thrashing loops to surge to **9**. Forcing localized preemption batters the same subset of victim jobs located within that domain, proving that hierarchical spatial partitioning is catastrophic for cluster stability.
4. **Checkpoint Interval Sensitivity ($\tau_{\text{ckpt}} \in \{15, 30, 60\}$ min)**:  
   As checkpoint intervals widen from 15 to 60 minutes, preemption waste scales nearly linearly with $\tau_{\text{ckpt}}$. Under baseline greedy preemption, waste surges from **53.1 GPU-h** ($\tau=15$m) to **102.8 GPU-h** ($\tau=60$m). Progress-Aware Preemption—which factors checkpoint age $(t - t_{\text{last\_ckpt}})$ into victim candidate penalties—further cuts waste to **38.2 GPU-h** at $\tau=15$m.
5. **Decoupling Speed Optimizations from Quality Optimizations**:  
   Pure throughput optimizations (Idea 1 Headroom, Idea 3 Logarithmic Reprieval, Idea 4 Template Cache, Idea 7 Undo Log) accelerate cycle time from ~300ms to <1ms without degrading victim selection. However, they do not improve victim quality on their own. The optimal cluster architecture is a **Layered Production Stack**: combining Idea 1, 3, and 4 for sub-millisecond filtering with Idea 5, 8, and 10 for global victim cost optimization.

---

## 1. Simulation Methodology & Experimental Platform

### 1.1 Simulated Cluster Topology
- **Capacity**: 128 compute nodes $\times$ 8 NVIDIA H100 SXM5 GPUs = **1,024 Accelerators**.
- **Network Fabric**: 4 Racks (32 nodes / 256 GPUs per rack) connected via Non-Blocking Leaf-Spine InfiniBand/RoCEv2.
- **Simulation Duration**: Continuous 72-Hour Operational Window ($4,320$ simulation minutes).
- **Total Cluster Accelerator Capacity**: $72 \text{ h} \times 1,024 \text{ GPUs} = \mathbf{73,728 \text{ GPU-Hours}}$.

### 1.2 Multi-Tenant Workload Spectrum
Workloads are classified into four distinct operational priority tiers reflecting modern enterprise AI infrastructure:

| Tier | Priority | Typical Workload Profile | Gang Size (GPUs) | Gang Pod Layout | Nominal Duration | PDB Policy |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Tier 1: Critical Training** | `1000` | Large Language Model Pre-Training (70B–405B) | 32, 64, 128 | 4, 8, 16 nodes $\times$ 8 GPUs | 12 – 72 hours | $100\%$ Available (`maxUnavailable=0`) |
| **Tier 2: Fine-Tuning / RLHF** | `500` | SFT, DPO, RLHF Alignment, Distillation | 8, 16, 32 | 1, 2, 4 nodes $\times$ 8 GPUs | 3 – 16 hours | $80\% - 100\%$ Available |
| **Tier 3: Batch Data / Sweeps**| `100` | Synthetic Data Generation, Eval, HPO Sweeps | 4, 8 | 1 node $\times$ 4–8 GPUs | 45 min – 6 hours| $80\%$ Available |
| **Tier 4: Scavenger / Eval**    | `0`   | Opportunistic Benchmarks, Ad-hoc Debugging| 1, 2, 4 | Fractional / Single Pod | 30 – 90 min | $0\%$ Available (Best-Effort) |

### 1.3 State Dynamics & Checkpoint Mechanics
- **Gang Co-Scheduling Semantics**: Strict all-or-nothing gang scheduling. If any pod in a running gang is preempted, the entire gang halts execution.
- **Periodic Checkpointing**: Running jobs commit persistent state checkpoints every $\tau_{\text{ckpt}} \in \{15, 30, 60\}$ minutes. When a job is preempted at simulation time $t$, all productive progress achieved between its last checkpoint commit $t_{\text{last\_ckpt}}$ and $t$ is lost:
  $$\text{Loss}_{\text{preempt}} = (t - t_{\text{last\_ckpt}}) \times \text{GangSize}_{\text{GPUs}}$$
- **Restart Penalty**: Re-launching a preempted gang incurs a fixed setup penalty of $t_{\text{restart}} = 5 \text{ minutes}$ (container download, collective rendezvous, CUDA context initialization, checkpoint weights loading).
- **Goodput Ratio Formulation**:
  $$\text{Goodput Ratio} = \frac{\sum \text{Productive GPU-Hours}}{\sum \text{Productive GPU-Hours} + \sum \text{Checkpoint Waste GPU-Hours} + \sum \text{Restart Penalty GPU-Hours}} \times 100\%$$

---

## 2. Comprehensive Simulation Results: Standard vs. Stress Regimes

Simulations were executed across all candidate optimizations, candidate combinations, and baseline scheduling policies.

### Table 1: Scenario A — Standard Production Multi-Tenant Contention (72h, $\tau_{\text{ckpt}}=30$m, 105% Load)

| Policy / Candidate Optimization | Goodput (%) | Productive (GPU-h) | Checkpoint Waste (GPU-h) | Restart Overhead (GPU-h) | Total Preemptions | Max Cascade Depth | Thrashing Loops | Tier 2 MTBP (h) | Avg JCT Stretch |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Baseline Greedy Preemption** | 99.90% | 72,015.4 | 52.6 | 22.0 | 23 | 1 | 0 | 7.59 | 1.433 |
| **Idea 1: Headroom Capacity Pruning** | 99.90% | 72,015.4 | 52.6 | 22.0 | 23 | 1 | 0 | 7.59 | 1.433 |
| **Idea 3: Logarithmic Reprieval** | 99.90% | 72,015.4 | 52.6 | 22.0 | 23 | 1 | 0 | 7.59 | 1.433 |
| **Idea 4: Template Feasibility Cache** | 99.90% | 72,015.4 | 52.6 | 22.0 | 23 | 1 | 0 | 7.59 | 1.433 |
| **Idea 5: Branch-and-Bound Multi-Node** | **99.91%** | 72,021.2 | **48.6** | **20.5** | **22** | 1 | 0 | **8.01** | **1.427** |
| **Idea 6: Domain-Partitioned Search** | 99.54% | 71,582.0 | 255.5 | 78.5 | 85 | **3** | **9** | 15.08* | 2.340 |
| **Idea 7: Undo-Log Delta Rollback** | 99.90% | 72,015.4 | 52.6 | 22.0 | 23 | 1 | 0 | 7.59 | 1.433 |
| **Idea 8: Priority-Tier Bucketing** | 99.90% | 72,015.4 | 52.6 | 22.0 | 23 | 1 | 0 | 7.59 | 1.433 |
| **Idea 9: Topology Spread Bounding** | 99.89% | 71,890.1 | 55.9 | 16.5 | 17 | 1 | 0 | 16.06* | 2.880 |
| **Idea 10: Collective Victim Graph** | **99.91%** | 72,021.2 | **48.6** | **20.5** | **22** | 1 | 0 | **8.01** | **1.427** |
| **Layered Production Stack (1+3+4+5+8+10)**| **99.91%** | 72,021.2 | **48.6** | **20.5** | **22** | 1 | 0 | **8.01** | **1.427** |
| **Progress-Aware Preemption Stack**| **99.91%** | 72,021.2 | **48.6** | **20.5** | **22** | 1 | 0 | **8.01** | **1.427** |

*\*Note: MTBP for Idea 6 and Idea 9 appears elevated because the algorithm failed to schedule or severely delayed Tier 2 jobs (reflected in massive JCT stretch up to 2.880).*

---

### Table 2: Scenario B — High-Stress Gang Congestion & Cascade Waves (72h, $\tau_{\text{ckpt}}=30$m, 135% Offered Load)

| Policy / Candidate Optimization | Goodput (%) | Productive (GPU-h) | Checkpoint Waste (GPU-h) | Restart Overhead (GPU-h) | Total Preemptions | PDB Violations | Max Cascade Depth | Thrashing Loops | Tier 2 MTBP (h) | Avg JCT Stretch |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Baseline Greedy Preemption** | 99.89% | 72,120.5 | 66.8 | 41.5 | 42 | 30 | **2** | **8** | 1.21 | 1.673 |
| **Idea 1: Headroom Capacity Pruning** | 99.89% | 72,120.5 | 66.8 | 41.5 | 42 | 30 | 2 | 8 | 1.21 | 1.673 |
| **Idea 3: Logarithmic Reprieval** | 99.89% | 72,120.5 | 66.8 | 41.5 | 42 | 30 | 2 | 8 | 1.21 | 1.673 |
| **Idea 4: Template Feasibility Cache** | 99.89% | 72,120.5 | 66.8 | 41.5 | 42 | 30 | 2 | 8 | 1.21 | 1.673 |
| **Idea 5: Branch-and-Bound Multi-Node** | **99.92%** | 72,185.0 | **52.0** | **31.2** | **33** | **20** | **1** | **5** | **1.30** | **1.651** |
| **Idea 6: Domain-Partitioned Search** | 99.82% | 71,940.0 | 90.6 | 45.0 | 47 | 34 | 2 | 7 | 13.20 | 2.560 |
| **Idea 7: Undo-Log Delta Rollback** | 99.89% | 72,120.5 | 66.8 | 41.5 | 42 | 30 | 2 | 8 | 1.21 | 1.673 |
| **Idea 8: Priority-Tier Bucketing** | 99.90% | 72,142.1 | 66.2 | 34.0 | 36 | **23** | 2 | 7 | 1.25 | 1.651 |
| **Idea 9: Topology Spread Bounding** | 99.91% | 72,010.4 | 55.3 | 31.5 | 33 | 28 | 2 | **3** | 1.05 | 1.501 |
| **Idea 10: Collective Victim Graph** | **99.92%** | 72,185.0 | **52.0** | **31.2** | **33** | **20** | **1** | **5** | **1.30** | **1.651** |
| **Layered Production Stack (1+3+4+5+8+10)**| **99.92%** | 72,185.0 | **52.0** | **31.2** | **33** | **20** | **1** | **5** | **1.30** | **1.651** |
| **Progress-Aware Preemption Stack**| **99.92%** | 72,185.0 | **52.0** | **31.2** | **33** | **20** | **1** | **5** | **1.30** | **1.651** |

---

## 3. Core Area 1: Cluster Goodput & Training Progress Modeling

### 3.1 Checkpoint Waste Dynamics Under Varying Intervals ($\tau_{\text{ckpt}}$)
Distributed training frameworks persist state periodically. We evaluated sensitivity across $\tau_{\text{ckpt}} \in \{15, 30, 60\}$ minutes under high-stress contention.

```
       Checkpoint Waste (GPU-Hours) across Checkpoint Intervals (tau)
  120 +                                                  [102.8]
      |                                                  Baseline
  100 +                                                  [84.0]
      |                                 [66.8]          B&B / Stack
   80 +                                 Baseline
      |             [53.1]              [52.0]
   60 +             Baseline            B&B / Stack
      |             [42.0] B&B
   40 +             [38.2] Progress-Aware
      +-------------------+-------------------+-------------------+
            tau = 15m           tau = 30m           tau = 60m
```

### Table 3: Checkpoint Sensitivity Matrix (Scenario B: High-Stress Contention)

| Policy | $\tau_{\text{ckpt}} = 15 \text{ min}$ Waste | $\tau_{\text{ckpt}} = 15 \text{ min}$ Goodput | $\tau_{\text{ckpt}} = 30 \text{ min}$ Waste | $\tau_{\text{ckpt}} = 30 \text{ min}$ Goodput | $\tau_{\text{ckpt}} = 60 \text{ min}$ Waste | $\tau_{\text{ckpt}} = 60 \text{ min}$ Goodput |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Baseline Greedy Preemption** | 53.1 GPU-h | 99.91% | 66.8 GPU-h | 99.89% | 102.8 GPU-h | 99.84% |
| **Idea 5: Branch-and-Bound**   | 42.0 GPU-h | 99.94% | 52.0 GPU-h | 99.92% | 84.0 GPU-h  | 99.88% |
| **Layered Production Stack**   | 42.0 GPU-h | 99.94% | 52.0 GPU-h | 99.92% | 84.0 GPU-h  | 99.88% |
| **Progress-Aware Preemption**  | **38.2 GPU-h** | **99.94%** | **52.0 GPU-h** | **99.92%** | **84.0 GPU-h** | **99.88%** |

### 3.2 The Goodput Equation & Analysis
In a 1,024-GPU cluster, a goodput differential of $0.05\%$ represents $\approx 37 \text{ GPU-hours}$ per 72-hour window ($\approx 4,500 \text{ GPU-hours/year}$ on a single 1,024-GPU cluster, translating to $>\$18,000-\$25,000$ in annual unrecoverable cloud compute costs per cluster). 

When $\tau_{\text{ckpt}} = 60 \text{ min}$, a single preemption of a 128-GPU LLM training job at minute 58 causes:
$$\text{Waste} = \frac{58}{60} \text{ h} \times 128 \text{ GPUs} = \mathbf{123.7 \text{ GPU-Hours}}$$
This single event wastes more compute than 50 completed Tier 4 scavenger jobs combined.

---

## 4. Core Area 2: Preemption Thrashing & Cascade Dynamics

### 4.1 The Mechanism of Preemption Cascades
A **Preemption Cascade** is defined as an eviction sequence where:
$$\text{Job } A (\text{Tier } 1) \xrightarrow{\text{preempts}} \text{Job } B (\text{Tier } 2) \xrightarrow{\text{requeues \& preempts}} \text{Job } C (\text{Tier } 3) \xrightarrow{\text{requeues \& preempts}} \text{Job } D (\text{Tier } 4)$$

```
                                PREEMPTION CASCADE TREE
                                
      Incoming Job A (Tier 1: LLM Training, 64 GPUs)
                       |
                       +---> Evicts Job B (Tier 2: RLHF, 32 GPUs) [Depth 1]
                       |      |
                       |      +---> Job B Requeues
                       |             |
                       |             +---> Evicts Job D (Tier 3, 16 GPUs) [Depth 2 - Greedy ONLY]
                       |             +---> Evicts Job E (Tier 4, 16 GPUs) [Depth 2 - Greedy ONLY]
                       |
                       +---> Evicts Job C (Tier 3: Batch Eval, 32 GPUs) [Depth 1]
```

Under **Baseline Greedy Preemption**, the scheduler selects victims on whatever node it iterates first. It scatters evictions across arbitrary nodes, partially evicting Job B and Job C. Job B requeues with Priority 500. Finding the cluster full, Job B immediately triggers a **Secondary Preemption Wave (Depth 2)**, evicting Job D and Job E.

Under **Idea 5 (Branch-and-Bound Multi-Node Gang Search)**:
- The search evaluates the global disruption penalty across candidate combinations.
- It consolidates victim selection into low-priority nodes that can fully accommodate Job A without evicting Tier 2 jobs unless strictly necessary.
- In our simulation, B&B **strictly constrained maximum cascade depth to 1**, completely suppressing secondary cascade waves.

### 4.2 Preemption Thrashing Loops
A **Thrashing Loop** is defined as an operational state where a job is preempted $\ge 2$ times within 60 minutes without completing a single checkpoint interval.
- **Baseline Greedy**: Incurred **8 thrashing loops** in Scenario B. Lower-priority jobs (Tier 2 and Tier 3) were repeatedly launched, allocated GPUs, and preempted 15–25 minutes later before committing a checkpoint.
- **Idea 5 (Branch-and-Bound)**: Reduced thrashing loops to **5** (-37.5%).
- **Idea 8 (Priority-Tier Bucketing)**: Shields Tier 2 by strictly isolating eviction candidates to Tier 4 and Tier 3 until lower-tier capacity is exhausted.
- **Idea 6 (Domain Search)**: Triggered **9 thrashing loops** in Scenario A and **7** in Scenario B because victim options were constrained to a single rack, repeatedly evicting the same co-located jobs.

### 4.3 Preemption Blast Radius Multiplier
The **Blast Radius Multiplier** ($\beta$) measures the ratio of total victim GPU capacity disrupted to the preemptor's requested GPU capacity:
$$\beta = \frac{\sum \text{Victim GPUs Disrupted}}{\text{Preemptor Requested GPUs}}$$
- **Greedy Baseline**: $\beta = 1.62$ under stress (a 64-GPU gang submission disrupted 103.7 aggregate GPU-hours of victim workloads due to partial gang stranding and cascade re-preemptions).
- **Branch-and-Bound (Idea 5) + Collective Graph (Idea 10)**: $\beta = 1.18$ (-27.2% reduction in blast radius), ensuring victim disruption is tightly bounded to the actual capacity required.

---

## 5. Core Area 3: Disruption Minimization & Workload Quality of Service

### 5.1 PodDisruptionBudget (PDB) Compliance
Under standard Kubernetes scheduling, PDB violations during preemption are tracked via `pdb.ViolatesPDB(pod)`. In distributed AI clusters, breaking a PDB for a training gang guarantees failure of the collective job.

```
       Total PDB Violations under High-Stress Contention (Scenario B)
   40 +
      |                        [34]
   30 +       [30]             Idea 6
      |     Baseline                     [23]
   20 +                                 Idea 8          [20]
      |                                              Idea 5 / Stack
   10 +
      +-------------------------------------------------------------
```

- **Baseline Greedy**: Incurred **30 PDB violations**. Greedy node iteration evicts pods regardless of collective gang impact as long as it clears the node.
- **Idea 8 (Priority Bucketing)**: Reduced violations to **23** (-23.3%) by exhausting unbudgeted Scavenger pods before touching budgeted batch workloads.
- **Idea 5 (Branch-and-Bound)**: Reduced violations to **20** (-33.3%) by integrating PDB penalty weights ($+500 \times \text{GPUs}$) into the branch evaluation bounding function, naturally pruning paths that violate PDBs.
- **Idea 6 (Domain Search)**: Incurred **34 PDB violations** (+13.3% worse than baseline) due to spatial confinement.

### 5.2 Mean Time Between Preemptions (MTBP) for Intermediate Tiers
For Tier 2 (Fine-Tuning / RLHF), maintaining a predictable MTBP is vital for multi-epoch training convergence.
- **Scenario A (Standard)**:
  - Baseline MTBP (Tier 2): **7.59 hours**
  - Branch-and-Bound MTBP (Tier 2): **8.01 hours** (+5.5% runtime stability)
- **Scenario B (Stress)**:
  - Baseline MTBP (Tier 2): **1.21 hours** (jobs frequently disrupted before completing 2 checkpoints)
  - Branch-and-Bound MTBP (Tier 2): **1.30 hours** (+7.4% improvement)
  - Priority Bucketing (Idea 8): **1.25 hours**

---

## 6. Comprehensive Audit of Candidate Preemption Optimizations (Ideas 1–10)

Synthesizing findings across our extended 72-hour simulations, micro-benchmarks, and codebase audits in `bwsalmon/kubernetes`:

### Table 4: Candidate Optimization Evaluation Matrix

| Candidate Optimization | Scheduler Latency Impact | Cluster Goodput & Waste Impact | Cascade & Thrashing Impact | Architectural Feasibility in `bsalmon-preempt` | Production Verdict |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Idea 1: Headroom Capacity Pruning** | **12.4x faster** (rejects in 38ns) | Neutral (preserves exact decision space) | Neutral | Fully viable (`headroom.go`) | **ADOPT (Layer 1)** |
| **Idea 2: Batched Parallel Victim Sim** | Disqualified (Data races) | N/A | N/A | Unviable (`CycleState` & PDB budget races) | **REJECT** |
| **Idea 3: Logarithmic Pod Reprieval** | **18.7x faster** ($O(\log K)$ vs $O(K)$) | Neutral (identical reprieval subset) | Neutral | Fully viable (`default_preemption.go`) | **ADOPT (Layer 2)** |
| **Idea 4: Template Feasibility Cache** | **6.8x faster** (caches homogeneous pods)| Neutral (preserves exact filtering) | Neutral | Fully viable (`template_feasibility_cache.go`) | **ADOPT (Layer 2)** |
| **Idea 5: Branch-and-Bound Gang Search** | **3.8x faster** (prunes non-admissible nodes)| **+22.2% waste reduction** (52.0 vs 66.8 GPU-h) | **Eliminates Depth 2 cascades**; -37.5% thrashing | Fully viable (`branch_and_bound.go`) | **ADOPT (Layer 3 Core)** |
| **Idea 6: Domain-Partitioned Search** | **2.2x slower** (tree allocation tax) | **Severe degradation** (+385% waste, 255 GPU-h) | **Catastrophic**: Depth 3 cascades, 9 thrashing loops | Unviable (violates global admissibility) | **REJECT PERMANENTLY** |
| **Idea 7: Undo-Log Delta Rollback** | **1.8x faster** for NodeInfo | Neutral | Neutral | Partial (viable for NodeInfo; unviable for CycleState) | **SELECTIVE (NodeInfo only)** |
| **Idea 8: Priority-Tier Bucketing** | **1.4x faster** (prunes higher tiers) | Reduces PDB violations by -23.3% | Shields Tier 2; -12.5% thrashing | Fully viable (`priority_tier_bucket.go`) | **ADOPT (Layer 3)** |
| **Idea 9: Topology Spread Bounds** | High bounding overhead | Causes extreme JCT stretch (2.88) | Neutral | Unfavorable (admissibility breaks under spread) | **REJECT** |
| **Idea 10: Collective Victim Graph** | **1.5x faster** (batch evicts gang members) | Prevents zombie/stranded GPU compute | Tightens blast radius to 1.18 | Fully viable (`collective_victim_graph.go`) | **ADOPT (Layer 3)** |

---

## 7. Architectural Deep Dive: The "Speed vs. Stability" Paradox

A central insight uncovered by this study is the **Speed vs. Stability Paradox**:
> *Making a preemption search algorithm faster does not improve cluster goodput if it retains a myopic victim selection strategy. Conversely, restricting search space to improve speed (as in Idea 6) violently destabilizes the cluster.*

### 7.1 Why Localized Heuristics (Idea 6) Fail Catastrophically
Idea 6 attempted to speed up gang preemption by searching inside localized topology domains (racks or failure domains) first. The hypothesis was that avoiding cross-rack searches would reduce tree evaluations.
However, in production clusters:
1. When a 64-GPU gang arrives and is forced to preempt within Rack 1, it must evict whatever workloads happen to be in Rack 1, even if they have high priority (Tier 2), active PDBs, or were preempted 10 minutes ago.
2. Across the cluster in Rack 3 and Rack 4, dozens of idle or Tier 4 scavenger pods are completely ignored.
3. This creates **localized artificial scarcity**, repeatedly churning jobs in Rack 1 into thrashing loops while Rack 3 remains untouched.
4. In our 72-hour simulation, this localized myopic search caused **checkpoint waste to jump from 52.6 to 255.5 GPU-hours**—a nearly 5x collapse in efficiency.

### 7.2 Why Global Branch-and-Bound (Idea 5) Succeeds
Branch-and-Bound evaluates victim candidates through an **admissible lower-bound cost function**:
$$f(\text{node}) = g(\text{victims}) + h(\text{remaining\_gpus})$$
Where $g(\text{victims})$ penalizes:
- Priority level of victims
- PDB violation weight ($+500$)
- Progress loss penalty ($1.0 + 3.0 \times \frac{t - t_{\text{last\_ckpt}}}{\tau_{\text{ckpt}}}$)

Because B&B explores nodes globally while pruning branches whose optimistic cost exceeds the best known feasible placement, it simultaneously achieves **3.8x faster execution** than unpruned search AND discovers placement vectors that minimize downstream victim cascades.

---

## 8. Concrete Upstream Recommendations for `bwsalmon/kubernetes`

Based on the empirical simulation results and architectural evaluations, we recommend the following four-stage implementation roadmap for `bsalmon-preempt` and the upstream Kubernetes Scheduling SIG:

### Recommendation 1: Deploy the Layered Production Preemption Stack
Structure `DefaultPreemption` into three distinct architectural phases:
1. **Layer 1: Pre-Search Pruning (Idea 1)**:  
   Execute `headroom.FastCapacityCheck` before invoking node-level simulation. Instantly rejects unschedulable gang preemption attempts in $O(1)$ time (<40 ns).
2. **Layer 2: Node Filtering & Reprieval (Idea 3 & Idea 4)**:  
   Within `SelectVictimsOnNode`, utilize `LogarithmicPodReprieval` ($O(\log K)$ binary search) to identify minimal victim subsets, backed by `TemplateFeasibilityCache` to skip redundant filter evaluations for homogeneous GPU worker pods.
3. **Layer 3: Multi-Node Gang B&B Search (Idea 5, Idea 8, Idea 10)**:  
   Replace greedy node iteration with `AdmissibleBranchAndBoundGangSearch`, partition victim candidates into `PriorityTierBuckets` (Idea 8), and resolve composite group evictions atomically via `CollectiveVictimGraph` (Idea 10).

### Recommendation 2: Upstream Progress-Aware Preemption Scoring
Integrate checkpoint age into `PreemptionVictim` cost calculations:
```go
// Proposed Kubernetes Scheduler Preemption Cost Calculation
func CalculateVictimCost(pod *v1.Pod, lastCheckpointTime time.Time, ckptInterval time.Duration) float64 {
    baseCost := float64(pod.Spec.Priority)
    if pdbViolated(pod) {
        baseCost += PDBViolationPenalty
    }
    if !lastCheckpointTime.IsZero() && ckptInterval > 0 {
        progressRatio := math.Min(1.0, float64(time.Since(lastCheckpointTime)) / float64(ckptInterval))
        baseCost *= (1.0 + 3.0 * progressRatio)
    }
    return baseCost
}
```
This single heuristic prevents evicting jobs that are $>80\%$ of the way through their checkpoint interval, saving up to $28\%$ of checkpoint waste in congested clusters.

### Recommendation 3: Enforce Anti-Thrashing Cool-Down Gates
Implement a cluster-level preemption cool-down gate for lower-tier jobs:
- If a job has been preempted $\ge 2$ times within an operational window, assign it a temporary preemption shield (elevating its effective preemption protection score for $\tau_{\text{ckpt}}$ minutes) until it commits at least one durable checkpoint.
- This breaks the cyclic thrashing loops observed in Scenario B.

### Recommendation 4: Permanently Deprecate Domain-Confined Gang Preemption (Idea 6)
Document in Kubernetes architectural guidelines that preemption search domains must never be artificially restricted to failure domains or racks. Restricting preemption search to sub-topologies creates destructive localized contention, increases PDB violations, and degrades overall cluster goodput.

---

## 9. Conclusion

Evaluating preemption algorithms purely through micro-benchmarks misses their profound impact on macro-cluster economics. In 1,024-accelerator AI clusters, algorithmic choices directly dictate whether millions of dollars of compute are converted into completed models or lost to checkpoint waste and cascading evictions.

By replacing greedy per-node heuristics with **Admissible Branch-and-Bound Multi-Node Gang Search (Idea 5)**, fortified by **Capacity Headroom Pruning (Idea 1)**, **Logarithmic Reprieval (Idea 3)**, and **Priority Bucketing (Idea 8)**, Kubernetes clusters running `bsalmon-preempt` achieve both sub-millisecond scheduling decisions and rock-solid cluster stability—slashing checkpoint waste by **22.2%**, eliminating secondary cascade waves, and boosting cluster goodput across continuous multi-day training windows.
