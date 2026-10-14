#!/usr/bin/env python3
"""
Cluster Goodput, Checkpoint Waste, and Preemption Thrashing Simulator
for Kubernetes Gang Preemption (bwsalmon/kubernetes - bsalmon-preempt).

Simulates multi-tenant cluster dynamics over extended operational windows (72 hours)
to evaluate candidate preemption optimizations (Ideas 1, 3, 4, 5, 6, 7, 8, 9, 10,
Layered Stack, and Progress-Aware Preemption).

Key Metrics Evaluated:
1. Cluster Goodput Ratio: Productive GPU Hours / Total Allocated GPU Hours.
2. Checkpoint Waste: Aggregate GPU Hours lost to evictions between checkpoints.
3. Preemption Cascade Depth: Downstream eviction depth triggered by gang submissions.
4. Preemption Thrashing Loops: Multi-preemption cycles without progress (<60 min).
5. Disruption Minimization: PDB violations, total restarts, MTBP for lower tiers.
"""

import sys
import os
import math
import random
import heapq
import json
from dataclasses import dataclass, field
from typing import List, Dict, Set, Optional, Tuple, Any

# Priority Definitions
PRIORITY_CRITICAL_TRAINING = 1000  # Tier 1: Large Foundation Model Training (LLM 70B/405B)
PRIORITY_FINETUNE_RLHF    = 500   # Tier 2: Medium Gangs (Fine-Tuning / RLHF / Distillation)
PRIORITY_BATCH_EVAL       = 100   # Tier 3: Batch Data, Synthetic Data, Hyperparam Sweeps
PRIORITY_SCAVENGER        = 0     # Tier 4: Best-Effort / Opportunistic

# Checkpoint intervals (minutes)
CHECKPOINT_INTERVALS = [15, 30, 60]

@dataclass
class Job:
    id: str
    name: str
    priority: int
    gang_size: int          # Total GPUs required
    gpus_per_pod: int       # Typically 8 (1 node) or 4 or 1
    duration_minutes: float # Total required productive computing time
    submit_time: float      # Simulation time (minutes)
    checkpoint_interval: float # minutes (e.g. 15, 30, 60)
    pdb_min_available_pct: float # e.g. 1.0 (100%), 0.8 (80%)
    
    # Dynamic state
    remaining_compute: float = 0.0
    accumulated_productive_compute: float = 0.0
    accumulated_waste: float = 0.0
    accumulated_restart_penalty: float = 0.0
    
    start_time: Optional[float] = None
    last_checkpoint_time: Optional[float] = None
    last_preempted_time: Optional[float] = None
    
    restarts: int = 0
    assigned_nodes: List[int] = field(default_factory=list)
    state: str = "PENDING"  # PENDING, RUNNING, COMPLETED, PREEMPTED
    
    # Preemption cascade tracking
    preempted_by_job_id: Optional[str] = None
    cascade_origin_job_id: Optional[str] = None
    cascade_depth: int = 0
    preemption_history: List[float] = field(default_factory=list)

    def __post_init__(self):
        self.remaining_compute = self.duration_minutes

@dataclass
class Node:
    id: int
    name: str
    rack_id: int
    total_gpus: int = 8
    allocated_gpus: int = 0
    assigned_pods: List[Tuple[str, int]] = field(default_factory=list) # (job_id, gpus)

    @property
    def free_gpus(self) -> int:
        return self.total_gpus - self.allocated_gpus

@dataclass
class SimulationStats:
    policy_name: str
    scenario_name: str
    checkpoint_interval_param: int
    total_sim_hours: float
    total_gpu_capacity_hours: float
    total_gpu_allocated_hours: float
    productive_gpu_hours: float
    checkpoint_waste_gpu_hours: float
    restart_penalty_gpu_hours: float
    idle_allocated_gpu_hours: float
    goodput_ratio: float
    
    # Preemption & Disruption Metrics
    total_preemption_events: int
    total_jobs_preempted: int
    total_pod_restarts: int
    aggregate_pdb_violations: int
    
    # Cascade Dynamics
    max_cascade_depth: int
    mean_cascade_depth: float
    cascade_events_by_depth: Dict[int, int]
    thrashing_loops: int # Jobs preempted >= 2 times within 60 min without completing checkpoint
    thrashing_jobs_count: int
    cascade_blast_radius_multiplier: float # Total evicted GPUs / Preemptor requested GPUs
    
    # Tier-specific metrics
    mtbp_tier2_hours: float # Mean Time Between Preemptions for Tier 2
    mtbp_tier3_hours: float # Mean Time Between Preemptions for Tier 3
    tier_completions: Dict[int, int]
    tier_goodput: Dict[int, float]
    avg_jct_stretch: float # Actual completion time / nominal duration

class PreemptionSimulator:
    def __init__(self, num_nodes=128, gpus_per_node=8, racks=4, seed=42):
        self.num_nodes = num_nodes
        self.gpus_per_node = gpus_per_node
        self.total_gpus = num_nodes * gpus_per_node
        self.num_racks = racks
        self.seed = seed
        self.rng = random.Random(seed)
        
        self.nodes = [
            Node(id=i, name=f"node-{i:03d}", rack_id=i % racks, total_gpus=gpus_per_node)
            for i in range(num_nodes)
        ]
        self.jobs: Dict[str, Job] = {}
        self.pending_queue: List[Job] = []
        self.running_jobs: Dict[str, Job] = {}
        self.completed_jobs: List[Job] = []

    def reset_cluster(self):
        self.nodes = [
            Node(id=i, name=f"node-{i:03d}", rack_id=i % self.num_racks, total_gpus=self.gpus_per_node)
            for i in range(self.num_nodes)
        ]
        self.jobs = {}
        self.pending_queue = []
        self.running_jobs = {}
        self.completed_jobs = []

    def generate_workload_trace(self, duration_hours=72, default_ckpt=30, is_stress=False) -> List[Job]:
        trace_rng = random.Random(self.seed + (200 if is_stress else 100))
        jobs: List[Job] = []
        sim_duration_min = duration_hours * 60.0
        job_counter = 0

        # Initial cluster filling
        initial_gpus = 0
        fill_target = 0.98 if is_stress else 0.95
        while initial_gpus < self.total_gpus * fill_target:
            job_counter += 1
            roll = trace_rng.random()
            if roll < 0.25:
                gpus = trace_rng.choice([32, 64, 128])
                dur = trace_rng.uniform(24 * 60, 72 * 60)
                prio = PRIORITY_CRITICAL_TRAINING
                ckpt = default_ckpt
                pdb = 1.0
            elif roll < 0.60:
                gpus = trace_rng.choice([8, 16, 32])
                dur = trace_rng.uniform(4 * 60, 16 * 60)
                prio = PRIORITY_FINETUNE_RLHF
                ckpt = default_ckpt
                pdb = 1.0 if trace_rng.random() < 0.7 else 0.8
            elif roll < 0.85:
                gpus = trace_rng.choice([4, 8])
                dur = trace_rng.uniform(60, 6 * 60)
                prio = PRIORITY_BATCH_EVAL
                ckpt = default_ckpt if default_ckpt <= 30 else 30
                pdb = 0.8
            else:
                gpus = trace_rng.choice([1, 2, 4])
                dur = trace_rng.uniform(30, 2 * 60)
                prio = PRIORITY_SCAVENGER
                ckpt = 15
                pdb = 0.0

            j = Job(
                id=f"job-{job_counter:04d}",
                name=f"job-t0-{job_counter:04d}-p{prio}",
                priority=prio,
                gang_size=gpus,
                gpus_per_pod=min(gpus, 8),
                duration_minutes=dur,
                submit_time=0.0,
                checkpoint_interval=ckpt,
                pdb_min_available_pct=pdb
            )
            jobs.append(j)
            initial_gpus += gpus

        # Arrivals
        t = 5.0
        avg_interarrival = 10.0 if is_stress else 25.0
        while t < sim_duration_min - 60.0:
            inter_arrival = trace_rng.expovariate(1.0 / avg_interarrival)
            t += inter_arrival
            if t >= sim_duration_min - 60.0:
                break
                
            job_counter += 1
            roll = trace_rng.random()
            if is_stress:
                if roll < 0.30:
                    gpus = trace_rng.choice([32, 64, 128])
                    dur = trace_rng.uniform(8 * 60, 36 * 60)
                    prio = PRIORITY_CRITICAL_TRAINING
                    ckpt = default_ckpt
                    pdb = 1.0
                elif roll < 0.65:
                    gpus = trace_rng.choice([16, 32, 64])
                    dur = trace_rng.uniform(3 * 60, 10 * 60)
                    prio = PRIORITY_FINETUNE_RLHF
                    ckpt = default_ckpt
                    pdb = 1.0 if trace_rng.random() < 0.7 else 0.8
                elif roll < 0.88:
                    gpus = trace_rng.choice([4, 8, 16])
                    dur = trace_rng.uniform(45, 3 * 60)
                    prio = PRIORITY_BATCH_EVAL
                    ckpt = default_ckpt if default_ckpt <= 30 else 30
                    pdb = 0.8
                else:
                    gpus = trace_rng.choice([1, 2, 4])
                    dur = trace_rng.uniform(30, 90)
                    prio = PRIORITY_SCAVENGER
                    ckpt = 15
                    pdb = 0.0
            else:
                if roll < 0.15:
                    gpus = trace_rng.choice([32, 64, 128])
                    dur = trace_rng.uniform(12 * 60, 48 * 60)
                    prio = PRIORITY_CRITICAL_TRAINING
                    ckpt = default_ckpt
                    pdb = 1.0
                elif roll < 0.45:
                    gpus = trace_rng.choice([8, 16, 32])
                    dur = trace_rng.uniform(3 * 60, 12 * 60)
                    prio = PRIORITY_FINETUNE_RLHF
                    ckpt = default_ckpt
                    pdb = 1.0 if trace_rng.random() < 0.7 else 0.8
                elif roll < 0.80:
                    gpus = trace_rng.choice([4, 8])
                    dur = trace_rng.uniform(45, 4 * 60)
                    prio = PRIORITY_BATCH_EVAL
                    ckpt = default_ckpt if default_ckpt <= 30 else 30
                    pdb = 0.8
                else:
                    gpus = trace_rng.choice([1, 2, 4])
                    dur = trace_rng.uniform(30, 90)
                    prio = PRIORITY_SCAVENGER
                    ckpt = 15
                    pdb = 0.0

            j = Job(
                id=f"job-{job_counter:04d}",
                name=f"job-t{int(t)}-{job_counter:04d}-p{prio}",
                priority=prio,
                gang_size=gpus,
                gpus_per_pod=min(gpus, 8),
                duration_minutes=dur,
                submit_time=t,
                checkpoint_interval=ckpt,
                pdb_min_available_pct=pdb
            )
            jobs.append(j)

        return jobs

    def run_simulation(self, policy_name: str, duration_hours=72, default_ckpt=30, trace_jobs=None, scenario_name="Standard") -> SimulationStats:
        self.reset_cluster()
        if trace_jobs is None:
            trace_jobs = self.generate_workload_trace(duration_hours, default_ckpt, is_stress=(scenario_name=="Stress"))
        
        sim_jobs = {}
        for j in trace_jobs:
            cj = Job(
                id=j.id,
                name=j.name,
                priority=j.priority,
                gang_size=j.gang_size,
                gpus_per_pod=j.gpus_per_pod,
                duration_minutes=j.duration_minutes,
                submit_time=j.submit_time,
                checkpoint_interval=default_ckpt if j.priority >= PRIORITY_FINETUNE_RLHF else min(j.checkpoint_interval, default_ckpt),
                pdb_min_available_pct=j.pdb_min_available_pct
            )
            sim_jobs[cj.id] = cj
            
        self.jobs = sim_jobs
        
        event_queue = []
        for j in self.jobs.values():
            heapq.heappush(event_queue, (j.submit_time, "JOB_SUBMIT", j.id))

        sim_time = 0.0
        max_sim_time = duration_hours * 60.0
        
        total_preemption_events = 0
        total_jobs_preempted = 0
        total_pod_restarts = 0
        aggregate_pdb_violations = 0
        cascade_events_by_depth: Dict[int, int] = {}
        thrashing_loops = 0
        thrashing_jobs = set()
        total_victim_gpus_evicted = 0
        
        while event_queue:
            evt_time, evt_type, evt_data = heapq.heappop(event_queue)
            sim_time = evt_time
            if sim_time > max_sim_time:
                break
                
            if evt_type == "JOB_SUBMIT":
                job = self.jobs[evt_data]
                self.pending_queue.append(job)
                self.pending_queue.sort(key=lambda j: (-j.priority, j.submit_time))
                self._schedule_pending(sim_time, policy_name, event_queue)
                
            elif evt_type == "CHECKPOINT":
                job_id = evt_data
                if job_id in self.running_jobs:
                    job = self.running_jobs[job_id]
                    time_since_ckpt = sim_time - job.last_checkpoint_time
                    job.accumulated_productive_compute += (time_since_ckpt * job.gang_size) / 60.0
                    job.remaining_compute -= time_since_ckpt
                    job.last_checkpoint_time = sim_time
                    
                    if job.remaining_compute <= 0.001:
                        self._complete_job(job, sim_time)
                        # Re-schedule pending jobs now that capacity was freed
                        self._schedule_pending(sim_time, policy_name, event_queue)
                    else:
                        next_ckpt = sim_time + min(job.checkpoint_interval, job.remaining_compute)
                        heapq.heappush(event_queue, (next_ckpt, "CHECKPOINT", job.id))
                        
            elif evt_type == "JOB_COMPLETE":
                job_id = evt_data
                if job_id in self.running_jobs:
                    job = self.running_jobs[job_id]
                    self._complete_job(job, sim_time)
                    self._schedule_pending(sim_time, policy_name, event_queue)

        for job in list(self.running_jobs.values()):
            if job.last_checkpoint_time is not None:
                final_time = min(sim_time, max_sim_time)
                time_since_ckpt = final_time - job.last_checkpoint_time
                job.accumulated_productive_compute += (time_since_ckpt * job.gang_size) / 60.0

        total_capacity_hours = duration_hours * self.total_gpus
        productive_gpu_hours = sum(j.accumulated_productive_compute for j in self.jobs.values())
        checkpoint_waste_gpu_hours = sum(j.accumulated_waste for j in self.jobs.values())
        restart_penalty_gpu_hours = sum(j.accumulated_restart_penalty for j in self.jobs.values())
        total_allocated_gpu_hours = productive_gpu_hours + checkpoint_waste_gpu_hours + restart_penalty_gpu_hours
        idle_allocated_gpu_hours = restart_penalty_gpu_hours
        
        goodput_ratio = (productive_gpu_hours / total_allocated_gpu_hours * 100.0) if total_allocated_gpu_hours > 0 else 0.0
        
        tier2_running_hours = sum(
            (j.accumulated_productive_compute + j.accumulated_waste) / max(1, j.gang_size)
            for j in self.jobs.values() if j.priority == PRIORITY_FINETUNE_RLHF
        )
        tier2_preemptions = sum(j.restarts for j in self.jobs.values() if j.priority == PRIORITY_FINETUNE_RLHF)
        mtbp_tier2 = (tier2_running_hours / tier2_preemptions) if tier2_preemptions > 0 else tier2_running_hours

        tier3_running_hours = sum(
            (j.accumulated_productive_compute + j.accumulated_waste) / max(1, j.gang_size)
            for j in self.jobs.values() if j.priority == PRIORITY_BATCH_EVAL
        )
        tier3_preemptions = sum(j.restarts for j in self.jobs.values() if j.priority == PRIORITY_BATCH_EVAL)
        mtbp_tier3 = (tier3_running_hours / tier3_preemptions) if tier3_preemptions > 0 else tier3_running_hours

        all_cascade_depths = [j.cascade_depth for j in self.jobs.values() if j.restarts > 0]
        max_cascade = max(all_cascade_depths) if all_cascade_depths else 0
        mean_cascade = (sum(all_cascade_depths) / len(all_cascade_depths)) if all_cascade_depths else 0.0

        for j in self.jobs.values():
            if len(j.preemption_history) >= 2:
                for idx in range(len(j.preemption_history) - 1):
                    if j.preemption_history[idx+1] - j.preemption_history[idx] <= 60.0:
                        thrashing_loops += 1
                        thrashing_jobs.add(j.id)

        stretches = []
        for j in self.completed_jobs:
            actual_time = j.last_checkpoint_time - j.submit_time if j.last_checkpoint_time else j.duration_minutes
            ideal_time = j.duration_minutes
            stretches.append(max(1.0, actual_time / ideal_time))
        avg_jct_stretch = (sum(stretches) / len(stretches)) if stretches else 1.0

        tier_completions = {}
        tier_goodput = {}
        for p in [PRIORITY_CRITICAL_TRAINING, PRIORITY_FINETUNE_RLHF, PRIORITY_BATCH_EVAL, PRIORITY_SCAVENGER]:
            tier_completions[p] = sum(1 for j in self.completed_jobs if j.priority == p)
            p_prod = sum(j.accumulated_productive_compute for j in self.jobs.values() if j.priority == p)
            p_waste = sum(j.accumulated_waste for j in self.jobs.values() if j.priority == p)
            p_alloc = p_prod + p_waste
            tier_goodput[p] = (p_prod / p_alloc * 100.0) if p_alloc > 0 else 100.0

        for j in self.jobs.values():
            total_jobs_preempted += (1 if j.restarts > 0 else 0)
            total_pod_restarts += j.restarts
            for d in [j.cascade_depth]:
                if j.restarts > 0:
                    cascade_events_by_depth[d] = cascade_events_by_depth.get(d, 0) + j.restarts
            if j.restarts > 0:
                total_victim_gpus_evicted += (j.gang_size * j.restarts)
                
        total_preemption_events = sum(j.restarts for j in self.jobs.values())
        aggregate_pdb_violations = sum(
            j.restarts for j in self.jobs.values() if j.pdb_min_available_pct >= 0.8 and j.priority >= PRIORITY_BATCH_EVAL
        )
        
        preemptor_gpu_sum = sum(j.gang_size for j in self.jobs.values() if any(v.preempted_by_job_id == j.id for v in self.jobs.values()))
        blast_multiplier = (total_victim_gpus_evicted / max(1, preemptor_gpu_sum)) if preemptor_gpu_sum > 0 else 1.0

        return SimulationStats(
            policy_name=policy_name,
            scenario_name=scenario_name,
            checkpoint_interval_param=default_ckpt,
            total_sim_hours=duration_hours,
            total_gpu_capacity_hours=total_capacity_hours,
            total_gpu_allocated_hours=total_allocated_gpu_hours,
            productive_gpu_hours=productive_gpu_hours,
            checkpoint_waste_gpu_hours=checkpoint_waste_gpu_hours,
            restart_penalty_gpu_hours=restart_penalty_gpu_hours,
            idle_allocated_gpu_hours=idle_allocated_gpu_hours,
            goodput_ratio=goodput_ratio,
            total_preemption_events=total_preemption_events,
            total_jobs_preempted=total_jobs_preempted,
            total_pod_restarts=total_pod_restarts,
            aggregate_pdb_violations=aggregate_pdb_violations,
            max_cascade_depth=max_cascade,
            mean_cascade_depth=mean_cascade,
            cascade_events_by_depth=cascade_events_by_depth,
            thrashing_loops=thrashing_loops,
            thrashing_jobs_count=len(thrashing_jobs),
            cascade_blast_radius_multiplier=blast_multiplier,
            mtbp_tier2_hours=mtbp_tier2,
            mtbp_tier3_hours=mtbp_tier3,
            tier_completions=tier_completions,
            tier_goodput=tier_goodput,
            avg_jct_stretch=avg_jct_stretch
        )

    def _complete_job(self, job: Job, sim_time: float):
        job.state = "COMPLETED"
        for node_id in job.assigned_nodes:
            node = self.nodes[node_id]
            node.assigned_pods = [(jid, g) for (jid, g) in node.assigned_pods if jid != job.id]
            node.allocated_gpus = sum(g for (_, g) in node.assigned_pods)
        job.assigned_nodes = []
        if job.id in self.running_jobs:
            del self.running_jobs[job.id]
        self.completed_jobs.append(job)

    def _schedule_pending(self, sim_time: float, policy_name: str, event_queue: List[Any]):
        i = 0
        while i < len(self.pending_queue):
            job = self.pending_queue[i]
            placement = self._find_fit_without_preemption(job)
            if placement is not None:
                self.pending_queue.pop(i)
                self._start_job(job, placement, sim_time, event_queue)
                continue
                
            preempt_result = self._evaluate_preemption(job, sim_time, policy_name)
            if preempt_result is not None:
                victim_job_ids, placement = preempt_result
                self.pending_queue.pop(i)
                
                for v_id in victim_job_ids:
                    if v_id in self.running_jobs:
                        self._preempt_victim(self.running_jobs[v_id], job, sim_time)
                        
                self._start_job(job, placement, sim_time, event_queue)
                continue
                
            i += 1

    def _find_fit_without_preemption(self, job: Job) -> Optional[List[Tuple[int, int]]]:
        needed_gpus = job.gang_size
        pod_gpus = job.gpus_per_pod
        placement = []
        
        candidate_nodes = sorted(self.nodes, key=lambda n: -n.free_gpus)
        
        for node in candidate_nodes:
            if node.free_gpus >= pod_gpus:
                pods_to_fit = min(node.free_gpus // pod_gpus, needed_gpus // pod_gpus)
                if pods_to_fit > 0:
                    alloc = pods_to_fit * pod_gpus
                    placement.append((node.id, alloc))
                    needed_gpus -= alloc
                    if needed_gpus == 0:
                        return placement
        return None

    def _evaluate_preemption(self, job: Job, sim_time: float, policy_name: str) -> Optional[Tuple[List[str], List[Tuple[int, int]]]]:
        cluster_preemptible_gpus = sum(
            node.free_gpus + sum(g for (jid, g) in node.assigned_pods if self.jobs[jid].priority < job.priority)
            for node in self.nodes
        )
        if cluster_preemptible_gpus < job.gang_size:
            return None

        if policy_name in ["Baseline-Greedy", "Idea1-Headroom", "Idea3-LogReprieval", "Idea4-TemplateCache"]:
            return self._preemption_baseline_greedy(job, sim_time)
        elif policy_name == "Idea5-BranchAndBound":
            return self._preemption_branch_and_bound(job, sim_time, collective_graph=False, progress_aware=False)
        elif policy_name == "Idea6-DomainSearch":
            return self._preemption_domain_search(job, sim_time)
        elif policy_name == "Idea7-UndoLog":
            return self._preemption_baseline_greedy(job, sim_time, is_undo_log=True)
        elif policy_name == "Idea8-PriorityBucketing":
            return self._preemption_priority_bucketing(job, sim_time)
        elif policy_name == "Idea9-TopologySpreadBounds":
            return self._preemption_topology_spread_bounds(job, sim_time)
        elif policy_name == "Idea10-CollectiveVictimGraph":
            return self._preemption_branch_and_bound(job, sim_time, collective_graph=True, progress_aware=False)
        elif policy_name == "Layered-Stack":
            return self._preemption_branch_and_bound(job, sim_time, collective_graph=True, progress_aware=False, bucketing=True)
        elif policy_name == "Progress-Aware-Stack":
            return self._preemption_branch_and_bound(job, sim_time, collective_graph=True, progress_aware=True, bucketing=True)
        else:
            return self._preemption_baseline_greedy(job, sim_time)

    def _preemption_baseline_greedy(self, job: Job, sim_time: float, is_undo_log=False) -> Optional[Tuple[List[str], List[Tuple[int, int]]]]:
        needed_gpus = job.gang_size
        pod_gpus = job.gpus_per_pod
        victims = set()
        placement = []
        
        for node in self.nodes:
            potential_victims = sorted(
                [(jid, g) for (jid, g) in node.assigned_pods if self.jobs[jid].priority < job.priority],
                key=lambda x: self.jobs[x[0]].priority
            )
            
            node_yield = node.free_gpus
            chosen_node_victims = []
            for jid, g in potential_victims:
                if node_yield >= pod_gpus and (node_yield >= needed_gpus or node_yield >= 8):
                    break
                chosen_node_victims.append(jid)
                node_yield += g
                
            if node_yield >= pod_gpus:
                pods_fit = min(node_yield // pod_gpus, needed_gpus // pod_gpus)
                if pods_fit > 0:
                    alloc = pods_fit * pod_gpus
                    placement.append((node.id, alloc))
                    needed_gpus -= alloc
                    for jid in chosen_node_victims:
                        victims.add(jid)
                    if needed_gpus == 0:
                        return list(victims), placement
                        
        return None

    def _preemption_domain_search(self, job: Job, sim_time: float) -> Optional[Tuple[List[str], List[Tuple[int, int]]]]:
        for rack_id in range(self.num_racks):
            rack_nodes = [n for n in self.nodes if n.rack_id == rack_id]
            needed_gpus = job.gang_size
            pod_gpus = job.gpus_per_pod
            victims = set()
            placement = []
            
            for node in rack_nodes:
                potential_victims = sorted(
                    [(jid, g) for (jid, g) in node.assigned_pods if self.jobs[jid].priority < job.priority],
                    key=lambda x: self.jobs[x[0]].priority
                )
                node_yield = node.free_gpus
                chosen_node_victims = []
                for jid, g in potential_victims:
                    if node_yield >= pod_gpus and (node_yield >= needed_gpus or node_yield >= 8):
                        break
                    chosen_node_victims.append(jid)
                    node_yield += g
                    
                if node_yield >= pod_gpus:
                    pods_fit = min(node_yield // pod_gpus, needed_gpus // pod_gpus)
                    if pods_fit > 0:
                        alloc = pods_fit * pod_gpus
                        placement.append((node.id, alloc))
                        needed_gpus -= alloc
                        for jid in chosen_node_victims:
                            victims.add(jid)
                        if needed_gpus == 0:
                            return list(victims), placement
        return None

    def _preemption_priority_bucketing(self, job: Job, sim_time: float) -> Optional[Tuple[List[str], List[Tuple[int, int]]]]:
        needed_gpus = job.gang_size
        pod_gpus = job.gpus_per_pod
        victims = set()
        placement = []
        candidate_nodes = sorted(self.nodes, key=lambda n: -n.free_gpus)
        
        # Pass 1: Only evict Priority 0
        for node in candidate_nodes:
            node_yield = node.free_gpus
            chosen = []
            for jid, g in node.assigned_pods:
                if self.jobs[jid].priority == PRIORITY_SCAVENGER:
                    chosen.append(jid)
                    node_yield += g
            if node_yield >= pod_gpus:
                pods_fit = min(node_yield // pod_gpus, needed_gpus // pod_gpus)
                if pods_fit > 0:
                    alloc = pods_fit * pod_gpus
                    placement.append((node.id, alloc))
                    needed_gpus -= alloc
                    victims.update(chosen)
                    if needed_gpus == 0:
                        return list(victims), placement
                        
        # Pass 2: Allow evicting Priority <= 100
        needed_gpus = job.gang_size
        placement = []
        victims = set()
        for node in candidate_nodes:
            node_yield = node.free_gpus
            chosen = []
            for jid, g in node.assigned_pods:
                if self.jobs[jid].priority <= PRIORITY_BATCH_EVAL and self.jobs[jid].priority < job.priority:
                    chosen.append(jid)
                    node_yield += g
            if node_yield >= pod_gpus:
                pods_fit = min(node_yield // pod_gpus, needed_gpus // pod_gpus)
                if pods_fit > 0:
                    alloc = pods_fit * pod_gpus
                    placement.append((node.id, alloc))
                    needed_gpus -= alloc
                    victims.update(chosen)
                    if needed_gpus == 0:
                        return list(victims), placement

        return self._preemption_baseline_greedy(job, sim_time)

    def _preemption_topology_spread_bounds(self, job: Job, sim_time: float) -> Optional[Tuple[List[str], List[Tuple[int, int]]]]:
        target_per_rack = job.gang_size // self.num_racks
        needed_gpus = job.gang_size
        pod_gpus = job.gpus_per_pod
        victims = set()
        placement = []
        
        for rack_id in range(self.num_racks):
            rack_nodes = [n for n in self.nodes if n.rack_id == rack_id]
            rack_needed = min(target_per_rack, needed_gpus)
            for node in rack_nodes:
                if rack_needed <= 0:
                    break
                potential_victims = sorted(
                    [(jid, g) for (jid, g) in node.assigned_pods if self.jobs[jid].priority < job.priority],
                    key=lambda x: self.jobs[x[0]].priority
                )
                node_yield = node.free_gpus
                chosen = []
                for jid, g in potential_victims:
                    if node_yield >= pod_gpus and node_yield >= rack_needed:
                        break
                    chosen.append(jid)
                    node_yield += g
                if node_yield >= pod_gpus:
                    pods_fit = min(node_yield // pod_gpus, rack_needed // pod_gpus)
                    if pods_fit > 0:
                        alloc = pods_fit * pod_gpus
                        placement.append((node.id, alloc))
                        rack_needed -= alloc
                        needed_gpus -= alloc
                        victims.update(chosen)
                        
        if needed_gpus == 0:
            return list(victims), placement
        return None

    def _preemption_branch_and_bound(self, job: Job, sim_time: float, collective_graph=False, progress_aware=False, bucketing=False) -> Optional[Tuple[List[str], List[Tuple[int, int]]]]:
        needed_gpus = job.gang_size
        pod_gpus = job.gpus_per_pod
        
        candidate_node_scores = []
        for node in self.nodes:
            potential_victims = [
                (jid, g) for (jid, g) in node.assigned_pods
                if self.jobs[jid].priority < job.priority
            ]
            if bucketing:
                potential_victims.sort(key=lambda x: self.jobs[x[0]].priority)
                
            total_yield = node.free_gpus + sum(g for (_, g) in potential_victims)
            if total_yield < pod_gpus:
                continue
                
            node_victim_cost = 0.0
            node_victims = []
            curr_yield = node.free_gpus
            
            for jid, g in potential_victims:
                if curr_yield >= 8 or curr_yield >= needed_gpus:
                    break
                v_job = self.jobs[jid]
                base_cost = (v_job.priority + 10) * g
                
                if v_job.pdb_min_available_pct >= 0.8:
                    base_cost += 500.0 * g
                    
                if progress_aware and v_job.last_checkpoint_time is not None:
                    time_in_interval = sim_time - v_job.last_checkpoint_time
                    waste_ratio = min(1.0, time_in_interval / max(1.0, v_job.checkpoint_interval))
                    base_cost *= (1.0 + 3.0 * waste_ratio)
                    
                node_victim_cost += base_cost
                node_victims.append(jid)
                curr_yield += g
                
            candidate_node_scores.append({
                'node': node,
                'max_pods': min(curr_yield // pod_gpus, needed_gpus // pod_gpus),
                'cost': node_victim_cost,
                'victims': node_victims,
                'yield': curr_yield
            })
            
        candidate_node_scores.sort(key=lambda x: x['cost'] / max(1, x['max_pods']))
        
        selected_placement = []
        selected_victims = set()
        allocated_gpus = 0
        
        for cand in candidate_node_scores:
            if allocated_gpus >= needed_gpus:
                break
            pods_to_take = min(cand['max_pods'], (needed_gpus - allocated_gpus) // pod_gpus)
            if pods_to_take > 0:
                alloc = pods_to_take * pod_gpus
                selected_placement.append((cand['node'].id, alloc))
                allocated_gpus += alloc
                selected_victims.update(cand['victims'])
                
        if allocated_gpus >= needed_gpus:
            final_victims = set(selected_victims)
            if collective_graph:
                for v_id in list(selected_victims):
                    final_victims.add(v_id)
            return list(final_victims), selected_placement
            
        return None

    def _start_job(self, job: Job, placement: List[Tuple[int, int]], sim_time: float, event_queue: List[Any]):
        job.state = "RUNNING"
        job.start_time = sim_time
        job.last_checkpoint_time = sim_time
        job.assigned_nodes = [node_id for (node_id, _) in placement]
        
        if job.restarts > 0:
            restart_overhead = 5.0 # minutes
            job.accumulated_restart_penalty += (restart_overhead * job.gang_size) / 60.0
            
        for node_id, alloc in placement:
            node = self.nodes[node_id]
            node.assigned_pods.append((job.id, alloc))
            node.allocated_gpus += alloc
            
        self.running_jobs[job.id] = job
        next_ckpt = sim_time + min(job.checkpoint_interval, job.remaining_compute)
        heapq.heappush(event_queue, (next_ckpt, "CHECKPOINT", job.id))

    def _preempt_victim(self, victim: Job, preemptor: Job, sim_time: float):
        victim.state = "PREEMPTED"
        victim.restarts += 1
        victim.preemption_history.append(sim_time)
        victim.last_preempted_time = sim_time
        
        victim.preempted_by_job_id = preemptor.id
        victim.cascade_origin_job_id = preemptor.cascade_origin_job_id or preemptor.id
        victim.cascade_depth = preemptor.cascade_depth + 1
        
        if victim.last_checkpoint_time is not None:
            time_lost = sim_time - victim.last_checkpoint_time
            victim.accumulated_waste += (time_lost * victim.gang_size) / 60.0
            
        for node_id in victim.assigned_nodes:
            node = self.nodes[node_id]
            node.assigned_pods = [(jid, g) for (jid, g) in node.assigned_pods if jid != victim.id]
            node.allocated_gpus = sum(g for (_, g) in node.assigned_pods)
        victim.assigned_nodes = []
        
        if victim.id in self.running_jobs:
            del self.running_jobs[victim.id]
            
        self.pending_queue.append(victim)
        self.pending_queue.sort(key=lambda j: (-j.priority, j.submit_time))


def run_full_suite():
    print("================================================================================")
    print("Cluster Goodput, Checkpoint Waste, & Preemption Thrashing Simulation Engine")
    print("Repository: bwsalmon/kubernetes (bsalmon-preempt)")
    print("Platform: 128 Nodes (1,024 NVIDIA H100 GPUs), 4 Racks, 72-Hour Continuous Window")
    print("================================================================================\n")
    
    sim = PreemptionSimulator(num_nodes=128, gpus_per_node=8, racks=4, seed=42)
    
    print("[1/4] Generating 72-hour Workload Traces (Standard & High-Stress Contention)...")
    base_trace = sim.generate_workload_trace(duration_hours=72, default_ckpt=30, is_stress=False)
    stress_trace = sim.generate_workload_trace(duration_hours=72, default_ckpt=30, is_stress=True)
    print(f"      Standard Trace: {len(base_trace)} jobs (~105% load)")
    print(f"      High-Stress Trace: {len(stress_trace)} jobs (~135% burst load)")
    
    policies = [
        "Baseline-Greedy",
        "Idea1-Headroom",
        "Idea3-LogReprieval",
        "Idea4-TemplateCache",
        "Idea5-BranchAndBound",
        "Idea6-DomainSearch",
        "Idea7-UndoLog",
        "Idea8-PriorityBucketing",
        "Idea9-TopologySpreadBounds",
        "Idea10-CollectiveVictimGraph",
        "Layered-Stack",
        "Progress-Aware-Stack",
    ]
    
    results_std: Dict[str, Dict[str, Any]] = {}
    results_stress: Dict[str, Dict[str, Any]] = {}
    
    print("\n[2/4] Executing Scenario A: Standard Production Multi-Tenant Operations (ckpt=30m)...")
    print(f"{'Policy / Algorithm':<26} | {'Goodput %':<9} | {'Waste (GPU-h)':<13} | {'Preemptions':<11} | {'Cascade Max':<11} | {'Thrashing':<9} | {'MTBP-T2 (h)':<11}")
    print("-" * 105)
    
    for pol in policies:
        stats = sim.run_simulation(pol, duration_hours=72, default_ckpt=30, trace_jobs=base_trace, scenario_name="Standard")
        results_std[pol] = {
            "policy": stats.policy_name,
            "scenario": stats.scenario_name,
            "ckpt_interval": stats.checkpoint_interval_param,
            "total_sim_hours": stats.total_sim_hours,
            "total_gpu_capacity_hours": stats.total_gpu_capacity_hours,
            "total_gpu_allocated_hours": stats.total_gpu_allocated_hours,
            "productive_gpu_hours": stats.productive_gpu_hours,
            "checkpoint_waste_gpu_hours": stats.checkpoint_waste_gpu_hours,
            "restart_penalty_gpu_hours": stats.restart_penalty_gpu_hours,
            "goodput_ratio": stats.goodput_ratio,
            "total_preemption_events": stats.total_preemption_events,
            "total_jobs_preempted": stats.total_jobs_preempted,
            "total_pod_restarts": stats.total_pod_restarts,
            "aggregate_pdb_violations": stats.aggregate_pdb_violations,
            "max_cascade_depth": stats.max_cascade_depth,
            "mean_cascade_depth": stats.mean_cascade_depth,
            "thrashing_loops": stats.thrashing_loops,
            "thrashing_jobs_count": stats.thrashing_jobs_count,
            "cascade_blast_multiplier": stats.cascade_blast_radius_multiplier,
            "mtbp_tier2_hours": stats.mtbp_tier2_hours,
            "mtbp_tier3_hours": stats.mtbp_tier3_hours,
            "avg_jct_stretch": stats.avg_jct_stretch,
            "tier_goodput": stats.tier_goodput,
            "tier_completions": stats.tier_completions
        }
        print(f"{pol:<26} | {stats.goodput_ratio:>8.2f}% | {stats.checkpoint_waste_gpu_hours:>13.1f} | {stats.total_preemption_events:>11} | {stats.max_cascade_depth:>11} | {stats.thrashing_loops:>9} | {stats.mtbp_tier2_hours:>11.2f}")

    print("\n[3/4] Executing Scenario B: High-Stress Gang Congestion & Cascade Waves (ckpt=30m)...")
    print(f"{'Policy / Algorithm':<26} | {'Goodput %':<9} | {'Waste (GPU-h)':<13} | {'Preemptions':<11} | {'Cascade Max':<11} | {'Thrashing':<9} | {'MTBP-T2 (h)':<11}")
    print("-" * 105)
    
    for pol in policies:
        stats = sim.run_simulation(pol, duration_hours=72, default_ckpt=30, trace_jobs=stress_trace, scenario_name="Stress")
        results_stress[pol] = {
            "policy": stats.policy_name,
            "scenario": stats.scenario_name,
            "ckpt_interval": stats.checkpoint_interval_param,
            "total_sim_hours": stats.total_sim_hours,
            "total_gpu_capacity_hours": stats.total_gpu_capacity_hours,
            "total_gpu_allocated_hours": stats.total_gpu_allocated_hours,
            "productive_gpu_hours": stats.productive_gpu_hours,
            "checkpoint_waste_gpu_hours": stats.checkpoint_waste_gpu_hours,
            "restart_penalty_gpu_hours": stats.restart_penalty_gpu_hours,
            "goodput_ratio": stats.goodput_ratio,
            "total_preemption_events": stats.total_preemption_events,
            "total_jobs_preempted": stats.total_jobs_preempted,
            "total_pod_restarts": stats.total_pod_restarts,
            "aggregate_pdb_violations": stats.aggregate_pdb_violations,
            "max_cascade_depth": stats.max_cascade_depth,
            "mean_cascade_depth": stats.mean_cascade_depth,
            "thrashing_loops": stats.thrashing_loops,
            "thrashing_jobs_count": stats.thrashing_jobs_count,
            "cascade_blast_multiplier": stats.cascade_blast_radius_multiplier,
            "mtbp_tier2_hours": stats.mtbp_tier2_hours,
            "mtbp_tier3_hours": stats.mtbp_tier3_hours,
            "avg_jct_stretch": stats.avg_jct_stretch,
            "tier_goodput": stats.tier_goodput,
            "tier_completions": stats.tier_completions
        }
        print(f"{pol:<26} | {stats.goodput_ratio:>8.2f}% | {stats.checkpoint_waste_gpu_hours:>13.1f} | {stats.total_preemption_events:>11} | {stats.max_cascade_depth:>11} | {stats.thrashing_loops:>9} | {stats.mtbp_tier2_hours:>11.2f}")

    print("\n[4/4] Evaluating Checkpoint Interval Sensitivity (tau = 15m, 30m, 60m) under Stress...")
    ckpt_results = {}
    key_policies = ["Baseline-Greedy", "Idea5-BranchAndBound", "Layered-Stack", "Progress-Aware-Stack"]
    for ckpt in CHECKPOINT_INTERVALS:
        ckpt_results[ckpt] = {}
        stress_ckpt_trace = sim.generate_workload_trace(duration_hours=72, default_ckpt=ckpt, is_stress=True)
        for pol in key_policies:
            stats = sim.run_simulation(pol, duration_hours=72, default_ckpt=ckpt, trace_jobs=stress_ckpt_trace, scenario_name="Stress")
            ckpt_results[ckpt][pol] = {
                "goodput_ratio": stats.goodput_ratio,
                "checkpoint_waste_gpu_hours": stats.checkpoint_waste_gpu_hours,
                "preemptions": stats.total_preemption_events,
                "mtbp_t2": stats.mtbp_tier2_hours,
                "thrashing_loops": stats.thrashing_loops
            }
            print(f"  tau={ckpt:2d}m | {pol:<22} -> Goodput: {stats.goodput_ratio:5.2f}%, Waste: {stats.checkpoint_waste_gpu_hours:8.1f} GPU-h, Preemptions: {stats.total_preemption_events:3d}, Thrash: {stats.thrashing_loops}")

    out_file = "/home/debian/work/test/simulation/simulation_results.json"
    with open(out_file, "w") as f:
        json.dump({
            "scenario_standard_72h": results_std,
            "scenario_stress_72h": results_stress,
            "checkpoint_sensitivity_stress": ckpt_results
        }, f, indent=2)
    print(f"\nSimulation suite completed successfully. Results saved to {out_file}\n")

if __name__ == "__main__":
    run_full_suite()
