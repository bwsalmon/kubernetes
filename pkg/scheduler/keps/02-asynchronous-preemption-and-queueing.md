# Deep Dive: Asynchronous Preemption & Queuing KEPs (KEP-4832 & KEP-5142)

**Governing KEPs:**
- **KEP-4832**: *Asynchronous Preemption & Scheduler Async API Calls*
- **KEP-5142**: *Scheduling Queue Backoff and Unschedulable Queue Optimizations*

**Primary Packages:**
- `pkg/scheduler/framework/preemption/`
- `pkg/scheduler/backend/api_dispatcher/`
- `pkg/scheduler/backend/queue/`
- `pkg/scheduler/schedule_one.go`
- `test/integration/scheduler/preemption/`

---

## 1. Executive Summary & Problem Statement

In the legacy preemption architecture (KEP-562), when a pod cannot find a node with sufficient available resources during `PostFilter` evaluation, the scheduler identifies lower-priority victim pods on candidate nodes and synchronously issues HTTPS `DELETE` / `EVICT` requests to `kube-apiserver` within the core scheduling thread (`scheduleOne`).

Under high-throughput cluster conditions or multi-victim preemption storms, this synchronous approach suffered from severe operational and throughput bottlenecks:

1. **Head-of-Line (HoL) Blocking**: 
   The primary scheduler thread blocked on synchronous API server round-trips and etcd serialization for each victim pod. Evicting multiple pods across nodes or encountering slow API server responses reduced scheduler throughput from hundreds of pods/second to single digits (<10 pods/sec).
2. **Victim Termination Latency & Pod Starvation**: 
   While `scheduleOne` waited for API deletion acknowledgments, all other independent, schedulable pods in `activeQ` were blocked, compounding queue latency across the cluster.
3. **Queue Churn & Mutex Contention**: 
   Without pre-enqueue gating and event-aware queue movement (KEP-5142), unschedulable preemptor pods repeatedly cycled through `activeQ` -> `scheduleOne` -> `PostFilter` -> `unschedulablePods`, burning CPU and causing severe mutex lock contention in `PriorityQueue`.

**KEP-4832** (*Asynchronous Preemption*) resolves HoL blocking by decoupling preemption evaluation from victim deletion actuation. `scheduleOne` identifies victims, sets nominated nodes and pre-enqueue gates in memory, dispatches background deletion goroutines via `Executor`, and immediately returns control to process subsequent pods.

**KEP-5142** (*Scheduling Queue Backoff and Unschedulable Queue Optimizations*) resolves queue thrashing and starvation by introducing event-driven `QueueingHint` filtering, `PreEnqueue` gating, `SchedulerPopFromBackoffQ` direct queue popping, and robust in-flight cluster mutation tracking.

---

## 2. End-to-End Subsystem Architecture

```
                  +-------------------------------------------------------------+
                  |                  scheduleOne (Main Thread)                  |
                  +-------------------------------------------------------------+
                                                 |
                                     [Run PostFilter Plugins]
                                                 |
                                 Identified Victims {V1, V2, ..., Vn}
                                                 |
                       +-------------------------+-------------------------+
                       |                                                   |
         (Synchronous In-Memory State)                           (Asynchronous Actuation)
                       |                                                   |
           Set NominatedNodeName in Cache                        Dispatch Background Goroutine
           Register in e.preempting map                          (context.WithCancel(Background))
           Set PreEnqueue In-Flight Gate                                   |
                       |                                                   v
           Return Status: Unschedulable                         prepareCandidateAsync()
                       |                                                   |
             Next Pod in activeQ!                        +-----------------+-----------------+
                                                         |                                   |
                                              (Victims 0..n-2: Parallel)             (Victim n-1: Last)
                                                         |                                   |
                                              fh.Parallelizer().Until(...)          Register in lastVictims
                                              - Patch DisruptionTarget              PendingPreemption map
                                              - Delete Pod via API / Dispatcher              |
                                                         |                          Delete Last Victim
                                                         +-----------------+-----------------+
                                                                           |
                                                      +--------------------+--------------------+
                                                      |                                         |
                                                 (All Succeeded / 404)                     (Any Failed != 404)
                                                      |                                         |
                                            Remove from e.preempting                   Abort Remaining Evictions
                                            Remove lastVictimsPendingPreemption        Clear e.preempting & Nominations
                                            Trigger e.fh.Activate(preemptor)           Trigger e.fh.Activate(preemptor)
                                                      |                                         |
                                            Preemptor Moves to activeQ/backoffQ        Preemptor Moves to backoffQ
```

---

## 3. Asynchronous Preemption Engine (`pkg/scheduler/framework/preemption/`)

The `Executor` struct in `pkg/scheduler/framework/preemption/executor.go` coordinates synchronous evaluation with asynchronous actuation.

### 3.1 Actuation Lifecycle & Background Worker Dispatch
When feature gate `SchedulerAsyncPreemption` is enabled:
1. `ActuatePodPreemption` or `ActuatePodGroupPreemption` is called by `DefaultPreemption` during `PostFilter`.
2. The executor invokes `prepareCandidateAsync(candidate, preemptor, pluginName)`.
3. The preemptor's UID is recorded in `e.preempting` (`sync.Map`), which acts as an in-flight gate across scheduling cycles.
4. A background goroutine is dispatched with a detached root context:
   ```go
   ctx, cancel := context.WithCancel(context.Background())
   ```
   Detaching from the scheduling cycle's context is essential so that when `scheduleOne` returns immediately with `Status: Unschedulable`, the background preemption worker is not canceled.
5. The `scheduleOne` thread immediately proceeds to the next pod in `activeQ`.

### 3.2 Parallel Victim Eviction & Last-Victim Serialization
To minimize eviction latency when multiple victim pods must be deleted on a node:
- **Intermediate Victims ($V_0 \dots V_{n-2}$)**:
  The executor deletes victims 0 through $n-2$ concurrently using the framework parallelizer:
  ```go
  fh.Parallelizer().Until(ctx, len(victims)-1, func(piece int) {
      if err := e.PreemptPod(ctx, candidate, preemptor, victims[piece], pluginName); err != nil {
          errCh <- err
      }
  })
  ```
- **Last Victim ($V_{n-1}$)**:
  The last victim is serialized and tracked in `e.lastVictimsPendingPreemption[preemptor.UID] = victim.UID`.
- **Early PreEnqueue Unblocking**:
  `IsPodRunningPreemption(podUID)` checks whether a pod is actively running preemption. If the last victim is already deleted or has a non-nil `DeletionTimestamp` in the pod lister cache, `IsPodRunningPreemption` returns `false` early. This allows the preemptor to pass the `PreEnqueue` check as soon as all victims are terminating or terminated.

### 3.3 Toleration of 404 Not Found & Idempotent Eviction
In dynamic Kubernetes clusters, victim pods may finish execution, get deleted by their managing controllers, or be evicted by kubelet concurrently while preemption is in flight.

In `PreemptPod`:
1. The scheduler creates a `v1.DisruptionTarget` condition with reason `PodReasonPreemptionByScheduler` and calls `schedutil.PatchPodStatus`.
2. It then issues `schedutil.DeletePod`.
3. If either operation returns `apierrors.IsNotFound(err)` (HTTP 404), the error is explicitly caught and treated as a successful eviction:
   ```go
   if apierrors.IsNotFound(err) {
       logger.V(2).Info("Victim Pod is already deleted", "preemptor", klog.KObj(preemptor), "victim", klog.KObj(victim))
       return false, nil
   }
   ```
   This idempotency prevents unnecessary preemption aborts and ensures scheduling converges rapidly.

### 3.4 In-Memory Eviction for `WaitingPod` & `PodsInPreBind`
Not all victim pods require network calls to `kube-apiserver`:
- **Victims in Permit Phase (`WaitingPod`)**:
  If a victim is currently waiting on gang-scheduling permits, `e.fh.GetWaitingPod(victim.UID)` retrieves the permit. `waitingPod.Preempt(pluginName, "preempted")` rejects the permit in-memory.
- **Victims in PreBind Phase (`PodInPreBind`)**:
  If a victim is undergoing plugin pre-bind execution, `e.fh.GetPodInPreBind(victim.UID)` cancels the binding in-memory via `podInPreBind.CancelPod(...)`.
- Both paths set `preemptedInMemory = true`, avoiding API server round-trips and immediately unblocking the preemptor.

### 3.5 Fast-Fail Rollback & Activation
If an unrecoverable non-404 API error occurs during intermediate victim deletion (e.g. `403 Forbidden`, `500 Internal Error`):
1. The worker records the error in `errCh` and aborts deletion of remaining victims (`preemptLastVictim = false`).
2. The deferred cleanup routine detects `result == metrics.GoroutineResultError`.
3. It cleans up `e.preempting` and `e.lastVictimsPendingPreemption`.
4. It calls `e.fh.Activate(logger, preemptor.Pods())`. Activation immediately moves the preemptor out of `unschedulableEntities` into `backoffQ` / `activeQ`, ensuring the preemptor does not become permanently stranded in the unschedulable queue.

---

## 4. Asynchronous API Dispatcher (`pkg/scheduler/backend/api_dispatcher/`)

When `SchedulerAsyncAPICalls` is enabled, the scheduler routes outbound mutation calls (such as status updates, bindings, and deletions) through `APIDispatcher`.

### 4.1 Architecture & Ring Buffer Queueing
`APIDispatcher` decouples caller goroutines from outbound HTTP latency:
- **`callQueue` Ring Buffer**: An internal FIFO queue storing incoming `apiCall` requests.
- **Worker Pool & Concurrency Limiter**: A pool of background worker goroutines bounded by `goroutines_limiter` to prevent overwhelming `kube-apiserver` with connection spikes.

### 4.2 Deduplication, Relevance Hierarchy & Coalescence
`APIDispatcher` implements intelligent call reconciliation per Kubernetes object:
1. **At-Most-One In-Flight Call**: For any object UID, only one API call is executed at any given time.
2. **Relevance Hierarchy**: Each call type is assigned a relevance priority. Higher-relevance calls (e.g. `PodDelete` or `PodBind`) supersede or coalesce with lower-relevance pending calls (e.g. intermediate status condition patches).
3. **State Reconciliation (`SyncObject`)**: Before dispatching a queued call, `SyncObject` verifies current informer cache state against the target mutation, discarding redundant or outdated API calls.

---

## 5. Scheduling Queue Backoff & Gating (KEP-5142)

The `PriorityQueue` in `pkg/scheduler/backend/queue/scheduling_queue.go` manages the scheduling lifecycle through three structured queues:

```
                  +-------------------------------------------------------------+
                  |                      Incoming New Pods                      |
                  +-------------------------------------------------------------+
                                                 |
                                                 v
                       +-------------------------------------------------+
                       |                     activeQ                     |
                       |       (Priority Heap: Priority + Arrival)       |
                       +-------------------------------------------------+
                                                 |
                                              Pop() (or direct pop from backoffQ)
                                                 |
                                                 v
                                           scheduleOne()
                                                 |
                       +-------------------------+-------------------------+
                       |                                                   |
                  (Schedulable)                                     (Unschedulable)
                       |                                                   |
                     Bind                                      PostFilter Preemption?
                                                                           |
                                                 +-------------------------+-------------------------+
                                                 |                                                   |
                                           (Preempting)                                      (No Candidates)
                                                 |                                                   |
                                     Hold in unschedulableEntities                      Move to podBackoffQ /
                                     (Gated by PreEnqueue check)                        unschedulableEntities
                                                 |                                                   |
                                        Victims Terminated /                               Cluster Event Occurs /
                                        Cluster Event Fires                                Backoff Timer Expires
                                                 |                                                   |
                                                 +-------------------------+-------------------------+
                                                                           |
                                                                           v
                                                       MoveAllToActiveOrBackoffQueue(...)
```

### 5.1 Queue Topology & Storage Subsystems
1. **`activeQ`**: A priority heap storing pods ready for immediate scheduling, ordered by priority and timestamp.
2. **`podBackoffQ`**: A heap storing pods that recently failed scheduling. Ordered by `backoffUntil` timestamp (calculated via exponential backoff with jitter based on `podInfo.Attempts`).
3. **`unschedulableEntities`**: A high-efficiency indexed key-value pool (`pod.UID` -> `*framework.QueuedPodInfo`) for pods awaiting cluster state changes.

### 5.2 Event-Driven Requeuing & `QueueingHint` Filtering
Legacy schedulers periodically flushed all unschedulable pods to `activeQ` on a 30-second timer, leading to massive CPU spikes. KEP-5142 replaces periodic flushing with event-driven movement:
- **Cluster Event Registration**: Events like `AssignedPodDelete`, `NodeResourceFitChanged`, `NodeTolerationChanged`, and `StorageClassAdd` emit specific cluster event metadata.
- **`QueueingHint` Execution**: Registered plugins evaluate whether a given cluster event could plausibly make the specific unschedulable pod schedulable.
- **`PreEnqueue` Validation**: Before moving a pod from `unschedulableEntities` to `activeQ` or `podBackoffQ`, the scheduler evaluates `PreEnqueue` plugins. If a pod is marked in-flight by `Executor.IsPodRunningPreemption()`, it remains safely gated in `unschedulableEntities`.

### 5.3 `SchedulerPopFromBackoffQ` Feature Gate
When `SchedulerPopFromBackoffQ` is enabled:
- `Pop()` checks if the top pod in `podBackoffQ` has completed its backoff duration.
- If expired, `Pop()` directly pops the pod from `podBackoffQ` without requiring an intermediate transfer step into `activeQ`, eliminating scheduling cycle delay.

### 5.4 In-Flight Event Recording (`inFlightEvents`)
To prevent lost wakeups during concurrent scheduling cycles:
- While a pod is being processed in `scheduleOne`, any cluster mutation events occurring concurrently on the cluster are recorded in `inFlightPods` / `inFlightEvents`.
- If scheduling fails, the scheduler cross-references in-flight events before depositing the pod in `unschedulableEntities`, ensuring events that occurred during the scheduling cycle immediately re-trigger evaluation.

---

## 6. Comprehensive Failure Modes, Race Conditions & Mitigation Matrix

| Failure Mode / Race Condition | Root Cause & Mechanism | Detection / Trigger Point | Architectural Mitigation & Guarantee |
| :--- | :--- | :--- | :--- |
| **FM-301: Partial Victim Deletion Failure** | API server returns non-404 error (`500`, `403`, timeout) while deleting an intermediate victim. | `prepareCandidateAsync` receives error on `errCh`. | Deletion of subsequent victims is immediately aborted. In-flight preemption sets are cleared, and `fh.Activate(preemptor)` is called via defer to requeue the preemptor to `backoffQ` without queue starvation. |
| **FM-302: Concurrent Victim Deletion (404 Not Found)** | Victim pod finishes execution or is deleted out-of-band before scheduler's `DELETE` reaches API server. | `schedutil.PatchPodStatus` or `schedutil.DeletePod` returns `apierrors.IsNotFound`. | `PreemptPod` catches 404 and treats it as successful deletion (`return false, nil`). Preemption proceeds to completion and preemptor schedules cleanly. |
| **FM-303: Inter-Preemptor Collision** | Lower-priority preemptor $P_1$ starts async preemption on Node $N$. Higher-priority preemptor $P_2$ arrives and selects Node $N$. | $P_2$ runs `PostFilter` and selects victims on Node $N$. | If $P_2$ completes first, $P_2$ claims the node. When $P_1$ finishes preemption, $P_1$ evaluates Node $N$, fails filters due to $P_2$, and is cleanly requeued with backoff. |
| **FM-304: Preemptor Deletion / Mutation Mid-Flight** | Preemptor pod is deleted or its spec/labels are updated in API server while its async preemption worker is executing. | Preemption worker completes and unblocks `PreEnqueue` / activation. | The preemption worker completes cleanly without nil pointer panics or goroutine leaks. If deleted, informer removes pod; if mutated, updated pod object is scheduled on next pop. |
| **FM-305: In-Memory Victim Permit / PreBind Race** | Victim pod is in `Permit` (`WaitingPod`) or `PreBind` phase when preemption triggers. | `e.fh.GetWaitingPod` or `e.fh.GetPodInPreBind` finds active in-memory victim. | Eviction executes in-memory via `waitingPod.Preempt` or `podInPreBind.CancelPod`. Skips API network calls (`preemptedInMemory = true`), immediately activating the preemptor. |
| **FM-306: Orphaned Gating on Crash** | Scheduler restarts or crashes while async preemption workers were active. | New scheduler leader starts up and initializes `PriorityQueue`. | In-memory `preempting` map starts fresh. Informer cache sync reconciles actual pod statuses and cleans stale nominations. |

---

## 7. Integration & Verification Test Suite

The preemption integration test framework (`test/integration/scheduler/preemption/`) provides comprehensive end-to-end verification with a live `kube-apiserver` and `etcd` backend.

### Key Integration Test Cases
1. **`TestAsyncPreemption_InterPreemptorCollision`**:
   - Tests concurrent arrival of competing preemptors ($P_1$ and $P_2$) with different priority levels, ensuring higher priority always takes precedence regardless of eviction completion ordering.
2. **`TestAsyncPreemption_PartialDeletionFailureRollback`**:
   - Validates that non-404 deletion errors halt subsequent victim deletions, clean up state, and activate the preemptor into the backoff queue.
3. **`TestAsyncPreemption_PreemptorDeletionDuringExecution`**:
   - Validates lifecycle safety when preemptor pods are deleted or mutated out-of-band while preemption goroutines are in-flight.
4. **`TestAsyncPreemption_Tolerate404NotFound`**:
   - Validates that victim pods deleted out-of-band returning 404 `IsNotFound` during single-victim and multi-victim preemption are handled idempotently without failing preemption.
5. **`TestAsyncPreemption_InMemoryVictimPreemption`**:
   - Validates that in-memory victim evictions (permits and pre-bind cancellations) bypass API network calls and immediately activate preemptors.
