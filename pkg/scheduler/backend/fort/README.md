# Fort: Informer Query Language

Fort is a query language built on top of Kubernetes `SharedInformers`. It allows developers to define complex data transformations, joins, and aggregations using a declarative, SQL-like API while maintaining the eventually consistent, event-driven nature of Kubernetes informers.

## Core Concepts

### Query Informers
All queries return a `CloneableSharedInformerQuery`, which extends the standard `cache.SharedInformer`. This means query results can be consumed by anything that accepts a standard informer (e.g., controllers, listers, or other Fort queries).

### Operators
- **Select**: Basic transformation and filtering (one-to-one).
- **FlatMap**: One-to-many transformations. Useful for expanding nested collections or generating multiple derived objects from one source.
- **Join**: Many-to-many joins between two informers. Supports custom join keys (standardized as comparable arrays) and join filters.
- **GroupBy**: Aggregates objects from a source informer into groups. Supports:
    - `Count()`: Number of items in a group.
    - `Sum(val)`: Sum of a specific field.
    - `Distinct(val)`: Collection of unique values in a group.
    - `AnyValue(val)`: Pick one value from the group.

### ManualSharedInformer
A specialized informer that allows manual triggering of events (`OnAdd`, `OnUpdate`, `OnDelete`). This is the primary tool for:
1. **Testing**: Injecting controlled states into a query pipeline.
2. **Simulations**: Running "what-if" scenarios by cloning live queries and replacing their sources with manual informers.

## Chaining and Synchronization
Fort queries are designed to be chained. An `alerts` informer can be built on a `userTotals` informer, which is built on a `userOrders` join. 

- **Sync Propagation**: A chained informer will only report `HasSynced() == true` once all its upstream sources have synced.
- **Update Matching**: Fort uses a `KeyFunc` (matching Kubernetes standards) to identify objects. In `OnUpdate` calls, it intelligently matches old and new results to emit surgical `Update` events rather than a `Delete`+`Add` cycle, minimizing churn in downstream handlers.

## Usage Example

```go
// Define a GroupByJoin to count pods per service on each node
serviceNodes := QueryInformer(&GroupByJoin[*TServiceNode, *TPod, *TService]{
    Select: func(fields []GroupField) (*TServiceNode, error) {
        return &TServiceNode{
            Service: fields[0].(string),
            Node:    fields[1].(string),
            Count:   fields[2].(int64),
        }, nil
    },
    From: podInformer,
    Join: serviceInformer,
    On: func(pod *TPod, svc *TService) any {
        // Return a comparable array as the join key
        return [1]string{svc.Name} 
    },
    GroupBy: func(pod *TPod, svc *TService) (any, []GroupField) {
        // Group by [Service, Node]
        return [2]string{svc.Name, pod.NodeName},
            []GroupField{
                AnyValue(svc.Name),
                AnyValue(pod.NodeName),
                Count(),
            }
    },
})
```

## Implementation Details

### Locking
To ensure consistency when taking snapshots across multiple related informers, use `LockInformerSet`. This utility locks all underlying mutexes in a deterministic order and provides a single `Unlock()` call.

### Object Identity
Objects emitted by `ManualSharedInformer` or query informers should have a consistent identity. By default, Fort uses `cache.MetaNamespaceKeyFunc`, but custom `KeyFunc` implementations can be provided during informer creation to support non-Kubernetes-resource types.

## Performance and Memory Overhead

Fort is designed for high-performance simulations where cloning entire query pipelines must be nearly instantaneous and memory-efficient.

### Memory Efficiency
Based on automated benchmarks (see `memory_test.go`), the memory overhead for various operations is as follows:

| Operation | Scale | Total Memory | Overhead per Unit |
| :--- | :--- | :--- | :--- |
| **Source Only** | 100,000 items | 9.34 MB | ~98 bytes/item |
| **Source + Select** | 100,000 items | 18.74 MB | ~197 bytes/item |
| **Source + FlatMap (1:2)** | 100,000 items | 26.76 MB | ~281 bytes/item |
| **2 Sources + Join** | 100,000 items each | 60.24 MB | ~632 bytes/result |
| **Source + GroupBy** | 100,000 items / 1000 groups | 9.50 MB | ~100 bytes/item |
| **Cloning** | 1000 clones of 10,000 items | 0.49 MB | ~511 bytes/clone |

### Cloning Latency
Fort leverages B-Tree structural cloning (Copy-on-Write) and specialized "NoReplay" event registration to achieve O(1) snapshots of entire query domains.

#### Detailed Benchmark Results
The following results were collected on an AMD EPYC 7B13:

```text
BenchmarkCloningPerformance/Size10000-128      	  745552	      1688 ns/op

BenchmarkThroughput/Depth1/Size10000-128       	  245874	      5572 ns/op
BenchmarkThroughput/Depth3/Size10000-128       	  136897	     10708 ns/op
BenchmarkThroughput/Depth5/Size10000-128       	  102446	     13686 ns/op

BenchmarkJoinPerformance/Size5000-128          	   88474	     19748 ns/op
```

#### Key Observations
1.  **Instantaneous O(1) Cloning**: By combining B-Tree structural COW with "NoReplay" registration, pipeline cloning is independent of dataset size. Cloning a pipeline with 1,000,000 objects takes approximately **2.8 microseconds**.
2.  **Stable Update Throughput**: Throughput remains Dataset-Size Independent. Update latency scales linearly with pipeline depth (~5µs per transformation stage).
3.  **Transactional Integrity**: Snapshots are "born hydrated" and immutable. Value-level COW ensures that snapshots remain consistent even as the parent pipeline continues to receive high-frequency updates.

