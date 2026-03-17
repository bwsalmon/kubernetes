# Fort: Informer Query Language

Fort is a query language built on top of Kubernetes `SharedInformers`. It allows developers to define complex data transformations, joins, and aggregations using a declarative, SQL-like API while maintaining the eventually consistent, event-driven nature of Kubernetes informers.

## Core Concepts

### Query Informers
All queries return a `CloneableSharedInformerQuery`, which extends the standard `cache.SharedInformer`. This means query results can be consumed by anything that accepts a standard informer (e.g., controllers, listers, or other Fort queries).

### Operators
- **Select**: Basic transformation and filtering (one-to-one).
- **FlatMap**: One-to-many transformations. Useful for expanding nested collections or generating multiple derived objects from one source.
- **Join**: Many-to-many joins between two informers. Supports custom join keys and join filters.
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
        // Return a comparable value as the join key
        return svc.Name 
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

### Shared Domain Locking
Fort informers that are part of the same query pipeline share a `LockGroup`. This ensures transactional consistency across the entire Directed Acyclic Graph (DAG) during event propagation and snapshots.

To safely clone a pipeline, use `SnapshotLockDomain(rootInformer)`. This acquires an exclusive lock on the entire domain, enabling a consistent O(1) structural clone of the underlying B-Trees.

### Storage Optimization
Fort queries are designed to be memory-efficient. Instead of maintaining redundant indexes for every stage of the pipeline, specialized informers like `Join` and `GroupBy` implement custom indexers that wrap their internal state:
- **Join**: Computes join results on-the-fly from its internal left/right B-Trees, avoiding the O(N^2) memory overhead of storing joined pairs.
- **GroupBy**: Directly serves results from its internal aggregation state.

This architecture is enabled by a `baseInformer` base class that provides standard event routing and registration logic without mandating a specific storage backend.

## Performance and Memory Overhead

Fort is optimized for high-performance simulations where cloning entire query pipelines must be nearly instantaneous and memory-efficient.

### Memory Efficiency
Based on automated benchmarks (see `memory_test.go`), the memory overhead for various operations is as follows:

| Operation | Scale | Total Memory | Overhead per Unit |
| :--- | :--- | :--- | :--- |
| **Source Only** | 100,000 items | 9.33 MB | ~98 bytes/item |
| **Source + Select** | 100,000 items | 18.75 MB | ~197 bytes/item |
| **Source + FlatMap (1:2)** | 100,000 items | 26.76 MB | ~281 bytes/item |
| **2 Sources + Join** | 100,000 items each | 49.35 MB | ~517 bytes/joined-result |
| **Source + GroupBy** | 100,000 items / 1000 groups | 9.49 MB | ~99 bytes/item |
| **Cloning** | 1000 clones of 10,000 items | 0.53 MB | ~557 bytes/clone |

*Note: Join memory overhead was reduced by ~18% through on-the-fly result computation.*

### Cloning Latency
Fort leverages B-Tree structural cloning (Copy-on-Write) and specialized "NoReplay" event registration to achieve O(1) snapshots of entire query domains.

#### Scalability at 1M+ Items
At a scale of 1,000,000 items, Fort maintains exceptional performance:
- **Cloning Latency**: ~2.8 microseconds per snapshot.
- **Update Latency**: ~28 microseconds for full propagation through a multi-stage pipeline.

#### Key Observations
1.  **Instantaneous O(1) Cloning**: Pipeline cloning is independent of dataset size.
2.  **Stable Update Throughput**: Update latency scales with pipeline depth, not dataset size.
3.  **Transactional Integrity**: Snapshots are "born hydrated" and immutable. Value-level COW ensures that snapshots remain consistent even as the parent pipeline continues to receive high-frequency updates.
