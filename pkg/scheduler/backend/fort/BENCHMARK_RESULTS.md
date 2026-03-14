# Fort Performance Benchmarks (with BTreeIndexer)

This document records the performance characteristics of the Fort query language after replacing the standard map-based `cache.Indexer` with a fast-cloneable `BTreeIndexer` based on `k8s.io/utils/third_party/forked/golang/btree`.

## Environment
- **OS**: Linux
- **Arch**: amd64
- **CPU**: AMD EPYC 7B13

## Results

```text
goos: linux
goarch: amd64
cpu: AMD EPYC 7B13
BenchmarkThroughput
BenchmarkThroughput/Depth1/Size100
BenchmarkThroughput/Depth1/Size100-128         	  184330	      7098 ns/op
BenchmarkThroughput/Depth1/Size1000
BenchmarkThroughput/Depth1/Size1000-128        	  170037	      6762 ns/op
BenchmarkThroughput/Depth1/Size10000
BenchmarkThroughput/Depth1/Size10000-128       	  204243	      7701 ns/op
BenchmarkThroughput/Depth3/Size100
BenchmarkThroughput/Depth3/Size100-128         	   73680	     13798 ns/op
BenchmarkThroughput/Depth3/Size1000
BenchmarkThroughput/Depth3/Size1000-128        	   84114	     13361 ns/op
BenchmarkThroughput/Depth3/Size10000
BenchmarkThroughput/Depth3/Size10000-128       	   98511	     15550 ns/op
BenchmarkThroughput/Depth5/Size100
BenchmarkThroughput/Depth5/Size100-128         	   50461	     22294 ns/op
BenchmarkThroughput/Depth5/Size1000
BenchmarkThroughput/Depth5/Size1000-128        	   52308	     26254 ns/op
BenchmarkThroughput/Depth5/Size10000
BenchmarkThroughput/Depth5/Size10000-128       	   61566	     20033 ns/op
BenchmarkCloningPerformance
BenchmarkCloningPerformance/Size100
BenchmarkCloningPerformance/Size100-128        	    3883	    294530 ns/op
BenchmarkCloningPerformance/Size1000
BenchmarkCloningPerformance/Size1000-128       	     321	   3402508 ns/op
BenchmarkCloningPerformance/Size10000
BenchmarkCloningPerformance/Size10000-128      	      28	  45289084 ns/op
BenchmarkJoinPerformance
BenchmarkJoinPerformance/Size100
BenchmarkJoinPerformance/Size100-128           	  147663	      7955 ns/op
BenchmarkJoinPerformance/Size1000
BenchmarkJoinPerformance/Size1000-128          	  160638	      7567 ns/op
BenchmarkJoinPerformance/Size5000
BenchmarkJoinPerformance/Size5000-128          	  158865	      8884 ns/op
```

## Key Observations

1.  **Update Throughput**: Updates are slightly slower than the map-based indexer (~7µs vs ~4µs for Depth 1) due to the O(log N) complexity and higher constant factors of the B-Tree structure. However, performance remains highly predictable and dataset-size independent.
2.  **Cloning Efficiency**: `BTreeIndexer.Clone()` is an O(1) structural copy thanks to Copy-on-Write logic. While overall pipeline cloning remains O(N) due to deep-copying of internal maps in `joiner` and `grouper`, the time to clone 10,000 objects decreased from ~51ms to ~45ms.
3.  **Reliability**: The system maintains full transactional consistency and passes all functional tests with the new indexer implementation.
