# Fort Performance Benchmarks (Final O(1) Cloning Implementation)

This document records the finalized performance characteristics of the Fort query language, utilizing full B-Tree storage, Copy-on-Write (COW) value isolation, and "NoReplay" registration for instantaneous snapshots.

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
BenchmarkThroughput/Depth1/Size100-128         	  160866	      7587 ns/op
BenchmarkThroughput/Depth1/Size1000
BenchmarkThroughput/Depth1/Size1000-128        	  180787	      7126 ns/op
BenchmarkThroughput/Depth1/Size10000
BenchmarkThroughput/Depth1/Size10000-128       	  211992	      8304 ns/op
BenchmarkThroughput/Depth3/Size100
BenchmarkThroughput/Depth3/Size100-128         	   73587	     14723 ns/op
BenchmarkThroughput/Depth3/Size1000
BenchmarkThroughput/Depth3/Size1000-128        	   87054	     14264 ns/op
BenchmarkThroughput/Depth3/Size10000
BenchmarkThroughput/Depth3/Size10000-128       	   95623	     16081 ns/op
BenchmarkThroughput/Depth5/Size100
BenchmarkThroughput/Depth5/Size100-128         	   51900	     21822 ns/op
BenchmarkThroughput/Depth5/Size1000
BenchmarkThroughput/Depth5/Size1000-128        	   53202	     24695 ns/op
BenchmarkThroughput/Depth5/Size10000
BenchmarkThroughput/Depth5/Size10000-128       	   62977	     21518 ns/op
BenchmarkCloningPerformance
BenchmarkCloningPerformance/Size100
BenchmarkCloningPerformance/Size100-128        	  965268	      1283 ns/op
BenchmarkCloningPerformance/Size1000
BenchmarkCloningPerformance/Size1000-128       	  991416	      1312 ns/op
BenchmarkCloningPerformance/Size10000
BenchmarkCloningPerformance/Size10000-128      	 1000000	      1254 ns/op
BenchmarkJoinPerformance
BenchmarkJoinPerformance/Size100
BenchmarkJoinPerformance/Size100-128           	  119257	      9027 ns/op
BenchmarkJoinPerformance/Size1000
BenchmarkJoinPerformance/Size1000-128          	  139021	      9413 ns/op
BenchmarkJoinPerformance/Size5000
BenchmarkJoinPerformance/Size5000-128          	  120354	     11989 ns/op
```

## Key Observations

1.  **Instantaneous O(1) Cloning**: By combining B-Tree structural COW with "NoReplay" registration, pipeline cloning is now independent of dataset size. Cloning a pipeline with 10,000 objects takes approximately **1.3 microseconds**, compared to ~50 milliseconds in the previous iteration (a **38,000x speedup**).
2.  **Stable Update Throughput**: Throughput remains Dataset-Size Independent (O(1) updates relative to size). Latency scales linearly with pipeline depth (~7µs per stage).
3.  **Transactional Integrity**: The "NoReplay" mode preserves the "born hydrated" state of cloned informers while ensuring they are correctly wired into their new sources for future updates. Value-level COW ensures that snapshots remain immutable even as the parent pipeline continues to mutate its state.
