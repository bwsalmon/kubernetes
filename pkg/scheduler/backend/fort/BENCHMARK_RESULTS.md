# Fort Performance Benchmarks

This document records the performance characteristics of the Fort query language, specifically focusing on update throughput and cloning latency under varying pipeline depths and dataset sizes.

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
BenchmarkThroughput/Depth1/Size100-128         	  321056	      4304 ns/op
BenchmarkThroughput/Depth1/Size1000
BenchmarkThroughput/Depth1/Size1000-128        	  334491	      3996 ns/op
BenchmarkThroughput/Depth1/Size10000
BenchmarkThroughput/Depth1/Size10000-128       	  335702	      3923 ns/op
BenchmarkThroughput/Depth3/Size100
BenchmarkThroughput/Depth3/Size100-128         	  163383	      7714 ns/op
BenchmarkThroughput/Depth3/Size1000
BenchmarkThroughput/Depth3/Size1000-128        	  154575	      8392 ns/op
BenchmarkThroughput/Depth3/Size10000
BenchmarkThroughput/Depth3/Size10000-128       	  149044	      7700 ns/op
BenchmarkThroughput/Depth5/Size100
BenchmarkThroughput/Depth5/Size100-128         	  110688	     10213 ns/op
BenchmarkThroughput/Depth5/Size1000
BenchmarkThroughput/Depth5/Size1000-128        	   99973	     10481 ns/op
BenchmarkThroughput/Depth5/Size10000
BenchmarkThroughput/Depth5/Size10000-128       	  126168	     10963 ns/op
BenchmarkCloningPerformance
BenchmarkCloningPerformance/Size100
BenchmarkCloningPerformance/Size100-128        	    2853	    390826 ns/op
BenchmarkCloningPerformance/Size1000
BenchmarkCloningPerformance/Size1000-128       	     273	   4017568 ns/op
BenchmarkCloningPerformance/Size10000
BenchmarkCloningPerformance/Size10000-128      	      24	  51434009 ns/op
BenchmarkJoinPerformance
BenchmarkJoinPerformance/Size100
BenchmarkJoinPerformance/Size100-128           	  256362	      4988 ns/op
BenchmarkJoinPerformance/Size1000
BenchmarkJoinPerformance/Size1000-128          	  289636	      5058 ns/op
BenchmarkJoinPerformance/Size5000
BenchmarkJoinPerformance/Size5000-128          	  267808	      5160 ns/op
```

## Key Observations

1.  **Update Scalability**: Individual updates take between 4µs and 11µs depending on pipeline depth. Throughput is O(1) with respect to the number of objects in the system, confirming the efficiency of the Transactional Domain Locking and map-indexed stores.
2.  **Cloning Cost**: Full state-aware cloning scales linearly (O(N)) with object count. A consistent snapshot of 10,000 objects across a 3-stage pipeline takes approximately 51ms.
3.  **Join Efficiency**: Joins add minimal overhead, with updates taking ~5µs regardless of the size of the join sets (tested up to 5,000 items).
