#!/bin/bash
# run_benchmark.sh - Script to run specific benchmarks for the HFT system

set -e  # Exit on any error

# Check if benchmark name is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <benchmark_name>"
    echo "Available benchmarks:"
    echo "  queue_perf        - Queue performance benchmark"
    echo "  routing_latency   - Routing latency benchmark"
    echo "  memory_allocation - Memory allocation benchmark"
    echo "  scaling           - Scaling benchmark"
    echo "  all               - Run all benchmarks"
    exit 1
fi

BENCHMARK=$1

# Run the benchmark
echo "Running benchmark: $BENCHMARK"
/usr/local/bin/hft --benchmark $BENCHMARK
