# Multi-Stage Lock-Free Message Router

## Overview

Build a high-performance message routing system that processes millions of messages per second through multiple stages without using any locks. This simulates a real trading system where market data flows through processing stages before reaching trading strategies.

## System Architecture

Your system will have three types of components connected in a pipeline:

1. **Producers** (4-8 threads) → Generate messages at high speed
2. **Processors** (4-8 threads) → Transform messages with simulated work
3. **Strategies** (2-4 threads) → Final consumers that validate ordering

Messages flow: Producers → Stage1 Router → Processors → Stage2 Router → Strategies

```
┌──────────┐     ┌────────┐     ┌──────────┐     ┌────────┐     ┌──────────┐
│Producer 0│────►│        │────►│Processor0│────►│        │────►│Strategy 0│
├──────────┤     │Primary │     ├──────────┤     │Second  │     ├──────────┤
│Producer 1│────►│        │────►│Processor1│────►│        │────►│Strategy 1│
├──────────┤     │Router  │     ├──────────┤     │Router  │     ├──────────┤
│Producer 2│────►│        │────►│Processor2│────►│        │────►│Strategy 2│
├──────────┤     │        │     ├──────────┤     │        │     └──────────┘
│Producer 3│────►│        │────►│Processor3│────►│        │
└──────────┘     └────────┘     └──────────┘     └────────┘
                 (Stage 1)       (Processing)      (Stage 2)      (End Consumers)

```

## Core Requirements

### Messages

Each message contains:

- Message type (0-7)
- Producer ID
- Sequence number (incrementing per producer)
- Timestamp

After processing, messages also include:

- Processor ID
- Processing timestamp

### Routing Rules

**Stage 1**: Routes messages from producers to processors based on message type

- Example: All type-0 messages go to processor-0

**Stage 2**: Routes processed messages to strategies based on message type

- Example: All type-0 messages go to strategy-0

### Critical Requirement: Message Ordering

Messages from the same producer with the same type must arrive at strategies in order.

Example:

- Producer-0 sends: type-0 seq-1, type-0 seq-2, type-0 seq-3
- Strategy must receive them as: seq-1, seq-2, seq-3 (never out of order)
- Messages from different producers can interleave

## Performance Targets

```
MetricTargetTotal Throughput10+ million messages/secondEnd-to-End Latency (p99)< 5 microsecondsMessage LossZeroOrdering ViolationsZero
```

### Configuration File (`config.json`)

```json
{
    "scenario": "baseline",
    "duration_secs": 10,
    "producers": {
        "count": 4,
        "messages_per_sec": 1000000,
        "distribution": {
            "msg_type_0": 0.25,
            "msg_type_1": 0.25,
            "msg_type_2": 0.25,
            "msg_type_3": 0.25
        }
    },
    "processors": {
        "count": 4,
        "processing_times_ns": {
            "msg_type_0": 100,
            "msg_type_1": 100,
            "msg_type_2": 100,
            "msg_type_3": 100
        }
    },
    "strategies": {
        "count": 3,
        "processing_times_ns": {
            "strategy_0": 100,
            "strategy_1": 100,
            "strategy_2": 100
        }
    },
    "stage1_rules": [
        {"msg_type": 0, "processors": [0]},
        {"msg_type": 1, "processors": [1]},
        {"msg_type": 2, "processors": [2]},
        {"msg_type": 3, "processors": [3]}
    ],
    "stage2_rules": [
        {"msg_type": 0, "strategy": 0, "ordering_required": true},
        {"msg_type": 1, "strategy": 1, "ordering_required": true},
        {"msg_type": 2, "strategy": 2, "ordering_required": true},
        {"msg_type": 3, "strategy": 0, "ordering_required": true}
    ]
}
```

## Test Scenarios

You must handle these test patterns:

### 1. Baseline (10 seconds)

- 4 producers × 1M messages/second = 4M total/second
- Even distribution across 4 message types
- Verify: All messages delivered in order
- **Configuration File**: `baseline.json`

### 2. Hot Message Type (15 seconds)

- 70% of messages are type-0 (creating hotspot)
- 10% each for types 1-3
- Verify: System handles imbalanced load without degradation
- **Configuration File**: `hot_type.json`

### 3. Burst Traffic (20 seconds)

- Alternating pattern every 2 seconds:
    - 200ms burst: 5× normal rate (20M messages/second)
    - 1800ms quiet: 0.5× normal rate (2M messages/second)
- Verify: Queues handle bursts without loss
- **Configuration File**: `burst_pattern.json`

### 4. Imbalanced Processing (15 seconds)

- Different processing times per message type:
    - Type-0: 50 nanoseconds (fast)
    - Type-1: 500 nanoseconds (medium)
    - Type-2: 2000 nanoseconds (slow - will bottleneck)
    - Type-3: 100 nanoseconds (fast)
- Verify: Slow processor doesn't block others
- **Configuration File**: `imbalanced_processing.json`

### 5. Ordering Stress Test (10 seconds)

- All producers send only type-0 messages
- All go through same processor to same strategy
- 8M messages/second total
- Verify: Perfect ordering despite extreme contention
- **Configuration File**: `ordering_stress.json`

### 6. Strategy Bottleneck (20 seconds)

- Strategy-0 processes slowly (1000ns per message)
- Strategies 1-2 process fast (50ns per message)
- Verify: Backpressure handling without message loss
- **Configuration File**: `strategy_bottleneck.json`

## Benchmarks

You must use **Google Benchmark** library for all performance measurements. This ensures consistent, reliable measurements across different submissions.

- **Queue Performance Benchmark**
    - Throughput of your lock-free queue(s) in isolation
- **Routing Latency Benchmark**
    - Overhead of routing logic vs direct queue access
- **Memory Allocation Benchmark**
    - Allocation patterns during steady state
    - Memory usage under different load scenarios
    - Impact of queue sizes on performance
- **Scaling Benchmark**
    - Performance with different numbers of producers (1, 2, 4, 8)
    - Performance with different numbers of processors (1, 2, 4, 8)
    - Identify scaling bottlenecks

## Docker Requirements

### Docker Setup

Your solution must include a complete Docker environment:

```json
project/
├── Dockerfile              # Multi-stage build
├── docker-compose.yml      # Orchestrates all tests
├── configs/               # All test configuration files
├── src/                   # Source code
├── benchmarks/            # Benchmark code
└── scripts/               # Helper scripts
```

### Dockerfile Requirements

- Base image: Ubuntu 22.04 or newer
- Multi-stage build (separate build and runtime stages)
- Install all required dependencies
- Compile with optimization flags (-O3, -march=native)
- Set up runtime environment with proper CPU settings

### Docker Compose

Provide `docker-compose.yml` that allows running:

```bash
*# Build the project*
docker-compose build

*# Run all test scenarios*
docker-compose run router-test

*# Run specific scenario*
docker-compose run router-test ./run_test baseline.json

*# Run benchmarks*
docker-compose run router-benchmark

*# Run specific benchmark*
docker-compose run router-benchmark queue_perf
```

### Container Requirements

- Mount results directory for output files
- Set CPU and memory limits appropriately
- Configure for performance (disable CPU throttling if possible)
- Output all results to mounted volume

### Example Usage

```bash
*# Clone repository*
git clone <your-repo>
cd message-router

*# Build Docker image*
docker-compose build

*# Run all scenarios (outputs to ./results/)*
docker-compose run router-test

*# Run benchmarks (outputs to ./results/benchmarks/)*
docker-compose run router-benchmark

*# View results*
cat results/baseline_summary.txt
cat results/benchmarks/queue_performance.txt
```

## Implementation Requirements

### Must Have

1. **Lock-Free Design**
    - No mutexes, semaphores, or condition variables
    - Use atomic operations only
    - Implement or use existing lock-free queues (SPSC/MPSC/MPMC)
2. **Configuration**
    - Read test scenarios from JSON files
    - Configurable producer rates and distributions
    - Configurable routing rules
3. **Monitoring**
    - Real-time stats every second (throughput, queue depths, latency)
    - Final summary with percentiles (p50, p90, p99, p99.9)
    - Ordering validation report
4. **Testing**
    - Run all 6 scenarios
    - Validate zero message loss
    - Validate ordering guarantees
    - Measure latency at each stage

### Architecture Guidelines

- Use separate threads for each producer, processor, and strategy
- Use lock-free queues between stages
- Minimize memory allocations on hot path
- Consider cache-line alignment for frequently accessed data
- Use busy-waiting for lowest latency (no sleep)

## Output Format

### During Execution (every second)

```bash
[1.00s] Produced: 4.00M | Processed: 3.98M | Delivered: 3.95M | Lost: 0
        Stage1 Queues: [256, 312, 298, 189] | Stage2 Queues: [512, 234, 445]
        Latencies(μs) - Stage1: 0.34 | Processing: 0.18 | Stage2: 0.41 | Total: 1.23
```

### Final Report

```bash
=== PERFORMANCE SUMMARY ===
Scenario: baseline
Duration: 10.00 seconds

Message Statistics:
  Total Produced:     40,000,000
  Total Processed:    40,000,000
  Total Delivered:    40,000,000
  Messages Lost:      0

Latency Percentiles (microseconds):
  Stage      p50    p90    p99    p99.9   max
  Stage1    0.12   0.23   0.45    1.2   15.3
  Process   0.15   0.18   0.21    0.5    2.1
  Stage2    0.18   0.31   0.52    1.1   12.1
  Total     0.51   0.89   1.45    3.2   28.4

Ordering Validation:
  Producer 0: 10,000,000 messages - IN ORDER ✓
  Producer 1: 10,000,000 messages - IN ORDER ✓
  Producer 2: 10,000,000 messages - IN ORDER ✓
  Producer 3: 10,000,000 messages - IN ORDER ✓
  
Test Result: PASSED
```

## Deliverables

### 1. Source Code

- Complete implementation
- Clear separation between components
- Comments explaining key design decisions

### 2. Docker Environment

- Dockerfile with multi-stage build
- docker-compose.yml for easy execution
- All dependencies properly configured

### 3. Benchmarks

- Standalone benchmark executables
- Benchmark results in `results/benchmarks/`
- Analysis of benchmark findings

### 4. Test Results

- Logs from all 6 scenarios in `results/`
- Performance analysis document explaining:
    - Bottlenecks identified
    - Optimizations attempted
    - Design trade-offs made

### 5. Documentation

- README with Docker build/run instructions
- Architecture document explaining your lock-free design
- How you handle backpressure
- Memory management strategy

## Evaluation Criteria

1. **Correctness** (40%)
    - Zero message loss
    - Perfect ordering preservation
    - Handling all test scenarios
2. **Performance** (30%)
    - Meeting latency targets
    - Achieving throughput goals
    - Efficient CPU usage
3. **Code Quality** (20%)
    - Clean, readable code
    - Proper error handling
    - Good architectural decisions
4. **Analysis** (10%)
    - Understanding of bottlenecks
    - Quality of performance measurements
    - Insights in documentation

## Hints

- Start with a simple version that works correctly, then optimize
- Profile first before optimizing - measure everything
- Consider queue sizes carefully - too small causes loss, too large wastes memory
- Test ordering validation thoroughly - it's easy to miss edge cases
- Use high-resolution timers for microsecond measurements
- Consider CPU affinity for threads to reduce context switching
- In Docker, be careful with CPU governor settings for consistent benchmarks

## Technical Environment

- Linux (Ubuntu 22+ recommended)
- C++17 minimum, C++20/23 preferred
- Clang 19+
- JSON library of your choice for configuration
- Docker 20+ and Docker Compose 2+

---

**This task tests your ability to build the kind of ultra-low latency systems used in real trading environments. Focus on correctness first, then optimize for performance. We will evaluate your solution by running your Docker containers on our hardware.**
