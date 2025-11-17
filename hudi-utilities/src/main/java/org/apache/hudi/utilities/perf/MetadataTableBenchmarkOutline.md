# Metadata Table Performance Microbenchmarking Outline

## Overview
Comprehensive benchmarking scenarios for Hudi metadata table read and write performance, focusing on identifying bottlenecks and memory inefficiencies.

## 1. READ Performance Benchmarks

### 1.1 HoodieBackedTableMetadata API Reads

#### 1.1.1 Point Lookups
- **Single key lookup** from different partitions (files, column_stats, bloom_filters, record_index)
- **Batch key lookups** with varying batch sizes (10, 100, 1000, 10000 keys)
- **Non-existent key lookups** (measure overhead of failed lookups)
- **Metrics**: Latency, throughput, memory usage

#### 1.1.2 Range Scans
- **getAllFilesInPartition()** with varying partition sizes:
  - Small: 10-100 files
  - Medium: 1,000 files
  - Large: 10,000 files
  - Very Large: 100,000 files
- **getAllPartitionPaths()** with varying partition counts
- **getColumnStats()** for different column counts
- **Metrics**: Scan time, memory consumption, GC pressure

#### 1.1.3 Secondary Index Reads
- **Point lookups** in secondary index with varying index sizes
- **Range queries** on secondary index
- **Multi-value lookups** (one secondary key → multiple record keys)
- **Metrics**: Query latency, index traversal time

### 1.2 FileGroupReader Performance

#### 1.2.1 Base File Only Reads
- **Small base files** (< 10MB)
- **Medium base files** (10MB - 100MB)
- **Large base files** (100MB - 1GB)
- **With/without column pruning**
- **With/without predicate pushdown**

#### 1.2.2 Base + Log Files Reads
- **Varying log file counts** (0, 1, 5, 10, 20 log files)
- **Varying log block sizes**:
  - Small blocks (< 1MB)
  - Medium blocks (1-10MB)
  - Large blocks (10-100MB)
  - Very large blocks (> 100MB) - stress test for the inefficiency we found
- **Different log block types**:
  - Data blocks only
  - Delete blocks only
  - Mixed data/delete blocks
  - CDC blocks

#### 1.2.3 Delete Block Processing (Critical Path)
- **Small delete blocks** (< 1K deletes)
- **Medium delete blocks** (1K - 100K deletes)
- **Large delete blocks** (100K - 1M deletes)
- **Very large delete blocks** (> 1M deletes, ~400MB) - reproduce OOM scenario
- **Metrics**: Deserialization time, peak memory usage, GC pauses

#### 1.2.4 Record Merging Performance
- **No updates** (all inserts)
- **Few updates** (10% records updated)
- **Heavy updates** (50% records updated)
- **Mostly updates** (90% records updated)
- **With/without record positions**

### 1.3 Concurrent Read Scenarios

#### 1.3.1 Multi-threaded Reads
- **Concurrent readers** on same file group (1, 2, 4, 8, 16 threads)
- **Concurrent readers** on different file groups
- **Mixed read patterns** (point lookups + range scans)

#### 1.3.2 Cache Performance
- **Cold cache** reads (first access)
- **Warm cache** reads (repeated access)
- **Cache eviction** scenarios
- **Cache hit ratio** under different workloads

## 2. WRITE Performance Benchmarks

### 2.1 HoodieAppendHandle (Append-only writes)

#### 2.1.1 Single Writer Performance
- **Small batches** (10-100 records)
- **Medium batches** (1K-10K records)
- **Large batches** (10K-100K records)
- **Very large batches** (> 100K records)
- **Different payload sizes**:
  - Small payloads (< 1KB)
  - Medium payloads (1-10KB)
  - Large payloads (> 10KB)

#### 2.1.2 Log Block Creation
- **Data block creation** rate
- **Delete block creation** with varying delete counts
- **Block compression** overhead (different compression algorithms)
- **Block rollover** scenarios (hitting size limits)

#### 2.1.3 Memory Management
- **Write buffer** utilization
- **Spilling to disk** scenarios
- **Memory pressure** handling
- **Peak memory** during flush

### 2.2 FileGroupReaderBasedMergeHandle (Compaction)

#### 2.2.1 Base File Only Compaction
- **Small file compaction** (< 100MB)
- **Medium file compaction** (100MB - 1GB)
- **Large file compaction** (> 1GB)
- **Metrics**: Read time, write time, total time, memory usage

#### 2.2.2 Base + Log Compaction
Using actual files like:
- Base: `/Users/ethan/Work/tmp/20251114-secondary-index/secondary-index-key-0008-0_49-132-30288_20251014084908397.hfile`
- Log: `/Users/ethan/Work/tmp/20251114-secondary-index/.secondary-index-key-0008-0_20251014085650395.log.1_49-176-40052`

Scenarios:
- **Few log files** (1-2 files)
- **Many log files** (10-20 files)
- **Large log blocks** (stress memory management)
- **High update rate** (many overwrites)

#### 2.2.3 Delete Processing During Compaction
- **No deletes**
- **Few deletes** (< 1% of records)
- **Moderate deletes** (10% of records)
- **Heavy deletes** (> 50% of records)
- **Large delete blocks** (400MB scenario)

#### 2.2.4 Callback Processing
- **CDC callback** overhead
- **Record index callback** overhead
- **Secondary index callback** overhead
- **Multiple callbacks** combined

### 2.3 Concurrent Write Scenarios

#### 2.3.1 Multi-partition Writes
- **Single partition** writes
- **Few partitions** (2-5)
- **Many partitions** (10-20)
- **Metrics**: Throughput per partition, lock contention

#### 2.3.2 Write Amplification
- **Small updates** triggering compaction
- **Cascading compactions**
- **Write amplification factor** measurement

## 3. End-to-End Scenarios

### 3.1 Mixed Workloads
- **Read-heavy** (90% reads, 10% writes)
- **Write-heavy** (10% reads, 90% writes)
- **Balanced** (50% reads, 50% writes)
- **Bursty** (alternating read/write phases)

### 3.2 Metadata Table Growth Scenarios
- **Small table** (< 1GB metadata)
- **Medium table** (1-10GB metadata)
- **Large table** (10-100GB metadata)
- **Very large table** (> 100GB metadata)

### 3.3 Failure & Recovery
- **Reader failure** during large scan
- **Writer failure** during compaction
- **OOM recovery** scenarios
- **Partial write** handling

## 4. Specific Performance Anti-patterns to Test

### 4.1 Memory Inefficiencies
- **Double buffering** in delete block deserialization (as identified)
- **Large array allocations** without streaming
- **Memory leaks** in long-running operations
- **Unnecessary object creation** in hot paths

### 4.2 I/O Inefficiencies
- **Small, frequent reads** vs batched reads
- **Redundant file opens**
- **Missing compression** opportunities
- **Inefficient seek** patterns

### 4.3 CPU Inefficiencies
- **Excessive deserialization** overhead
- **Inefficient sorting** algorithms
- **Missing vectorization** opportunities
- **Lock contention** hotspots

## 5. Benchmark Implementation Guidelines

### 5.1 Test Data Generation
```java
// Example data patterns to generate
- Uniform distribution (even spread of keys)
- Skewed distribution (hot keys)
- Sequential patterns (time-series like)
- Random patterns
- Real-world patterns (from production traces)
```

### 5.2 Metrics to Collect
- **Latency**: p50, p90, p95, p99, p999
- **Throughput**: ops/sec, MB/sec
- **Memory**: heap usage, off-heap usage, GC metrics
- **CPU**: utilization, thread contention
- **I/O**: read/write bytes, IOPS, queue depth

### 5.3 JVM Settings to Test
```bash
# Small heap (stress memory management)
-Xmx2g -Xms2g

# Medium heap (typical production)
-Xmx8g -Xms8g

# Large heap (high-memory scenarios)
-Xmx16g -Xms16g

# Different GC algorithms
-XX:+UseG1GC
-XX:+UseZGC
-XX:+UseShenandoahGC
```

### 5.4 Configuration Variations
- `hoodie.metadata.max.logfile.size`: 128MB, 512MB, 1GB
- `hoodie.memory.compaction.max.size`: 1GB, 2GB, 4GB
- `hoodie.metadata.cleaner.commits.retained`: 10, 20, 50
- `hoodie.metadata.record.merge.mode`: POSITIONAL vs KEY_BASED

## 6. Benchmark Execution Plan

### Phase 1: Baseline Establishment
1. Run all benchmarks with default configurations
2. Identify top 10 slowest operations
3. Profile memory usage patterns

### Phase 2: Stress Testing
1. Push boundaries with large data sizes
2. Reproduce known issues (400MB delete block OOM)
3. Find breaking points for each component

### Phase 3: Optimization Validation
1. Test with proposed fixes
2. Compare before/after performance
3. Verify no regression in other areas

### Phase 4: Production Simulation
1. Use real production data patterns
2. Simulate production load profiles
3. Long-running stability tests (24-48 hours)

## 7. Expected Outputs

### 7.1 Performance Report
- Detailed metrics for each scenario
- Comparison charts and graphs
- Bottleneck identification
- Optimization recommendations

### 7.2 Code Artifacts
- JMH benchmark classes
- Data generation utilities
- Result analysis scripts
- Reproduction steps for issues found

### 7.3 Documentation
- Performance tuning guide
- Configuration recommendations
- Best practices for different workloads
- Known limitations and workarounds

## 8. Success Criteria

- **Read latency**: < 10ms for point lookups
- **Write throughput**: > 100K records/sec for appends
- **Compaction speed**: > 100MB/sec
- **Memory efficiency**: No OOM with 2GB heap for 100MB files
- **Scalability**: Linear scaling up to 10 concurrent operations

## Notes

- Focus extra attention on delete block processing given the identified inefficiency
- Consider using actual production data files for realistic testing
- Monitor for memory leaks in long-running tests
- Document any unexpected behavior or performance cliffs
- Test with both HFile and Parquet base file formats