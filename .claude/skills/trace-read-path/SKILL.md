---
name: trace-read-path
description: Trace the Hudi read path for different query types (snapshot, incremental, time-travel, read_optimized). Shows how Spark reads Hudi tables, file pruning, and record merging.
user-invocable: true
allowed-tools: Read, Grep, Glob, Agent
argument-hint: [query type e.g. "snapshot query", "incremental query", "time travel", "read_optimized"]
---

# Trace Hudi Read Path

Query type: **$ARGUMENTS**

## Instructions

Trace the actual code path from Spark SQL/DataFrame read to record output.

### Entry points:
1. **Spark SQL**: `hudi-spark-datasource/hudi-spark/src/main/scala/org/apache/spark/sql/hudi/` (Spark catalog integration)
2. **DataSource V2**: `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/` (HoodieDataSourceV2 readers)
3. **DataSource V1**: `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/DefaultSource.scala`
4. **File index**: `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieFileIndex.scala`
5. **Record merging**: `hudi-common/src/main/java/org/apache/hudi/common/table/read/` (HoodieFileGroupReader)

### Key phases to trace:
1. **Query type resolution** - How Hudi determines snapshot vs incremental vs time-travel
2. **Timeline resolution** - Which instant defines the query snapshot
3. **Partition pruning** - How partitions are filtered using predicates
4. **File pruning** - How data skipping uses column stats, bloom filters, record-level index
5. **File group selection** - Which file groups and file slices are read
6. **Base file reading** - Parquet/ORC file scanning with predicate pushdown
7. **Log file merging** (MoR) - How delta log records are merged with base file records
8. **Schema evolution** - How reader handles schema differences between files
9. **Record merging** - How multiple versions of a record are resolved

### Query type specifics:
- **Snapshot**: Latest committed state
- **Incremental**: Changes between two instants
- **Time-travel**: State at a specific instant (via `AS OF` syntax)
- **Read-optimized**: Base files only (MoR, skips log files)

### Performance-critical paths:
Highlight the data skipping and pruning optimizations, as these are the most impactful for query performance.
