---
name: compare-cow-mor
description: Compare Copy-on-Write (CoW) and Merge-on-Read (MoR) table types in Hudi. Use when choosing table type, understanding performance tradeoffs, or migrating between types.
user-invocable: true
allowed-tools: Read, Grep, Glob, Agent
argument-hint: [workload e.g. "read-heavy", "update-heavy", "mixed workload", "streaming ingestion"]
---

# CoW vs MoR Table Comparison

Workload: **$ARGUMENTS**

## Instructions

Read the actual source code to ground comparisons in implementation details.

### Key implementation files:
- CoW commit: `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/commit/` (BaseCommitActionExecutor)
- MoR append: look for `AppendHandle`, `HoodieAppendHandle`
- File group reader: `hudi-common/src/main/java/org/apache/hudi/common/table/read/HoodieFileGroupReader.java`
- Log file format: `hudi-common/src/main/java/org/apache/hudi/common/table/log/`
- Table type enum: `hudi-common/src/main/java/org/apache/hudi/common/model/HoodieTableType.java`

### Comparison axes:
1. **Write path**: CoW rewrites full Parquet files vs MoR appends to log files
2. **Read path**: CoW reads Parquet directly vs MoR merges base + log files
3. **Write latency**: MoR much faster for updates (append-only)
4. **Read latency**: CoW faster (no merge), MoR depends on log size
5. **Write amplification**: CoW high (full file rewrite), MoR low (append)
6. **Storage overhead**: MoR has temporary duplication in logs until compaction
7. **Compaction**: Only MoR needs it - extra operational overhead
8. **Query types**: MoR supports read-optimized (base files only) as optimization

### For the user's workload, recommend:
- **Read-heavy, few updates**: CoW
- **Update-heavy, can tolerate read latency**: MoR
- **Streaming ingestion**: MoR (low write latency)
- **Batch ETL, full rewrites**: CoW or `insert_overwrite`
- **Mixed**: MoR with tuned compaction

### Provide:
1. **Recommendation** with reasoning grounded in their workload
2. **Config template** for the recommended type
3. **Performance expectations** (qualitative, based on code paths)
4. **Operational considerations** (compaction, clean, monitoring)
