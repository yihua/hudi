---
name: trace-write-path
description: Trace the Hudi write path end-to-end for a specific operation (upsert, insert, bulk_insert, delete). Shows exactly what happens from Spark DataFrame write to files on storage. Use when debugging write issues or learning how writes work internally.
user-invocable: true
allowed-tools: Read, Grep, Glob, Agent
argument-hint: [operation e.g. "upsert", "insert", "bulk_insert", "delete", "insert_overwrite"]
---

# Trace Hudi Write Path

Operation: **$ARGUMENTS**

## Instructions

Trace the actual code path from the user's Spark API call down to file writes on storage. Read the source code at each step.

### Entry points to trace from:
1. **Spark DataSource V2**: `hudi-spark-datasource/hudi-spark/src/main/scala/org/apache/spark/sql/hudi/command/` (for SQL INSERT/UPDATE/MERGE)
2. **Spark DataSource V1**: `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/DefaultSource.scala` (for DataFrame `.write.format("hudi")`)
3. **Write client**: `hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/SparkRDDWriteClient.java`
4. **Base write client**: `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/BaseHoodieWriteClient.java`

### For each step in the path, document:
1. **Class.method()** with file:line reference
2. **What it does** in 1-2 sentences
3. **Key decisions** made at this step (e.g., index lookup, partition routing, small file selection)
4. **Error paths** - what exceptions can be thrown here

### Key phases to trace:
1. **Config resolution** - How write config is built from user options
2. **Schema resolution** - How writer schema is determined and validated
3. **Deduplication** - How duplicate records in the batch are handled
4. **Index lookup** - How existing records are located (tagged with file group)
5. **Partitioning** - How records are routed to file groups (new vs existing)
6. **Small file handling** - How the writer avoids creating small files
7. **File writing** - Actual Parquet/log file creation
8. **Commit** - Timeline instant creation, metadata update, commit callback
9. **Table services** - Inline compaction/clustering/clean if configured
10. **Post-commit** - Archive, metadata table update, sync

### CoW vs MoR differences:
Highlight where the code path diverges for Copy-on-Write vs Merge-on-Read tables.

### Output format:
Numbered sequence of steps, each with class:method reference, showing the complete flow.
