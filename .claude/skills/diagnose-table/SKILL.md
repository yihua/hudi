---
name: diagnose-table
description: Health check a Hudi table. Use when a table is slow, has stuck operations, small files, or needs a diagnostic.
user-invocable: true
disable-model-invocation: true
allowed-tools: Read, Grep, Glob, Bash, Agent
argument-hint: [table-path or symptoms e.g. "/data/my_table" or "compaction stuck"]
---

# Diagnose Hudi Table

User's input: **$ARGUMENTS**

## Instructions

You are a Hudi production operations expert. Help diagnose the table's health.

### If a table path is provided:
Run these diagnostic checks using Spark SQL CALL procedures or direct filesystem inspection:

#### 1. Timeline Health
```sql
-- Show recent commits
CALL show_commits(path => '<table_path>', limit => 20);
-- Show timeline with all action types
CALL show_timeline(path => '<table_path>', limit => 50);
```
Look for:
- Gaps in commit times (indicates failed writes)
- INFLIGHT instants that never completed (stuck operations)
- REQUESTED compactions/clusterings that were never executed
- Ratio of delta_commits to compactions (for MoR tables)

#### 2. Pending Operations
```sql
-- Pending compactions
CALL show_compaction(path => '<table_path>', limit => 50);
-- Pending clustering
CALL show_clustering(path => '<table_path>', limit => 50);
```
Flag: More than 5 pending compactions means compaction is falling behind writes.

#### 3. File System Health
```sql
-- File sizes per partition
CALL stats_file_size(table => '<table_name>');
-- Write amplification
CALL stats_write_amplification(table => '<table_name>');
-- Invalid parquet files
CALL show_invalid_parquet(path => '<table_path>');
```

#### 4. Metadata Table Health
```sql
-- Metadata table stats
CALL show_metadata_table_stats(table => '<table_name>');
-- Validate metadata consistency
CALL validate_metadata_table_files(table => '<table_name>');
```

#### 5. Clean & Archive Status
```sql
CALL show_cleans(path => '<table_path>', limit => 10);
```
Check: Is cleaning keeping up? Are old file versions accumulating?

### If symptoms are described:
Map symptoms to likely causes:

| Symptom | Likely Cause | Check |
|---------|-------------|-------|
| Slow reads | Too many small files, missing compaction | File sizes, pending compactions |
| Slow writes | Lock contention, too many table services inline | Lock config, inline service configs |
| OOM during compaction | Large log files, wrong memory config | Log file sizes, `hoodie.memory.merge.max.size` |
| Stuck INFLIGHT | Writer crashed mid-operation | Heartbeat files, rollback needed |
| Growing .hoodie dir | Archival not keeping up | Archive config, `hoodie.keep.max.commits` |
| Query returns stale data | Sync lag, metadata stale | Metadata table health, sync status |

### Output format:
1. **Table State Summary** - Key metrics at a glance
2. **Issues Found** - Ordered by severity (critical first)
3. **Recommended Actions** - Specific commands/configs to fix each issue
4. **Preventive Configs** - Settings to prevent recurrence
