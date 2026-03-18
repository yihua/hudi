---
name: debug-compaction
description: Debug compaction issues in Hudi MoR tables. Use when compaction is slow, stuck, failing, producing small files, or not running when expected.
user-invocable: true
allowed-tools: Read, Grep, Glob, Bash, Agent
argument-hint: [symptom e.g. "compaction stuck inflight", "too many log files", "compaction OOM"]
---

# Debug Hudi Compaction

Problem: **$ARGUMENTS**

## Instructions

### Background (read from source code if needed)
Compaction in Hudi converts log files (delta writes) into new base files in MoR tables. Two phases:
1. **Schedule** (`ScheduleCompactionActionExecutor`) - Creates a compaction plan (REQUESTED instant)
2. **Execute** (`RunCompactionActionExecutor`) - Runs the plan, creates new base files (INFLIGHT -> COMPLETED)

Key source files:
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/compact/ScheduleCompactionActionExecutor.java`
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/compact/RunCompactionActionExecutor.java`
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/config/HoodieCompactionConfig.java`

### Diagnostic Steps

#### 1. Check compaction state
```sql
CALL show_compaction(path => '<path>', limit => 20);
CALL show_timeline(path => '<path>', limit => 50);
```

#### 2. Common issues and fixes

**Compaction not scheduling:**
- Check `hoodie.compact.inline` or async compaction is enabled
- Check `hoodie.compact.inline.max.delta.commits` (default: 5) - compaction triggers after this many delta commits
- For async: verify the compaction service is running

**Compaction stuck INFLIGHT:**
- Writer may have crashed. Check heartbeat files in `.hoodie/.heartbeat/`
- Rollback the stuck instant:
  ```sql
  CALL rollback_to_instant_time(table => '<name>', instant_time => '<stuck_instant>');
  ```
- Or rollback inflight table service:
  ```sql
  CALL run_rollback_inflight_table_service(table => '<name>');
  ```

**Compaction OOM:**
- Check log file sizes - large log files need more memory to merge
- Key configs:
  - `hoodie.memory.merge.max.size` (default: 1GB) - max memory for merge
  - `hoodie.compaction.memory.merge.fraction` - fraction of executor memory
  - `hoodie.memory.spillable.map.path` - spill directory (ensure sufficient disk)
- Consider: `hoodie.compaction.strategy` = `org.apache.hudi.table.action.compact.strategy.BoundedIOCompactionStrategy` with `hoodie.compaction.target.io` to limit IO per compaction

**Too many pending compactions:**
- Writes outpacing compaction throughput
- Increase compaction parallelism: `hoodie.compaction.max.num.writers`
- Increase delta commit threshold: `hoodie.compact.inline.max.delta.commits`
- Move to async compaction if using inline
- Consider log compaction as intermediate step

**Small files after compaction:**
- Check `hoodie.parquet.max.file.size` (default: 120MB)
- Check `hoodie.parquet.small.file.limit` (default: 104857600 = 100MB)
- Enable file sizing: `hoodie.copy.on.write.insert.auto.size` / record size estimation

**Log compaction vs regular compaction:**
- Log compaction (`LOG_COMPACT`) merges log files without creating new base files
- Useful as intermediate step when full compaction is expensive
- Config: `hoodie.compact.inline.log.compact` and `hoodie.log.compaction.inline.max.delta.commits`

### Output
1. **Root cause** of the compaction issue
2. **Fix** - specific commands or config changes
3. **Prevention** - configs to avoid recurrence
4. **Monitoring** - what to watch going forward
