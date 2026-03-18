---
name: debug-clustering
description: Debug clustering issues in Hudi tables. Use when clustering is slow, stuck, failing, or not improving query performance as expected.
user-invocable: true
allowed-tools: Read, Grep, Glob, Bash, Agent
argument-hint: [symptom e.g. "clustering not improving queries", "clustering OOM", "too many small files"]
---

# Debug Hudi Clustering

Problem: **$ARGUMENTS**

## Instructions

### Background
Clustering reorganizes data files for better query performance. Two phases:
1. **Plan** (`ClusteringPlanActionExecutor`) - Selects file groups to cluster and creates plan
2. **Execute** (`ExecuteClusteringActionExecutor`) - Rewrites data according to plan

Key source files:
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/cluster/ClusteringPlanActionExecutor.java`
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/config/HoodieClusteringConfig.java`
- Plan strategies: `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/clustering/plan/strategy/`
- Execution strategies: `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/clustering/run/strategy/`

### Key Configs
Read `HoodieClusteringConfig.java` for full details. Essential configs:
- `hoodie.clustering.inline` / `hoodie.clustering.async.enabled` - enable clustering
- `hoodie.clustering.inline.max.commits` - trigger threshold
- `hoodie.clustering.plan.strategy.class` - how files are selected
- `hoodie.clustering.execution.strategy.class` - how files are rewritten
- `hoodie.clustering.plan.strategy.sort.columns` - columns to sort by (critical for query perf)
- `hoodie.clustering.plan.strategy.target.file.max.bytes` - target output file size
- `hoodie.clustering.plan.strategy.small.file.limit` - files below this are candidates
- `hoodie.clustering.plan.strategy.max.num.groups` - max groups per plan
- `hoodie.clustering.plan.strategy.max.bytes.per.group` - max data per group

### Diagnostic approach

1. **Check clustering state:**
```sql
CALL show_clustering(path => '<path>', limit => 20);
```

2. **Not scheduling:** Verify trigger threshold, check if files meet small file limit criteria

3. **Not improving queries:**
   - Are sort columns aligned with query filter columns?
   - Is Z-ordering configured for multi-column predicates?
   - Check column stats overlap: `CALL show_column_stats_overlap(table => '<name>', columns => '<cols>');`

4. **OOM/slow execution:**
   - Reduce `max.bytes.per.group` and `max.num.groups`
   - Check target file size vs input data volume
   - Verify executor memory is sufficient for sort operations

5. **Interaction with other services:**
   - Clustering creates REPLACE_COMMIT actions
   - Pending clustering blocks cleaning of involved file groups
   - Check for conflicts with concurrent writes

### Output
1. **Root cause** with evidence from timeline/configs
2. **Fix** - config changes or operational commands
3. **Query performance validation** - how to verify improvement
