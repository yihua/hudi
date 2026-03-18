---
name: openclaw-hudi
description: Design and build OpenClaw skills and MCP servers for analyzing and operating Hudi tables. Use when planning or implementing OpenClaw integration with Hudi for autonomous table monitoring, diagnostics, and natural language table operations.
user-invocable: true
disable-model-invocation: true
allowed-tools: Read, Grep, Glob, Bash, Agent
argument-hint: [task e.g. "design MCP server", "build monitoring skill", "setup autonomous agent"]
---

# OpenClaw + Hudi Integration

Task: **$ARGUMENTS**

## Architecture Overview

OpenClaw is an autonomous AI agent framework that can be integrated with Hudi tables through two pathways:

### Pathway 1: Hudi MCP Server (recommended)
Build an MCP (Model Context Protocol) server that exposes Hudi table operations as tools. This works with both OpenClaw and Claude Code.

**MCP Server Tools to implement:**

#### Read/Diagnostic Tools (safe, no side effects):
```
hudi_table_info(path) -> table type, version, schema, partition fields, record key, precombine
hudi_show_timeline(path, limit, action_filter) -> recent timeline instants
hudi_show_commits(path, limit) -> commit details with write stats
hudi_show_compactions(path) -> pending/completed compaction status
hudi_show_clustering(path) -> pending/completed clustering status
hudi_file_sizes(path) -> file size distribution and small file count
hudi_write_amplification(path) -> write amplification metrics
hudi_metadata_health(path) -> metadata table sync status
hudi_query_preview(path, sql_predicate, limit) -> preview query results
hudi_schema(path) -> current schema with evolution history
hudi_partition_stats(path) -> per-partition file counts and sizes
hudi_pending_operations(path) -> all REQUESTED/INFLIGHT instants
hudi_config_dump(path) -> effective configuration
```

#### Write/Action Tools (require confirmation):
```
hudi_run_compaction(path, instant) -> trigger compaction
hudi_run_clustering(path) -> trigger clustering
hudi_run_clean(path) -> trigger cleaning
hudi_rollback(path, instant) -> rollback a failed instant
hudi_repair_metadata(path, dry_run) -> repair metadata table
```

**Implementation approach:**
The MCP server wraps Spark SQL CALL procedures. Implementation options:
1. **Python + hudi-rs**: Use the Rust/Python Hudi reader for read-only operations (lightweight, no Spark needed)
2. **Python + PySpark**: Start a SparkSession with Hudi jars for full procedure access
3. **Java + Spark**: Native JVM implementation using HoodieTableMetaClient directly

### Pathway 2: OpenClaw Custom Skills
Build OpenClaw skills that leverage the MCP server or direct tools:

#### Skill: hudi-table-monitor
```yaml
# Autonomous monitoring skill
# Periodically checks table health and alerts on issues
triggers:
  - schedule: "*/15 * * * *"  # Every 15 minutes
actions:
  - Check pending operations count
  - Check file size distribution
  - Check compaction lag (delta commits since last compaction)
  - Check metadata table sync status
  - Alert if any metric exceeds threshold
```

#### Skill: hudi-natural-language-query
```yaml
# Convert natural language to Hudi operations
# "Show me the last 10 commits on my orders table"
# -> CALL show_commits(path => '/data/orders', limit => 10)
```

#### Skill: hudi-incident-responder
```yaml
# Automated incident response
# "My queries are slow on the orders table"
# -> Run diagnostic chain: file sizes, pending compactions, metadata health
# -> Identify root cause and suggest fix
```

#### Skill: hudi-capacity-planner
```yaml
# Analyze growth patterns and predict capacity needs
# Uses commit history to project storage growth, compaction resource needs
```

### Pathway 3: Claude Code MCP Integration
Add the Hudi MCP server to this project's `.mcp.json` for use directly in Claude Code:

```json
{
  "mcpServers": {
    "hudi": {
      "command": "python",
      "args": [".claude/mcp/hudi-mcp-server/server.py"],
      "env": {
        "SPARK_HOME": "/path/to/spark",
        "HUDI_JARS": "/path/to/hudi/packaging/hudi-spark-bundle/target/*.jar"
      }
    }
  }
}
```

## Implementation Plan

### Phase 1: Read-only MCP Server (hudi-rs based)
- Install: `pip install hudi`
- Implement read-only tools using hudi-rs Python bindings
- No Spark dependency needed for basic table inspection
- Supports: table_info, schema, file listing, timeline reading

### Phase 2: Full MCP Server (PySpark based)
- Add PySpark for CALL procedure access
- Implement all diagnostic and action tools
- Add confirmation prompts for write operations

### Phase 3: OpenClaw Skills
- Build monitoring skill with scheduled checks
- Build incident response skill with diagnostic chains
- Build natural language query translator
- Connect to messaging channels (Slack, etc.) for alerts

### Phase 4: Autonomous Table Management
- Self-healing: auto-rollback stuck operations
- Auto-compaction: trigger when lag exceeds threshold
- Auto-clustering: trigger when small files accumulate
- Requires careful safeguards and dry-run modes

## Getting Started

To scaffold the MCP server:
1. Create `.claude/mcp/hudi-mcp-server/` directory
2. Implement `server.py` using the MCP Python SDK
3. Add to `.mcp.json` for Claude Code integration
4. For OpenClaw: publish as a ClawHub skill

Refer to the procedures in `hudi-spark-datasource/hudi-spark/src/main/scala/org/apache/spark/sql/hudi/command/procedures/` for the complete set of operations to expose.
