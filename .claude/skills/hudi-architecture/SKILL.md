---
name: hudi-architecture
description: Explain Hudi's internal architecture, module structure, and design patterns. Use when someone asks about how Hudi is organized, module dependencies, layering, or wants a deep dive into a subsystem's architecture.
user-invocable: true
allowed-tools: Read, Grep, Glob, Agent
argument-hint: [subsystem e.g. "write path", "read path", "metadata table", "timeline", "index", "table services"]
---

# Hudi Architecture Deep Dive

The user wants to understand: **$ARGUMENTS**

## Instructions

Explore the actual codebase to explain the architecture. Do NOT rely on memory - read the actual code.

### Key module layout:
- `hudi-io/` - Low-level IO abstractions, base exception hierarchy
- `hudi-common/` - Engine-agnostic core: timeline, table metadata, configs, file formats, schemas
- `hudi-client/hudi-client-common/` - Engine-agnostic write client: table services, action executors, transaction management
- `hudi-client/hudi-spark-client/` - Spark-specific write client implementation
- `hudi-client/hudi-flink-client/` - Flink-specific write client implementation
- `hudi-spark-datasource/` - Spark DataSource V1/V2 integration
- `hudi-flink-datasource/` - Flink table/datastream integration
- `hudi-utilities/` - HoodieStreamer, repair tools, validators
- `hudi-cli/` - Interactive CLI tool
- `hudi-sync/` - Metastore sync (Hive, BigQuery, etc.)
- `hudi-timeline-service/` - Timeline server for marker and view management
- `hudi-hadoop-common/` - Hadoop FileSystem bindings
- `hudi-aws/`, `hudi-gcp/` - Cloud-specific implementations
- `packaging/` - Bundle assembly for different Spark/Flink versions

### For each subsystem, explain:
1. **Entry points** - The main classes a user/developer first encounters
2. **Key abstractions** - Interfaces that define the contract
3. **Implementation flow** - How data/control flows through the classes (with file:line refs)
4. **Extension points** - Where users/developers can plug in custom behavior
5. **Design patterns** - Builder, Strategy, Template Method, etc. used and why
6. **Layering rules** - What can depend on what (e.g., hudi-common CANNOT import Spark classes)

### Visual aid
When helpful, describe the flow as a numbered sequence of steps showing class interactions.
