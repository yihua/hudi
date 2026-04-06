<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# RFC-98: Spark Datasource V2 Read

## Proposers

- @geserdugarov

## Approvers

- @yihua
- @vinothchandar
- @danny0405

## Status

GH Discussion: https://github.com/apache/hudi/discussions/13955

## Abstract

Data source is one of the foundational APIs in Spark, with two major versions known as "V1" and "V2".
Moving Hudi reads to the V2 API unlocks Spark-native pushdown interfaces that the V1 scan path cannot support:

- Aggregate pushdown (`SupportsPushDownAggregates`): queries like `SELECT COUNT(*)` or `MIN/MAX(col)` can be resolved from column statistics without scanning data files, dramatically reducing query time.
- Column pruning at scan level (`SupportsPushDownRequiredColumns`): the V2 scan prunes unneeded columns before data reaches Spark operators, reducing I/O for projection queries.
- Filter pushdown (`SupportsPushDownFilters`): predicate evaluation is pushed into the scan, enabling more efficient data skipping and partition pruning.
- Limit and TopN pushdown (`SupportsPushDownLimit`, `SupportsPushDownTopN`): Spark pushes row limits into the scan, avoiding full-table reads for `LIMIT` queries.

## Background

The current implementation of Spark Datasource V2 integration is presented in the schema below:

![Current integration with Spark](initial_integration_with_Spark.jpg)

## Implementation

Hudi's write path is mature, and involves indexing, precombining, upsert/insert routing, file sizing, and table services (compaction/clustering/cleaning). 
Also `HoodieSparkSqlWriter::write` handles schema evolution, partition encoding, metadata updates, and multi-writer concurrency.
DSv2's `WriteBuilder` >> `BatchWrite` >> DataWriter API is too simplistic for this, and moving to this entirely would be a non-starter. Also, due to the flexibility of the V1 API in terms of allowing the writes to shuffle data after the `df.write.format....save` is invoked, Hudi supports a streaming DF write for its upsert operation. A good majority of Hudi jobs work this way today, and we cannot break all of these at once

The proposed approach is hybrid: DSv2 for reads, with a DSv1 fallback for writes (`V2TableWithV1Fallback`) in the current state.
Later, if a DSv2 write path can be implemented without loss of performance or functionality, it may become possible to move to full DSv2 support.
However, this migration should still be incremental, please check the "Future Work" chapter for details.

Overall proposed architecture for the hybrid approach is shown in the following schema:

![Proposed approach with hybrid V1 write and V2 read](integration_with_DSv2_read.jpg)

### DataFrame API

A new SPI short name, `"hudi_v2"`, activates the DSv2 read path when using the Spark DataFrame API.
The existing `"hudi"` path remains unchanged.
This is done to unblock incremental development of the DSv2 path and will be removed in the long term, please check the "Future Work" chapter for details.
It also allows switching later from the current DSv1 fallback to a DSv2 write path, if an implementation without performance degradation is found.
The DSv2 write path is currently under research.

<table>
<tr>
<th>Operation</th>
<th>Current implementation</th>
<th>Additional functionality proposed in this RFC</th>
</tr>
<tr>
<td>Write</td>
<td>
<pre>
df.write.format("hudi").mode(...).save(path)
        v
BaseDefaultSource (V1) -> DefaultSource
        v
CreatableRelationProvider.createRelation(...)
        v
HoodieSparkSqlWriter.write(...)
        v
SparkRDDWriteClient -> upsert/insert/bulk_insert
</pre>
</td>
<td>
<pre>
df.write.format("hudi_v2").mode(...).save(path)
        v
HoodieDataSourceV2 (TableProvider + DataSourceRegister + CreatableRelationProvider)
        v
Spark treats as V1 source for writes
        v
CreatableRelationProvider.createRelation(...)
        v
HoodieSparkSqlWriter.write(...)
        v
SparkRDDWriteClient -> upsert/insert/bulk_insert
</pre>
</td>
</tr>
<tr>
<td>Read</td>
<td>
<pre>
spark.read.format("hudi").load(path)
        v
V1 DataSource resolution (via ServiceLoader + DataSourceRegister)
        v
BaseDefaultSource found
(extends DefaultSource with DataSourceRegister)
(not a TableProvider)
        v
Spark treats as V1 DataSource
        v
DefaultSource.createRelation(...)
        v
MergeOnReadSnapshotRelation / BaseRelation
        v
LogicalRelation -> FileScan -> ...
</pre>
</td>
<td>
<pre>
spark.read.format("hudi_v2").load(path)
        v
DataSourceV2Utils.lookupProvider("hudi_v2")
        v
HoodieDataSourceV2 found
(extends TableProvider with DataSourceRegister)
(does not extend SupportsCatalogOptions)
        v
Spark uses TableProvider.getTable() directly
(no catalog routing since no SupportsCatalogOptions)
        v
HoodieDataSourceV2.getTable(...)
        v
HoodieSparkV2Table(...)
(no catalogTable, no tableIdentifier)
        v
HoodieScanBuilder -> HoodieBatchScan -> ...
</pre>
</td>
</tr>
</table>

### SQL Queries

Spark SQL API is managed by new configuration parameter `hoodie.datasource.read.use.v2`, which controls the returned table type.

<table>
<tr>
<th>Operation</th>
<th>Current implementation</th>
<th>Additional functionality proposed in this RFC</th>
</tr>
<tr>
<td>Write</td>
<td>
<pre>
INSERT INTO hudi_table VALUES (...);   -- table created with USING hudi
        v
Spark Analyzer resolves table via catalog
        v
HoodieCatalog.loadTable(Identifier("hudi_table"))
        v
isHoodieTable => true, v2ReadEnabled = false, schemaEvol = false
        v
RETURNS: V1Table(catalogTable) via v1TableWrapper
        v
Spark V1 write path -> InsertIntoHoodieTableCommand (analysis rule)
        v
HoodieSparkSqlWriter.write(...)
</pre>
</td>
<td>
<pre>
INSERT INTO hudi_table VALUES (...);   -- table created with USING hudi
        v
Spark Analyzer resolves table via catalog
        v
HoodieCatalog.loadTable(Identifier("hudi_table"))
        v
isHoodieTable => true, v2ReadEnabled = true
        v
RETURNS: HoodieSparkV2Table(...)
        v
SupportsWrite.newWriteBuilder() -> HoodieV1WriteBuilder
        v
V1Write -> InsertableRelation.insert(data, overwrite)
        v
Align columns (rename + cast to table's user schema)
        v
HoodieSparkSqlWriter.write(...)
</pre>
</td>
</tr>
<tr>
<td>Read</td>
<td>
<pre>
SELECT * FROM hudi_table;   -- table created with USING hudi
        v
Spark Analyzer resolves table name via catalog
        v
HoodieCatalog.loadTable(Identifier("hudi_table"))
        v
super.loadTable(ident)
        v
V1Table(catalogTable) where catalogTable.provider = "hudi"
        v
isHoodieTable(catalogTable) => true
        v
v2ReadEnabled = false, schemaEvolutionEnabled = false (defaults)
        v
RETURNS: HoodieInternalV2Table(...).v1TableWrapper = V1Table(catalogTable)
        v
Spark uses V1 fallback -> DefaultSource.createRelation()
        v
HoodieFileIndex -> FileScan -> ...
</pre>
</td>
<td>
<pre>
SELECT * FROM hudi_table;   -- table created with USING hudi
        v
Spark Analyzer resolves table name via catalog
        v
HoodieCatalog.loadTable(Identifier("hudi_table"))
        v
super.loadTable(ident)
        v
V1Table(catalogTable) where catalogTable.provider = "hudi"
        v
isHoodieTable(catalogTable) => true
        v
v2ReadEnabled = conf("hoodie.datasource.read.use.v2") = true
        v
RETURNS: HoodieSparkV2Table(...)
        v
SupportsRead.newScanBuilder() -> HoodieScanBuilder
        v
HoodieBatchScan -> ...
</pre>
</td>
</tr>
</table>

### Read

All new classes go into package `org.apache.spark.sql.hudi.v2` inside `hudi-spark-common`.

| Class                           | Spark Interface                                                                                                                   | Responsibility                                                                                                                                                                                                                                                                     |
|---------------------------------|-----------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `HoodieDataSourceV2`            | `TableProvider`, `DataSourceRegister`, `CreatableRelationProvider`                                                                | SPI entry point for `format("hudi_v2")`. `CreatableRelationProvider` enables DataFrame API writes via `df.write.format("hudi_v2")`.                                                                                                                                                |
| `HoodieSparkV2Table`            | `Table`, `SupportsRead`, `SupportsWrite`, `V2TableWithV1Fallback`                                                                 | Routes reads to DSv2, writes to DSv1 fallback via `HoodieV1WriteBuilder`.                                                                                                                                                                                                          |
| `HoodieScanBuilder`             | `ScanBuilder`, `SupportsPushDownFilters`, `SupportsPushDownRequiredColumns`, `PartialLimitPushDown`, `SupportsPushDownAggregates` | Collects filter, column pruning, limit, and aggregate pushdowns.                                                                                                                                                                                                                   |
| `HoodieBatchScan`               | `Scan`, `Batch`                                                                                                                   | Plans input partitions using existing `HoodieFileIndex`.                                                                                                                                                                                                                           |
| `HoodieInputPartition`          | `InputPartition`                                                                                                                  | Serializable descriptor for file slices.                                                                                                                                                                                                                                           |
| `HoodiePartitionReaderFactory`  | `PartitionReaderFactory`                                                                                                          | Creates readers on executors. Overrides `supportColumnarReads()` and `createColumnarReader()` for COW vectorized reads.                                                                                                                                                            |
| `HoodiePartitionReader`         | `PartitionReader[InternalRow]`                                                                                                    | Row-based reader for MOR, incremental, CDC, and COW fallback (unsupported schema).                                                                                                                                                                                                 |
| `HoodieColumnarPartitionReader` | `PartitionReader[ColumnarBatch]`                                                                                                  | Columnar reader for COW base files. Returns vectorized Parquet batches directly to Spark.                                                                                                                                                                                          |
| `HoodieV1WriteBuilder` (reused) | `SupportsTruncate`, `SupportsOverwrite`, `ProvidesHoodieConfig`                                                                   | Existing V1 write fallback builder, defined as `private[hudi]` in `HoodieInternalV2Table.scala`. `HoodieSparkV2Table` directly instantiates it (sibling class, not a subclass of `HoodieInternalV2Table`). `HoodieInternalV2Table` is retained for the schema-evolution code path. |

### Table services

Table services (compaction, clustering, cleaning) are not affected by this change.
They operate via the write client and are triggered independently of the read path.

### Implementation phases

The phases below describe the logical design ordering. 
In practice, `HoodieScanBuilder` declares all pushdown interfaces from the outset with working implementations, and the PRs may ship multiple phases together.

1. **Coexistence POC.** All new classes return empty read results, SPI registration, reuse of `HoodieV1WriteBuilder` for V1 write fallback, `hoodie.datasource.read.use.v2` config, 
`HoodieV1OrV2Table` extractor update in `HoodieSparkBaseAnalysis` to recognize `HoodieSparkV2Table` for DDL operations.
2. **COW snapshot read.** Wire `HoodieBatchScan.planInputPartitions()` to `HoodieFileIndex`, implement base file reading in `HoodiePartitionReader`. Column pruning support.
3. **Filter pushdown.** Implement `HoodieScanBuilder.pushFilters()` for partition pruning and data skipping via `HoodieFileIndex`.
4. **Vectorized COW reads.** Enable columnar batch output for COW snapshot reads to match V1 performance.
5. **MOR snapshot read.** Extend `HoodiePartitionReader` with base + log merge logic, reusing `HoodieFileGroupReader`.
6. **Incremental and CDC queries.** Route based on query type option in `HoodieScanBuilder`.
7. **Advanced pushdowns.** `SupportsPushDownAggregates`, `SupportsPushDownLimit`, `SupportsPushDownTopN`.

## Rollout/Adoption Plan

- The existing `format("hudi")` path is completely untouched, so there is no regression risk.
- For DataFrame API, users opt in by using `format("hudi_v2")`. No config needed.
- For SQL queries, users set `hoodie.datasource.read.use.v2=true` to route reads through DSv2.
- Rollback: switch back to `format("hudi")` or set the config to `false`.

### Config interaction: `hoodie.datasource.read.use.v2` vs `hoodie.schema.on.read.enable`

In `HoodieCatalog.loadTable()`, `v2ReadEnabled` is evaluated first and takes strict precedence:

| `hoodie.datasource.read.use.v2` | `hoodie.schema.on.read.enable` | Table returned                                           |
|---------------------------------|--------------------------------|----------------------------------------------------------|
| `true` (Spark ≥ 3.5)            | any                            | `HoodieSparkV2Table` (DSv2 read)                         |
| `false`                         | `true`                         | `HoodieInternalV2Table` (existing schema-evolution path) |
| `false`                         | `false`                        | `V1Table` wrapper (existing default)                     |

The two configs are independent. When both are `true`, `v2ReadEnabled` wins.

## Test Plan

- Verify that `EXPLAIN` plans show `BatchScanExec` (DSv2) instead of `FileSourceScanExec` (DSv1) when DSv2 is enabled.
- Existing unit and functional tests must pass unchanged (no regressions in DSv1 path).
- New tests for DSv2 read path: COW snapshot, MOR snapshot, filter pushdown, column pruning.
- TPC-H benchmark to compare DSv1 vs DSv2 read performance at each implementation phase.
  Success criteria:
    - DSv2 COW snapshot full data read should show no regression versus DSv1.
    - DSv2 COW snapshot read with projections and filter pushdowns should show 10% faster wall-clock time.
    - DSv2 COW snapshot read with limit and aggregate pushdowns should show 20% faster wall-clock time.
    - MOR benchmarks should show no regression versus DSv1's row-based MOR path.

## Future Work

1. DSv2 read support using `hudi_v2` for the DataFrame API, and `hoodie.datasource.read.use.v2` for the SQL API (`false` by default).
   These means that all stages from "Implementation phases" chapter are completed.
2. (Optional) DSv2 write support using `hudi_v2` for the DataFrame API, and `hoodie.datasource.write.use.v2` for the SQL API (`false` by default).
3. `hoodie.datasource.read/write.use.v2` is `true` by default.
4. Switch format short names: `hudi_v2` -> `hudi`, `hudi` -> `hudi_v1`.
5. Deprecate use of `hudi_v1`, `hoodie.datasource.read/write.use.v2`.
6. Remove `hudi_v1`, `hoodie.datasource.read/write.use.v2` from the codebase.

### SPI short name mapping at each step

| Step from Future Work | `BaseDefaultSource` | `HoodieDataSourceV2` | `DefaultSource`        |
|-----------------------|---------------------|----------------------|------------------------|
| 1 (this RFC)          | `"hudi"`            | `"hudi_v2"`          | `"hudi_v1"` (internal) |
| 4 (name swap)         | `"hudi_v1"`         | `"hudi"`             | removed or aligned     |
| 6 (removal)           | removed             | `"hudi"`             | removed                |

Note: `DefaultSource.shortName() = "hudi_v1"` is an internal SPI name that is never user-facing because `BaseDefaultSource` overrides it to `"hudi"`. 
This existing naming aligns naturally with the planned swap at step 4.
