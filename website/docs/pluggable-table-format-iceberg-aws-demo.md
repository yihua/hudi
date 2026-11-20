---
title: "Pluggable Table Format in action on AWS"
keywords: [hudi, iceberg, pluggable table format, xtable, aws, emr, glue, athena, trino]
summary: "Write an Iceberg table with Hudi on Amazon EMR, commit it to the AWS Glue Data Catalog, and read it from Athena, Trino and Spark"
toc: true
last_modified_at: 2026-11-20T00:00:00-08:00
---

**Write once with Hudi. Read anywhere Iceberg is read.**

This demo writes a table with Hudi on Amazon EMR and stores it on Amazon S3. Through Hudi's pluggable table format,
every Hudi commit is also an Iceberg commit in the AWS Glue Data Catalog. Engines that read Iceberg tables from Glue
then read the same table: Amazon Athena, Trino and Spark.

You keep what Hudi does well on the write side: record-level upserts and deletes with Hudi's indexes, and table
services (clustering, compaction, cleaning, archival) that Hudi plans and runs. Readers get a standard Iceberg
table. There is one copy of the data, and nothing to migrate.

:::caution Preview
This page describes a preview that needs snapshot builds. For the concepts, the full list of settings and the
limitations, read the [quick start](pluggable-table-format-iceberg-quick-start.md) first.
:::

## What you will build

```text
  Amazon EMR (Spark 3.5)
  Hudi writer + XTable Iceberg plugin
     |                          |
     | data files and           | each Hudi commit is an Iceberg commit
     | Iceberg metadata         | (metadata_location updated)
     v                          v
  Amazon S3  <-------------  AWS Glue Data Catalog
  s3://<bucket>/<prefix>/      table <glue_db>.<table>, table_type=ICEBERG
     ^
     | read through Glue
     |
  Athena (format version 2) . Trino (format version 2) . Spark on EMR (2 and 3)
  Snowflake (Glue catalog integration)
```

The demo runs three tables:

| Table | Table type | Iceberg format version | Read by |
|:------|:-----------|:-----------------------|:--------|
| `trips_v3` | copy-on-write | 3 (the default) | Spark on EMR, Snowflake |
| `trips_v2` | copy-on-write | 2 | Spark on EMR, Athena, Trino |
| `trips_mor` | merge-on-read, deletion vectors | 3 | Spark on EMR, Snowflake |

All use Hudi table version 9.

## Prerequisites

- An EMR 7.x cluster with Spark 3.5. You do not need EMR's Iceberg or Hudi applications. If they are installed,
  keep their jars off the job's classpath.
- Java 17 on the cluster.
- An S3 location, `s3://<bucket>/<prefix>/`, for the tables.
- A Glue database, `<glue_db>`, in `<region>`.
- An EMR instance role that can read and write the S3 prefix, and read, create and update tables in the Glue
  database.
- The jar set from the quick start, plus `iceberg-aws-bundle-1.11.0.jar` (Apache Iceberg 1.11.0 release) for the
  Glue catalog:

| Jar |
|:----|
| `hudi-spark3.5-bundle_2.12-1.3.0-SNAPSHOT.jar` |
| `iceberg-spark-runtime-3.5_2.12-1.11.0.jar` |
| `iceberg-aws-bundle-1.11.0.jar` |
| `xtable-api-0.5.0-SNAPSHOT.jar` |
| `xtable-hudi-support-utils-0.5.0-SNAPSHOT.jar` |
| `xtable-core_2.12-0.5.0-SNAPSHOT.jar` |
| `xtable-iceberg-pluggable-tf_2.12-0.5.0-SNAPSHOT.jar` |

See [Build the jars](pluggable-table-format-iceberg-quick-start.md#build-the-jars). You can build
them on the EMR primary node: Hudi needs JDK 11 and XTable needs JDK 17. Put the jars in one folder on the primary
node, for example `/home/hadoop/ptf-jars`.

**Demo script:** TBD

## Step 1: Start Spark with the Glue catalog

Run on the EMR primary node. The `glue` catalog is how Spark reads the Iceberg table. Hudi writes through its own
catalog client, configured in the table settings in step 2.

```shell
JARS_DIR=/home/hadoop/ptf-jars
PTF_JARS=$JARS_DIR/hudi-spark3.5-bundle_2.12-1.3.0-SNAPSHOT.jar,\
$JARS_DIR/iceberg-spark-runtime-3.5_2.12-1.11.0.jar,\
$JARS_DIR/iceberg-aws-bundle-1.11.0.jar,\
$JARS_DIR/xtable-api-0.5.0-SNAPSHOT.jar,\
$JARS_DIR/xtable-hudi-support-utils-0.5.0-SNAPSHOT.jar,\
$JARS_DIR/xtable-core_2.12-0.5.0-SNAPSHOT.jar,\
$JARS_DIR/xtable-iceberg-pluggable-tf_2.12-0.5.0-SNAPSHOT.jar

pyspark --master yarn --deploy-mode client \
  --jars "$PTF_JARS" \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
  --conf spark.sql.catalog.glue.warehouse=s3://<bucket>/<prefix>/glue-warehouse \
  --conf spark.sql.catalog.glue.cache-enabled=false
```

- `--jars` puts Hudi and the XTable plugin in the same class loader. Do not use `extraClassPath` for them.
- `cache-enabled=false` keeps Spark from serving a stale table: Hudi commits through a separate catalog client.
- Use `s3://` paths everywhere. The file paths Iceberg records must match the paths Hudi lists.

Create the Glue database if it does not exist:

```python
spark.sql("CREATE NAMESPACE IF NOT EXISTS glue.<glue_db>")
```

## Step 2: Write with Hudi

These are the write options the demo uses for `trips_v2`. For `trips_v3`, change the table name, the base path and
the format version.

```python
table = "trips_v2"                                   # also the Glue table name
base_path = "s3://<bucket>/<prefix>/tables/trips_v2"

options = {
    "hoodie.table.name": table,
    "hoodie.datasource.write.table.name": table,
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.recordkey.field": "key",
    "hoodie.datasource.write.partitionpath.field": "region",
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.SimpleKeyGenerator",
    "hoodie.write.table.version": "9",
    "hoodie.record.merge.mode": "COMMIT_TIME_ORDERING",
    # Iceberg table format
    "hoodie.table.format": "ICEBERG",
    "hoodie.table.format.iceberg.format-version": "2",
    # commit each Iceberg snapshot through the Glue Data Catalog
    "hoodie.table.format.iceberg.catalog.name": "glue",
    "hoodie.table.format.iceberg.catalog.impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    "hoodie.table.format.iceberg.catalog.option.warehouse": "s3://<bucket>/<prefix>/glue-warehouse",
    "hoodie.table.format.iceberg.namespace": "<glue_db>",
}
```

The `hoodie.table.format*` settings are stored in `hoodie.properties` when the first write creates the table.
Every later writer and table service then commits to the same Glue table.

The Glue table gets the Hudi table name, in the namespace you set. Its location is the Hudi base path. The
`warehouse` option is required by `GlueCatalog`, but this table does not live under it.

Write as you would to any Hudi table. `df` is a DataFrame with the columns `key` and `region` and your data columns:

```python
def hudi_write(df, operation):
    (df.write.format("hudi").options(**options)
       .option("hoodie.datasource.write.operation", operation)
       .mode("append").save(base_path))

hudi_write(inserts_df, "insert")
hudi_write(changes_df, "upsert")
hudi_write(deletes_df, "delete")
```

After the first write, Glue has a table `<glue_db>.trips_v2` with `table_type` set to `ICEBERG`. Its
`metadata_location` moves to a new metadata file on every commit.

The Glue table at the end of our run (`aws glue get-table`; `trips_v2` was named `ptf_cow_v2_glue_r2` there):

```json
{
    "Type": "EXTERNAL_TABLE",
    "Params": {
        "metadata_location": "s3://<bucket>/<prefix>/ptf_cow_v2_glue_r2/metadata/00023-a66c1e10-b546-4dc0-9c97-30f09ecaf521.metadata.json",
        "previous_metadata_location": "s3://<bucket>/<prefix>/ptf_cow_v2_glue_r2/metadata/00022-3cde44e9-986b-4b07-b299-26bfc1d4e18a.metadata.json",
        "table_type": "ICEBERG"
    },
    "Location": "s3://<bucket>/<prefix>/ptf_cow_v2_glue_r2"
}
```

Each Hudi commit moves `metadata_location` to a new Iceberg metadata file through Glue's compare-and-swap update, and
`previous_metadata_location` keeps the one before.

## Step 3: Run table services from Hudi

Table services run from Hudi, in the writing job. Each one that changes files commits an Iceberg snapshot to Glue,
like any write. The demo drives them through Hudi's write client, which works on a table that is only registered in
Glue as Iceberg:

```python
jvm = spark._jvm
empty = jvm.org.apache.hudi.common.util.Option.empty()

def write_client(extra=None):
    storage_conf = jvm.org.apache.hudi.hadoop.fs.HadoopFSUtils.getStorageConf(spark._jsc.hadoopConfiguration())
    meta_client = (jvm.org.apache.hudi.common.table.HoodieTableMetaClient.builder()
                   .setConf(storage_conf).setBasePath(base_path).build())
    schema = jvm.org.apache.hudi.common.table.TableSchemaResolver(meta_client).getTableSchema(False).toString()
    params = jvm.java.util.HashMap()
    for k, v in dict(options, **(extra or {})).items():
        params.put(k, v)
    return jvm.org.apache.hudi.DataSourceUtils.createHoodieClient(spark._jsc, schema, base_path, table, params)

client = write_client({"hoodie.clustering.plan.strategy.sort.columns": "key"})
try:
    # clustering: rewrite the files of each partition, sorted by key
    instant = client.scheduleClustering(empty)
    if instant.isPresent():
        client.cluster(instant.get(), True)
    # clean: delete file versions no reader of the retained commits needs
    client.clean()
finally:
    client.close()
```

For a merge-on-read table, compaction works the same way: `client.scheduleCompaction(empty)`, then
`client.compact(instant.get(), True)`.

**Archival.** The demo keeps a short history so that archival triggers within a few commits:

```python
retention = {
    "hoodie.clean.automatic": "true",
    "hoodie.archive.automatic": "true",
    "hoodie.cleaner.commits.retained": "1",
    "hoodie.keep.min.commits": "2",
    "hoodie.keep.max.commits": "3",
    "hoodie.metadata.compact.max.delta.commits": "3",
}
```

Hudi then cleans and archives after each commit. When it archives instants, the plugin expires their Iceberg
snapshots, so Glue readers can time travel only within Hudi's active timeline. Hudi does not archive past the latest
compaction of its metadata table, so the demo also lowers `hoodie.metadata.compact.max.delta.commits`.

**Rollback.** Roll back the latest commit by its instant time:

```python
no_mdt_compaction = {"hoodie.clean.automatic": "false", "hoodie.metadata.compact.max.delta.commits": "50"}
client = write_client(no_mdt_compaction)
try:
    client.rollback("<instant_time>")
finally:
    client.close()
```

Hudi refuses to roll back a commit that comes before the latest compaction of its metadata table. The demo writes
the commit it rolls back, and runs the rollback, with the settings above so that no such compaction happens in
between.

The rollback commits a new Iceberg snapshot that restores the earlier files. The rolled-back snapshot stays in the
Iceberg history, but its files are deleted, so do not time travel to it.

The SQL procedures in the quick start run the same actions. `run_clustering`, `run_compaction` and
`archive_commits` also accept a `path` instead of a table name, which suits a table that Spark's session catalog
does not know as a Hudi table.

## Step 4: Read with Spark on EMR

Read the Glue table through the `glue` catalog. This works for both format versions.

```python
spark.table("glue.<glue_db>.trips_v3").show()

spark.sql("SELECT snapshot_id, operation, committed_at FROM glue.<glue_db>.trips_v3.snapshots").show(truncate=False)

# time travel to an earlier snapshot
spark.read.option("snapshot-id", "<snapshot_id>").table("glue.<glue_db>.trips_v3").show()
```

## Step 5: Read with Amazon Athena

Athena reads Iceberg format version 2 tables from Glue. Query `trips_v2` with Athena engine version 3:

```sql
SELECT * FROM "<glue_db>"."trips_v2" LIMIT 10;

-- time travel to the snapshot of an earlier step
SELECT * FROM "<glue_db>"."trips_v2" FOR VERSION AS OF <snapshot_id>;
```

In our run, Athena's rows matched the expected rows exactly, row by row, for every snapshot still in the table's
history: the last two upsert rounds and the rollback (950 rows each). Earlier snapshots had already been expired by
archival, so time travel to them is not possible, which is expected. The snapshot of the rolled-back commit stays in
the Iceberg history, but its data files are gone, so do not time travel to it.

The current-table query on `trips_v2` returned the expected 950 rows.

At the time of writing, Athena does not read Iceberg format version 3. That is why the demo has a `trips_v2` table.

In our run (Athena engine version 3), a plain `SELECT` from `trips_v3` fails with:

```
GENERIC_INTERNAL_ERROR: Iceberg format version 3 is not supported
```

## Step 6: Read with Trino

Trino reads the same Glue table with its Iceberg connector. On EMR, add a catalog file such as
`/etc/trino/conf/catalog/iceberg.properties`:

```properties
connector.name=iceberg
iceberg.catalog.type=glue
```

Restart Trino, then query:

```sql
SELECT * FROM iceberg."<glue_db>"."trips_v2" LIMIT 10;

SELECT * FROM iceberg."<glue_db>"."trips_v2" FOR VERSION AS OF <snapshot_id>;
```

The Trino release on EMR 7.8.0 reads Iceberg format versions 1 and 2, so this demo reads `trips_v2` from Trino.

| | Result |
|:--|:--|
| Trino version | TBD |
| Current table, `trips_v2` | TBD |
| Time travel to each step's snapshot, `trips_v2` | TBD |

## Step 7: Read with Snowflake

Snowflake reads Iceberg tables registered in the AWS Glue Data Catalog through a catalog integration for Glue.

**Setup:** TBD

| | Result |
|:--|:--|
| Current table, `trips_v3` | TBD |
| Current table, `trips_mor` | TBD |

## The Iceberg route and Hudi's Glue sync

Hudi already has a way to put a table in Glue: its [Glue catalog sync](syncing_aws_glue_data_catalog.md). That sync
registers a Hudi table. Readers then need Hudi support, at a Hudi table version they understand.

The demo registers the same base path both ways and queries both from Athena:

| | Iceberg route (this demo) | Hudi Glue sync |
|:--|:--|:--|
| Glue table | `<glue_db>.trips_v2`, `table_type=ICEBERG` | `<glue_db>.trips_v2_hudi`, a Hudi table with Hudi input formats |
| Kept current by | the writer, on every commit | a sync run after commits |
| Athena, Hudi table version 9 | reads it (Iceberg format version 2) | cannot read it |

In our run the sync succeeded, and `hoodie.properties` recorded Hudi table version 9. A plain `SELECT` from the
synced Hudi table in Athena (engine version 3) then failed with `HIVE_UNKNOWN_ERROR`. The error does not name the
cause. The synced copies of both demo tables (`trips_v2_hudi` and `trips_v3_hudi`) fail the same way.

The sync took about 6 seconds and left the Iceberg Glue table's `metadata_location` unchanged.

The demo runs the sync once after the writes, with Hudi's `AwsGlueCatalogSyncTool`. It comes in
`hudi-aws-bundle-1.3.0-SNAPSHOT.jar` (build it with `-pl packaging/hudi-aws-bundle`). Add that jar to the end of
`--jars`, after the Hudi Spark bundle.

```python
props = jvm.java.util.Properties()
for k, v in {
    "hoodie.datasource.meta.sync.base.path": base_path,
    "hoodie.datasource.hive_sync.database": "<glue_db>",
    "hoodie.datasource.hive_sync.table": "trips_v2_hudi",
    "hoodie.datasource.hive_sync.partition_fields": "region",
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    "hoodie.datasource.hive_sync.base_file_format": "PARQUET",
}.items():
    props.setProperty(k, v)

jvm.org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool(
    props, spark._jsc.hadoopConfiguration(), empty).syncHoodieTable()
```

:::caution
Hudi's catalog sync does not know about the table format. If you turn it on for a table with
`hoodie.table.format=ICEBERG`, give the synced table a different name from the Iceberg table, as above. You do not
need the Hudi sync to read the table as Iceberg.
:::

## Limitations

The [quick start limitations](pluggable-table-format-iceberg-quick-start.md#limitations) apply. On AWS in particular:

- **Hudi is the only writer.** Do not write to the Glue table from Athena, Trino or Spark's Iceberg writer, and do
  not run Iceberg maintenance on it, such as Athena `OPTIMIZE` or `VACUUM`. Hudi runs the table services.
- **Athena and the Trino release we used read format version 2 only.** Use format version 2 for tables they must
  read. Format version 2 works for copy-on-write tables only.
- **One writer at a time.** Table services run inside the writing job.

## Clean up

Drop the Glue tables `trips_v3`, `trips_v2`, `trips_mor` and `trips_v2_hudi` from `<glue_db>`, and delete
`s3://<bucket>/<prefix>/`.
