---
title: "Interoperability with the Pluggable Table Format"
keywords: [hudi, iceberg, pluggable table format, xtable, deletion vectors, row lineage, quick start]
summary: "Write an Apache Iceberg table with Hudi's pluggable table format, run Hudi table services on it, and read it with any Iceberg engine"
toc: true
last_modified_at: 2026-11-20T00:00:00-08:00
---

Hudi can write a table that is also an Apache Iceberg table. You write with Hudi and keep what Hudi gives a
writer: record-level indexes, upserts and deletes by key, and table services such as compaction, clustering and
cleaning. Any engine that reads Iceberg reads the same table. There is one copy of the data, and no sync job.

This works through Hudi's pluggable table format. A table format plugin from
[Apache XTable (incubating)](https://xtable.apache.org/) publishes every Hudi commit as an Iceberg snapshot, in
Iceberg format version 3 by default.

:::caution Preview
This feature is a preview. It needs snapshot builds of Hudi (`1.3.0-SNAPSHOT`) and XTable (`0.5.0-SNAPSHOT`)
until the next releases. Spark 3.5 is the only supported engine for now.
Read the [limitations](#limitations) before you use it with data you care about.
:::

:::tip Hand this guide to your AI agent
The steps in [Run it](#run-it) work for AI coding agents as well as people: pinned versions, fixed paths, and a
`CHECK ... PASS` line after every step. To jumpstart writing Iceberg tables with Hudi, point your agent at this
page and ask it to run the steps and report the `CHECK` lines.
:::

## What this guide shows

In one local Spark session you create a Hudi table with `hoodie.table.format=ICEBERG`, change it with Spark SQL
and Hudi procedures, and after every step read it both as Iceberg and as Hudi. The two readers always return the
same rows.

| Step | What Hudi does | What Iceberg readers see |
|:-----|:---------------|:-------------------------|
| Insert 8 rows | Commit | Snapshot `append`, 8 rows |
| Update, delete, merge | Upserts by key through Hudi's index | One `overwrite` snapshot each |
| Time travel | | The first snapshot still holds the original 8 rows |
| Roll back the merge | Rollback | A new `overwrite` snapshot with the rows as before the merge |
| Clustering, clean | Table services | Files rewritten, same rows, same row ids |
| Merge-on-read writes | Updates and deletes as positional deletes | Deletion vectors in Puffin files, one per data file |
| Compaction | Rewrites files that have deletes | The deletion vectors go away |
| Commit through a catalog | Commits to an Iceberg catalog | The table by name, for example `ice.db.trips_cat` |

With format version 3, rows also keep their Iceberg row lineage, as in Iceberg: a row keeps its `_row_id` through
updates, merges and clustering, and an updated row takes the sequence number of the commit that changed it.

## For Iceberg users

| Topic | What to expect |
|:------|:---------------|
| Spec | A standard Iceberg table: metadata files, manifests and snapshots are written with the Iceberg library. The Iceberg schema holds only your table's columns. Readers need no Hudi jars. |
| Format version | 3 by default, with deletion vectors (Puffin) for merge-on-read and row lineage (`_row_id`, `_last_updated_sequence_number`). Version 2 for readers that need it. |
| Catalogs | Path-based, or any Iceberg catalog named by class: REST (`org.apache.iceberg.rest.RESTCatalog`), AWS Glue, JDBC, Hive. This guide uses JDBC; the [AWS page](pluggable-table-format-iceberg-aws-demo.md) uses Glue. |
| Query engines | Any engine that reads Iceberg from the catalog or the path, for example Spark, Trino, Amazon Athena (format version 2) and Snowflake. See the [AWS page](pluggable-table-format-iceberg-aws-demo.md). |
| Snapshots and time travel | One snapshot per Hudi commit, with the usual operations (`append`, `overwrite`, `delete`). Time travel by snapshot id. Metadata tables such as `snapshots`, `files` and `delete_files` work. |
| Writes | Upserts and deletes by key through Hudi's indexes, from Spark SQL `INSERT`, `UPDATE`, `DELETE` and `MERGE INTO`, or the DataFrame writer. |
| Commits | Each Hudi commit is one atomic Iceberg commit through the catalog, for example a compare-and-swap of the metadata location in Glue. |
| Table maintenance | Hudi runs it: compaction, clustering, cleaning, and snapshot expiry through archival. No separate Iceberg maintenance jobs. |

## How it works

- **One copy of the data.** Hudi writes Parquet base files. The Iceberg metadata points at the same files.
- **One Hudi commit, one Iceberg snapshot.** The plugin commits an Iceberg snapshot for every completed Hudi
  instant, including table services. Each snapshot names its Hudi instant in its summary, under `XTABLE_METADATA`.
- **Iceberg holds the commit record.** Hudi rebuilds its timeline of completed instants from the current Iceberg
  snapshot and its ancestors. A Hudi instant counts as completed only when its snapshot is in that history.
- **Hudi is the writer.** Hudi's indexes, merge logic and table services decide what the table holds. Iceberg
  engines read it.

Hudi finds the plugin through Java's `ServiceLoader`: the plugin jar registers `org.apache.xtable.IcebergTableFormat`
as the `ICEBERG` implementation of `org.apache.hudi.common.HoodieTableFormat`.

| Hudi action | What the Iceberg snapshot does |
|:------------|:-------------------------------|
| Insert, upsert, delete, insert overwrite, delete partition (copy-on-write) | Adds the new data files and removes the replaced ones |
| Delta commit (merge-on-read) | Adds the new data files and one deletion vector per touched data file |
| Compaction | Replaces data files with compacted ones; their deletion vectors go away |
| Clustering | Replaces data files with clustered ones |
| Clean, savepoint | Commits a snapshot with no file changes |
| Rollback | Commits a new snapshot that restores the files from before the rolled-back commit |
| Archival | Expires the Iceberg snapshots of the archived instants |
| Restore | Not supported |

## Run it

Every step below runs in one `spark-shell` session, in order. Each step ends with a line such as
`CHECK insert PASS`. If every line says `PASS`, your run matches this guide. Snapshot ids, commit times and file
names differ on every run; the checks do not depend on them.

The SQL inside `spark.sql(...)` also runs as is in `spark-sql`.

### 1. Versions

| Component | Version |
|:----------|:--------|
| Java | 17 |
| Spark | 3.5.5, Scala 2.12 (`spark-3.5.5-bin-hadoop3`) |
| Iceberg Spark runtime | `iceberg-spark-runtime-3.5_2.12-1.11.0.jar`, from Maven Central |
| Hudi | `1.3.0-SNAPSHOT`, built from source (step 2) |
| XTable | `0.5.0-SNAPSHOT`, built from source (step 2) |
| SQLite JDBC driver, for the catalog step | `org.xerial:sqlite-jdbc:3.43.0.0`, fetched by `--packages` |

### 2. Build the jars {#build-the-jars}

**Source:** Hudi TBD, XTable TBD

Build Hudi first: XTable compiles against the Hudi snapshot in your local Maven repository.

```shell
export JARS_DIR=$HOME/hudi-iceberg-jars
mkdir -p $JARS_DIR

# Hudi, with JDK 11
git clone https://github.com/apache/hudi.git && cd hudi
mvn clean install -DskipTests -Dscala-2.12 -Dspark3.5 -pl packaging/hudi-spark-bundle -am
cp packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-1.3.0-SNAPSHOT.jar $JARS_DIR/
cd ..

# XTable, with JDK 17
git clone https://github.com/apache/incubator-xtable.git && cd incubator-xtable
./mvnw clean package -DskipTests -Dspark.version.prefix=3.5 -pl xtable-hudi-support/xtable-iceberg-pluggable-tf -am
cp xtable-api/target/xtable-api-0.5.0-SNAPSHOT.jar \
   xtable-hudi-support/xtable-hudi-support-utils/target/xtable-hudi-support-utils-0.5.0-SNAPSHOT.jar \
   xtable-core/target/xtable-core_2.12-0.5.0-SNAPSHOT.jar \
   xtable-hudi-support/xtable-iceberg-pluggable-tf/target/xtable-iceberg-pluggable-tf_2.12-0.5.0-SNAPSHOT.jar \
   $JARS_DIR/
cd ..

# Iceberg runtime
curl -fsSL -o $JARS_DIR/iceberg-spark-runtime-3.5_2.12-1.11.0.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.11.0/iceberg-spark-runtime-3.5_2.12-1.11.0.jar

ls $JARS_DIR
```

Expect six jars in `$JARS_DIR`.

### 3. Start Spark

Start from an empty folder. Spark keeps its session catalog (`metastore_db/`) in the working directory, and the
table names below must be new.

```shell
rm -rf /tmp/hudi_iceberg && mkdir -p /tmp/hudi_iceberg/work && cd /tmp/hudi_iceberg/work

export PTF_JARS=$(ls $JARS_DIR/*.jar | paste -sd, -)

spark-shell --master "local[2]" \
  --jars $PTF_JARS \
  --packages org.xerial:sqlite-jdbc:3.43.0.0 \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions' \
  --conf 'spark.sql.catalog.ice=org.apache.iceberg.spark.SparkCatalog' \
  --conf 'spark.sql.catalog.ice.catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog' \
  --conf 'spark.sql.catalog.ice.uri=jdbc:sqlite:/tmp/hudi_iceberg/catalog.db' \
  --conf 'spark.sql.catalog.ice.warehouse=file:///tmp/hudi_iceberg/warehouse' \
  --conf 'spark.sql.catalog.ice.cache-enabled=false'
```

- Pass the Hudi and XTable jars with `--jars`, not `spark.driver.extraClassPath`: they must load in the same class
  loader. If your Spark already ships Hudi or Iceberg jars, keep those off the classpath.
- The `ice` catalog is used only in [step 12](#commit-through-an-iceberg-catalog).

### 4. Define helpers

```scala
import spark.implicits._

val base = "file:///tmp/hudi_iceberg/trips"

// rows as read by one format: "iceberg" or "hudi"
def rows(format: String, path: String = base) =
  spark.read.format(format).load(path).select("uuid", "rider", "fare", "city").orderBy("uuid")
// true when the Iceberg reader and the Hudi reader return the same rows
def sameRows(path: String = base) = rows("iceberg", path).collect.toSeq == rows("hudi", path).collect.toSeq
// rider -> fare, read as Iceberg
def fares(path: String = base): Map[String, Double] =
  rows("iceberg", path).collect.map(r => r.getString(1) -> r.getDouble(2)).toMap
// Iceberg snapshot operations, oldest first
def ops(path: String = base): Seq[String] =
  spark.read.format("iceberg").load(s"$path#snapshots").orderBy("committed_at").select("operation").as[String].collect.toSeq
// rider -> (_row_id, _last_updated_sequence_number)
def lineage(path: String = base): Map[String, (Long, Long)] =
  spark.read.format("iceberg").load(path).select("rider", "_row_id", "_last_updated_sequence_number").
    as[(String, Long, Long)].collect.map { case (r, id, seq) => r -> (id, seq) }.toMap
def check(step: String, ok: Boolean): Unit = println(s"CHECK $step ${if (ok) "PASS" else "FAIL"}")
```

### 5. Create the table and insert {#inserts}

A copy-on-write table partitioned by `city`, with `uuid` as the primary key. `hoodie.table.format = 'ICEBERG'` is
the setting that makes it an Iceberg table; the first commit creates the Iceberg metadata under `metadata/` in the
base path, next to Hudi's `.hoodie/`.

```scala
spark.sql("""
CREATE TABLE trips (ts BIGINT, uuid STRING, rider STRING, driver STRING, fare DOUBLE, city STRING)
USING hudi
PARTITIONED BY (city)
LOCATION 'file:///tmp/hudi_iceberg/trips'
TBLPROPERTIES (
  'primaryKey' = 'uuid',
  'hoodie.table.format' = 'ICEBERG'
)""")

spark.sql("""
INSERT INTO trips VALUES
(1695159649087,'334e26e9-8355-45cc-97c6-c31daf0df330','rider-A','driver-K',19.10,'san_francisco'),
(1695091554788,'e96c4396-3fad-413a-a942-4cb36106d721','rider-C','driver-M',27.70,'san_francisco'),
(1695046462179,'9909a8b1-2d15-4d3d-8ec9-efc48c536a00','rider-D','driver-L',33.90,'san_francisco'),
(1695332066204,'1dced545-862b-4ceb-8b43-d2a568f6616b','rider-E','driver-O',93.50,'san_francisco'),
(1695516137016,'e3cf430c-889d-4015-bc98-59bdce1e530c','rider-F','driver-P',34.15,'sao_paulo'),
(1695376420876,'7a84095f-737f-40bc-b62f-6b69664712d2','rider-G','driver-Q',43.40,'sao_paulo'),
(1695173887231,'3eeb61f7-c2b0-4636-99bd-5d7a5a1d2c04','rider-I','driver-S',41.06,'chennai'),
(1695115999911,'c8abbe79-8d89-47ea-b4ce-4d224bae5bfa','rider-J','driver-T',17.85,'chennai')""")

rows("iceberg").show(false)
spark.read.format("iceberg").load(s"$base#snapshots").
  selectExpr("snapshot_id", "operation", "get_json_object(summary['XTABLE_METADATA'], '$.sourceIdentifier') AS hudi_instant").show(false)
spark.sql("CALL show_commits(table => 'trips', limit => 10)").select("commit_time", "action", "total_records_written").show(false)

val idsAtInsert = lineage()
check("insert", sameRows() && fares().size == 8 && ops() == Seq("append"))
```

Expect `CHECK insert PASS`. The snapshot's `hudi_instant` equals the `commit_time` that `show_commits` prints.

- `hoodie.table.format`: write it as `ICEBERG`, in upper case.
- The table is copy-on-write and Iceberg format version 3, both defaults. See [Iceberg format version](#iceberg-format-version).
- The table has no ordering field, so the latest write of a key wins.

### 6. Update, delete and merge {#upserts}

Hudi locates each record through its index and rewrites the file that holds it. Each statement commits one Iceberg
`overwrite` snapshot.

```scala
spark.sql("UPDATE trips SET fare = 25.0 WHERE rider = 'rider-D'")
spark.sql("DELETE FROM trips WHERE rider = 'rider-F'")

spark.sql("""
CREATE OR REPLACE TEMP VIEW trip_changes AS SELECT * FROM VALUES
  (1695091554788L, 'e96c4396-3fad-413a-a942-4cb36106d721', 'rider-C', 'driver-M', 31.40D, 'san_francisco'),
  (1695517776122L, 'f1a2b3c4-0000-4000-8000-000000000001', 'rider-K', 'driver-U', 12.25D, 'chennai')
  AS t(ts, uuid, rider, driver, fare, city)""")
spark.sql("""
MERGE INTO trips AS target USING trip_changes AS source ON target.uuid = source.uuid
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *""")

rows("iceberg").show(false)

val afterMerge = Map("rider-A" -> 19.1, "rider-C" -> 31.4, "rider-D" -> 25.0, "rider-E" -> 93.5,
  "rider-G" -> 43.4, "rider-I" -> 41.06, "rider-J" -> 17.85, "rider-K" -> 12.25)
check("update-delete-merge", sameRows() && fares() == afterMerge &&
  ops() == Seq("append", "overwrite", "overwrite", "overwrite"))
```

Expect `CHECK update-delete-merge PASS`: `rider-D` has fare 25.0, `rider-F` is gone, `rider-C` has the merged fare
and `rider-K` is new.

### 7. Row lineage {#row-lineage}

With format version 3, every row has a `_row_id` and a `_last_updated_sequence_number`, and Hudi maintains them as
Iceberg does. Rows that were not changed keep both. Updated rows keep their `_row_id` and take the sequence number of
the commit that changed them. New rows get new ids.

```scala
spark.read.format("iceberg").load(base).
  select("rider", "fare", "_row_id", "_last_updated_sequence_number").orderBy("rider").show(false)

val idsAfterMerge = lineage()
check("lineage-unchanged", Seq("rider-A", "rider-E", "rider-G", "rider-I", "rider-J").forall(r => idsAfterMerge(r) == idsAtInsert(r)))
check("lineage-updated", Seq("rider-C", "rider-D").forall(r =>
  idsAfterMerge(r)._1 == idsAtInsert(r)._1 && idsAfterMerge(r)._2 > idsAtInsert(r)._2))
check("lineage-new", idsAfterMerge("rider-K")._1 >= 8)
```

Expect `CHECK lineage-unchanged PASS`, `CHECK lineage-updated PASS` and `CHECK lineage-new PASS`. The insert gave ids
0 to 7, so the new row's id is 8 or more.

### 8. Time travel

Every Hudi commit is an Iceberg snapshot, so Iceberg time travel works. Read the first snapshot:

```scala
val firstSnapshot = spark.read.format("iceberg").load(s"$base#snapshots").
  orderBy("committed_at").select("snapshot_id").as[Long].head
val asOfFirst = spark.read.format("iceberg").option("snapshot-id", firstSnapshot).load(base)
asOfFirst.select("uuid", "rider", "fare", "city").orderBy("uuid").show(false)

check("time-travel", asOfFirst.count == 8 && asOfFirst.filter("rider = 'rider-F' AND fare = 34.15").count == 1)
```

Expect `CHECK time-travel PASS`: the original 8 rows, including `rider-F`.

How far back you can travel follows Hudi's retention: the [cleaner](cleaning.md) deletes old file versions, and
[archival](#archival) expires the snapshots of archived instants. A rolled-back commit keeps its snapshot in the
Iceberg history, but its files are gone; do not time travel to it.

### 9. Roll back the merge {#rollback}

Roll back the latest commit, the `MERGE INTO`. The rollback does not rewind Iceberg's history: it commits a new
snapshot that restores the files from before the merge.

```scala
val latest = spark.sql("CALL show_commits(table => 'trips', limit => 1)").select("commit_time").as[String].head
spark.sql(s"CALL rollback_to_instant(table => 'trips', instant_time => '$latest')").show(false)

rows("iceberg").show(false)

val afterRollback = afterMerge - "rider-K" + ("rider-C" -> 27.7)
check("rollback", sameRows() && fares() == afterRollback && ops().size == 5 && ops().last == "overwrite")
```

Expect `CHECK rollback PASS`: `rider-C` has its old fare and `rider-K` is gone.

Roll back only the latest commit. Hudi refuses to roll back a commit that comes before a completed clustering, or
before the latest compaction of Hudi's metadata table, so this guide rolls back before it runs table services.

### 10. Clustering and clean {#clustering}

Table services run from Hudi, as on any Hudi table. Clustering rewrites the files sorted by `uuid`; rows and row ids
do not change. The cleaner deletes file versions that are no longer needed.

```scala
val idsBeforeClustering = lineage()
spark.sql("CALL run_clustering(table => 'trips', order => 'uuid')").show(false)
check("clustering", sameRows() && fares() == afterRollback && lineage() == idsBeforeClustering && ops().size == 6)

spark.sql("CALL run_clean(table => 'trips', retain_commits => 1)").show(false)
check("clean", sameRows() && fares() == afterRollback)
```

Expect `CHECK clustering PASS` and `CHECK clean PASS`. On a table this small and new, the cleaner may find nothing
to delete; it then returns no rows and commits nothing. When it deletes files, it commits a snapshot with no file
changes, because the deleted files belonged to older snapshots only.

#### Archival {#archival}

Archival moves old instants out of Hudi's active timeline, and the plugin expires their Iceberg snapshots. Hudi
archives automatically once the active timeline is longer than `hoodie.keep.max.commits` (30 by default), so the
few commits in this guide archive nothing.

:::caution
Keep `hoodie.keep.min.commits` (20 by default) above `hoodie.metadata.compact.max.delta.commits` (10 by default).
If archival runs before Hudi's metadata table has compacted the archived commits, for example through
`archive_commits` with small `min_commits` and `max_commits` on a new table, the plugin can no longer list those
commits' files and stops publishing until the metadata table is rebuilt. The defaults are safe.
:::

### 11. Merge-on-read with deletion vectors {#merge-on-read}

A merge-on-read table is published as Iceberg format version 3 with deletion vectors. Hudi writes each update as a
positional delete against the file that holds the old row, plus an insert of the new row, and the plugin turns
those deletes into one deletion vector per data file. The DataFrame writer is the tested path for merge-on-read.

```scala
val morPath = "file:///tmp/hudi_iceberg/trips_mor"
val morOptions = Map(
  "hoodie.table.name" -> "trips_mor",
  "hoodie.datasource.write.table.type" -> "MERGE_ON_READ",
  "hoodie.datasource.write.recordkey.field" -> "uuid",
  "hoodie.datasource.write.partitionpath.field" -> "city",
  "hoodie.table.format" -> "ICEBERG",
  "hoodie.write.updates.as.deletes.and.inserts" -> "true")
val columns = Seq("ts", "uuid", "rider", "driver", "fare", "city")
def writeMor(rows: Seq[(Long, String, String, String, Double, String)], operation: String): Unit =
  rows.toDF(columns: _*).write.format("hudi").options(morOptions).
    option("hoodie.datasource.write.operation", operation).mode("append").save(morPath)

writeMor(Seq(
  (1695159649087L, "334e26e9-8355-45cc-97c6-c31daf0df330", "rider-A", "driver-K", 19.10, "san_francisco"),
  (1695091554788L, "e96c4396-3fad-413a-a942-4cb36106d721", "rider-C", "driver-M", 27.70, "san_francisco"),
  (1695516137016L, "e3cf430c-889d-4015-bc98-59bdce1e530c", "rider-F", "driver-P", 34.15, "sao_paulo"),
  (1695115999911L, "c8abbe79-8d89-47ea-b4ce-4d224bae5bfa", "rider-J", "driver-T", 17.85, "chennai")), "insert")
writeMor(Seq(
  (1695091554788L, "e96c4396-3fad-413a-a942-4cb36106d721", "rider-C", "driver-M", 31.40, "san_francisco"),
  (1695517776122L, "f1a2b3c4-0000-4000-8000-000000000001", "rider-K", "driver-U", 12.25, "chennai")), "upsert")
writeMor(Seq(
  (1695516137016L, "e3cf430c-889d-4015-bc98-59bdce1e530c", "rider-F", "driver-P", 34.15, "sao_paulo")), "delete")

val deleteFiles = spark.read.format("iceberg").load(s"$morPath#delete_files")
deleteFiles.selectExpr("file_format", "substring_index(referenced_data_file, '/', -2) AS data_file", "record_count").show(false)

val morRows = Map("rider-A" -> 19.1, "rider-C" -> 31.4, "rider-J" -> 17.85, "rider-K" -> 12.25)
check("mor", sameRows(morPath) && fares(morPath) == morRows && ops(morPath) == Seq("append", "overwrite", "delete") &&
  deleteFiles.count == 2 && deleteFiles.filter("file_format = 'PUFFIN'").count == 2)
```

Expect `CHECK mor PASS`: two deletion vectors, one for the old version of `rider-C` and one for the deleted
`rider-F`. The upsert adds files and deletion vectors, so it is an `overwrite`; the delete adds only a deletion
vector, so it is a `delete`.

`hoodie.write.updates.as.deletes.and.inserts=true` turns on the write mode. Hudi's defaults do the rest: the
`SIMPLE` index locates each record's row position, and with no ordering field the latest write wins.

#### Compaction

Compaction rewrites the data files that have deletion vectors, and the Iceberg snapshot drops their vectors.
`hoodie.compact.inline.max.delta.commits=1` lets Hudi schedule it after a single delta commit.

```scala
spark.sql(s"CALL run_compaction(op => 'run', path => '$morPath', options => 'hoodie.compact.inline.max.delta.commits=1')").show(false)

check("compaction", sameRows(morPath) && fares(morPath) == morRows &&
  spark.read.format("iceberg").load(s"$morPath#delete_files").count == 0)
```

Expect `CHECK compaction PASS`: no deletion vectors left, same four rows.

### 12. Commit through an Iceberg catalog {#commit-through-an-iceberg-catalog}

By default the Iceberg metadata is path-based. To register the table in an Iceberg catalog, name the catalog in the
`hoodie.table.format.iceberg.*` settings. Every Hudi commit then commits to the catalog, and engines find the table
by name: the Hudi table name, in the namespace you set. Here Iceberg's `JdbcCatalog` on a SQLite file stands in for
a real catalog; the `ice` Spark catalog from step 3 points at the same database.

```scala
spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.db")

val catPath = "file:///tmp/hudi_iceberg/trips_cat"
val catOptions = Map(
  "hoodie.table.name" -> "trips_cat",
  "hoodie.datasource.write.recordkey.field" -> "uuid",
  "hoodie.datasource.write.partitionpath.field" -> "city",
  "hoodie.table.format" -> "ICEBERG",
  "hoodie.table.format.iceberg.catalog.name" -> "ice",
  "hoodie.table.format.iceberg.catalog.impl" -> "org.apache.iceberg.jdbc.JdbcCatalog",
  "hoodie.table.format.iceberg.catalog.option.uri" -> "jdbc:sqlite:/tmp/hudi_iceberg/catalog.db",
  "hoodie.table.format.iceberg.catalog.option.warehouse" -> "file:///tmp/hudi_iceberg/warehouse",
  "hoodie.table.format.iceberg.namespace" -> "db")

Seq(
  (1695159649087L, "334e26e9-8355-45cc-97c6-c31daf0df330", "rider-A", "driver-K", 19.10, "san_francisco"),
  (1695115999911L, "c8abbe79-8d89-47ea-b4ce-4d224bae5bfa", "rider-J", "driver-T", 17.85, "chennai")
).toDF(columns: _*).write.format("hudi").options(catOptions).
  option("hoodie.datasource.write.operation", "upsert").mode("append").save(catPath)

spark.sql("SELECT uuid, rider, fare, city FROM ice.db.trips_cat").show(false)
val catSnapshot = spark.sql("SELECT snapshot_id FROM ice.db.trips_cat.snapshots").as[Long].head
check("catalog", spark.table("ice.db.trips_cat").count == 2 &&
  spark.read.option("snapshot-id", catSnapshot).table("ice.db.trips_cat").count == 2)
```

Expect `CHECK catalog PASS`.

- `cache-enabled=false` on the Spark catalog matters: Hudi commits through its own catalog client, so a table
  cached by Spark would go stale.
- For time travel by name, use the DataFrame reader with the `snapshot-id` option, as above. SQL `VERSION AS OF`
  fails on Iceberg tables in a session that loads Hudi's SQL extension.
- The table's location in the catalog is the Hudi base path. The catalog's `warehouse` setting is required by the
  catalog but does not hold this table. For AWS Glue, see the [Pluggable Table Format in action on AWS](pluggable-table-format-iceberg-aws-demo.md).

| Setting | Meaning |
|:--------|:--------|
| `hoodie.table.format.iceberg.catalog.name` | Catalog name. Defaults to `iceberg` when only the implementation is set. |
| `hoodie.table.format.iceberg.catalog.impl` | Iceberg catalog class, for example `org.apache.iceberg.aws.glue.GlueCatalog` or `org.apache.iceberg.jdbc.JdbcCatalog`. Required when a catalog is named. |
| `hoodie.table.format.iceberg.catalog.option.<key>` | Passed to the catalog as `<key>`, for example `warehouse` or `uri`. |
| `hoodie.table.format.iceberg.namespace` | Namespace of the table, dot separated, for example `db` or `a.b`. |

If a writer cannot set table settings, the plugin also reads the catalog from the Hadoop configuration, under
`xtable.iceberg.catalog.name`, `xtable.iceberg.catalog.impl` and `xtable.iceberg.catalog.option.<key>`, when
`hoodie.properties` names no catalog.

### Run all steps unattended

Save the Scala blocks of steps 4 to 12, in order, to `quickstart.scala`, add `System.exit(0)` at the end, and run
the step 3 command with `-i quickstart.scala` appended. Then:

```shell
grep '^CHECK' <spark-shell output>
```

Expect 12 lines, all `PASS`.

## Iceberg format version {#iceberg-format-version}

`hoodie.table.format.iceberg.format-version` sets the Iceberg format version of a new table:

- `3` is the default. Use it unless a reader you need cannot read version 3.
- `2` is for engines that read only version 2, for example Amazon Athena at the time of writing. Version 2
  works for copy-on-write tables only.
- Other values are refused. If the table config does not set it, the plugin falls back to
  `xtable.iceberg.format-version` in the Hadoop configuration (for example
  `--conf spark.hadoop.xtable.iceberg.format-version=2`), then to `3`.
- An existing Iceberg table keeps its version. Choose it when you create the table.

Format version 3 gives you [row lineage](#row-lineage) and, for merge-on-read tables,
[deletion vectors](#merge-on-read).

## Limitations

These hold for the current snapshot builds.

- **Preview builds.** Hudi `1.3.0-SNAPSHOT` and XTable `0.5.0-SNAPSHOT` until the next releases.
- **Spark 3.5 only**, with Scala 2.12. Other Spark versions and other engines, such as Flink, are not supported
  yet.
- **Hudi is the only writer.** Do not write to the table or run Iceberg maintenance procedures (such as
  `expire_snapshots` or `rewrite_data_files`) from Iceberg engines. A snapshot that Hudi did not write makes the
  table unreadable through Hudi until it is rolled back on the Iceberg side.
- **One writer at a time.** Table services run inside the writing job, inline or async. Separate table service
  jobs and concurrent writers are not supported yet.
- **Hudi schema-on-read evolution is not supported** on these tables.
- **Identity partitioning only.** Timestamp-based key generators are refused.
- **Restore is not supported**, and neither are Iceberg branches and tags.
- **Hudi table version 8 or later.** Older table versions are refused.
- **Merge-on-read** needs format version 3, the update-as-delete-insert mode, the `SIMPLE` or `BLOOM` index and
  commit-time ordering. In this mode, partial updates and tables without a primary key are not supported, and log
  compaction is not available. Spark SQL `UPDATE`, `DELETE` and `MERGE INTO` on merge-on-read tables are at an
  early stage; use the DataFrame writer or Hudi Streamer.
- **In merge-on-read `MERGE INTO`, a `WHEN MATCHED` condition cannot reference target columns** (for example
  `WHEN MATCHED AND target.fare > 10`). Conditions on source columns work.
- **Row ids on merge-on-read updates:** an updated row gets a new `_row_id`, because the update is written as a
  delete plus an insert. Rollback can also give restored rows new row ids.
- **SQL `VERSION AS OF` fails on Iceberg tables** in a session that loads Hudi's SQL extension. Use the DataFrame
  reader with the `snapshot-id` option instead.
- **Time travel** reaches back only as far as cleaning and archival allow, and not to a rolled-back commit.
- **Every Hudi reader and writer of the table needs the plugin jars.** Hudi refuses to load a table whose table
  format plugin is missing. Iceberg readers do not need them.
- **Hudi's catalog sync (Hive, Glue) does not know about the table format.** To register the table, use the Iceberg
  catalog settings above.
- **Unpartitioned merge-on-read tables** scan the whole Iceberg table on each delta commit to find the current
  deletion vectors. Partitioned tables scan only the partitions a commit touches.

## Next steps

- Run the same table on AWS: [Pluggable Table Format in action on AWS](pluggable-table-format-iceberg-aws-demo.md).
- Hudi's own features on this table: [table services](compaction.md), [indexes](indexes.md),
  [write operations](write_operations.md).
- For translating existing tables between formats after the fact, see [Apache XTable](syncing_xtable.md).
