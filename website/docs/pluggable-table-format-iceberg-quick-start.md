---
title: "Write Iceberg Tables with Hudi"
keywords: [hudi, iceberg, pluggable table format, xtable, deletion vectors, row lineage, quick start]
summary: "Write an Apache Iceberg table with Hudi's pluggable table format, run Hudi table services on it, and read it with any Iceberg engine"
toc: true
last_modified_at: 2026-11-20T00:00:00-08:00
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Hudi can write a table that is also an Apache Iceberg table. You write with Hudi and keep Hudi's indexes,
upserts and table services. Any engine that reads Iceberg reads the same table. There is one copy of the data.

This works through Hudi's pluggable table format. A table format plugin from
[Apache XTable (incubating)](https://xtable.apache.org/) publishes every Hudi commit as an Iceberg snapshot.

In this guide you use a local Spark 3.5 shell to:

- create a table with `hoodie.table.format=ICEBERG`,
- insert, update, delete and merge rows with Spark SQL,
- read the same table as Iceberg, by path and through an Iceberg catalog, and time travel,
- roll back a commit and run Hudi table services (clustering, clean, archival, and compaction for merge-on-read).

:::caution Preview
This feature is a preview. It needs snapshot builds of Hudi (`1.3.0-SNAPSHOT`) and XTable (`0.5.0-SNAPSHOT`)
until the next releases. Spark 3.5 is the only supported engine for now.
Read the [limitations](#limitations) before you use it with data you care about.
:::

## How it works

- **One copy of the data.** Hudi writes Parquet base files. The Iceberg metadata points at the same files.
  Nothing is copied or converted after the fact.
- **One Hudi commit, one Iceberg snapshot.** The plugin commits an Iceberg snapshot for every completed Hudi
  instant, including table services.
- **Iceberg holds the commit record.** Each snapshot names its Hudi instant in its summary, under the key
  `XTABLE_METADATA`. Hudi rebuilds its timeline of completed instants from the current Iceberg snapshot and its
  ancestors. A Hudi instant counts as completed only when its snapshot is in that history.
- **Hudi is the writer.** Hudi's indexes, merge logic and table services decide what the table holds.
  Iceberg engines read it.

Hudi finds the plugin through Java's `ServiceLoader`. The plugin jar registers
`org.apache.xtable.IcebergTableFormat` as an implementation of `org.apache.hudi.common.HoodieTableFormat`
under the name `ICEBERG`.

This is how each Hudi action shows up in Iceberg:

| Hudi action | What the Iceberg snapshot does |
|:------------|:-------------------------------|
| Insert, upsert, delete, insert overwrite, delete partition (copy-on-write) | Adds the new data files and removes the replaced ones |
| Delta commit (merge-on-read) | Adds the new data files and one deletion vector per touched data file |
| Compaction | Replaces data files with compacted ones; their deletion vectors go away |
| Clustering | Replaces data files with clustered ones |
| Clean | Commits a snapshot with no file changes |
| Savepoint | Commits a snapshot with no file changes |
| Rollback | Commits a new snapshot that restores the files from before the rolled-back commit |
| Archival | Expires the Iceberg snapshots of the archived instants |
| Restore | Not supported |

## Setup

### Requirements

- Spark 3.5 with Scala 2.12.
- Java 17. The plugin has been tested on Java 17.
- These jars:

| Jar | Where it comes from |
|:----|:--------------------|
| `hudi-spark3.5-bundle_2.12-1.3.0-SNAPSHOT.jar` | Hudi, built from source |
| `iceberg-spark-runtime-3.5_2.12-1.11.0.jar` | Apache Iceberg 1.11.0 release |
| `xtable-api-0.5.0-SNAPSHOT.jar` | XTable, built from source |
| `xtable-hudi-support-utils-0.5.0-SNAPSHOT.jar` | XTable, built from source |
| `xtable-core_2.12-0.5.0-SNAPSHOT.jar` | XTable, built from source |
| `xtable-iceberg-pluggable-tf_2.12-0.5.0-SNAPSHOT.jar` | XTable, built from source |

### Build the snapshot jars

Until the next Hudi and XTable releases, build both projects from source. Build Hudi first: XTable compiles
against the Hudi snapshot in your local Maven repository.

**TODO(release):** name the Hudi and XTable branches or commits that carry the pluggable table format changes, or
replace this section with release coordinates.

```shell
# Hudi, with JDK 11
git clone https://github.com/apache/hudi.git
cd hudi
mvn clean install -DskipTests -Dscala-2.12 -Dspark3.5 -pl packaging/hudi-spark-bundle -am
# jar: packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-1.3.0-SNAPSHOT.jar
cd ..

# XTable, with JDK 17
git clone https://github.com/apache/incubator-xtable.git
cd incubator-xtable
./mvnw clean package -DskipTests -Dspark.version.prefix=3.5 -pl xtable-hudi-support/xtable-iceberg-pluggable-tf -am
# jars:
#   xtable-api/target/xtable-api-0.5.0-SNAPSHOT.jar
#   xtable-hudi-support/xtable-hudi-support-utils/target/xtable-hudi-support-utils-0.5.0-SNAPSHOT.jar
#   xtable-core/target/xtable-core_2.12-0.5.0-SNAPSHOT.jar
#   xtable-hudi-support/xtable-iceberg-pluggable-tf/target/xtable-iceberg-pluggable-tf_2.12-0.5.0-SNAPSHOT.jar
```

Copy the five jars and `iceberg-spark-runtime-3.5_2.12-1.11.0.jar` (from Maven Central) into one folder.

### Start Spark

Point `JARS_DIR` at that folder and start a shell with Hudi's and Iceberg's SQL extensions.

<Tabs
groupId="programming-language"
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
{ label: 'Spark SQL', value: 'sparksql', },
]}
>
<TabItem value="scala">

```shell
export JARS_DIR=/path/to/jars
export PTF_JARS=$JARS_DIR/hudi-spark3.5-bundle_2.12-1.3.0-SNAPSHOT.jar,\
$JARS_DIR/iceberg-spark-runtime-3.5_2.12-1.11.0.jar,\
$JARS_DIR/xtable-api-0.5.0-SNAPSHOT.jar,\
$JARS_DIR/xtable-hudi-support-utils-0.5.0-SNAPSHOT.jar,\
$JARS_DIR/xtable-core_2.12-0.5.0-SNAPSHOT.jar,\
$JARS_DIR/xtable-iceberg-pluggable-tf_2.12-0.5.0-SNAPSHOT.jar

spark-shell --master "local[2]" \
  --jars $PTF_JARS \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions'
```

</TabItem>
<TabItem value="python">

```shell
export PYSPARK_PYTHON=$(which python3)
export JARS_DIR=/path/to/jars
export PTF_JARS=$JARS_DIR/hudi-spark3.5-bundle_2.12-1.3.0-SNAPSHOT.jar,\
$JARS_DIR/iceberg-spark-runtime-3.5_2.12-1.11.0.jar,\
$JARS_DIR/xtable-api-0.5.0-SNAPSHOT.jar,\
$JARS_DIR/xtable-hudi-support-utils-0.5.0-SNAPSHOT.jar,\
$JARS_DIR/xtable-core_2.12-0.5.0-SNAPSHOT.jar,\
$JARS_DIR/xtable-iceberg-pluggable-tf_2.12-0.5.0-SNAPSHOT.jar

pyspark --master "local[2]" \
  --jars $PTF_JARS \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions'
```

</TabItem>
<TabItem value="sparksql">

```shell
export JARS_DIR=/path/to/jars
export PTF_JARS=$JARS_DIR/hudi-spark3.5-bundle_2.12-1.3.0-SNAPSHOT.jar,\
$JARS_DIR/iceberg-spark-runtime-3.5_2.12-1.11.0.jar,\
$JARS_DIR/xtable-api-0.5.0-SNAPSHOT.jar,\
$JARS_DIR/xtable-hudi-support-utils-0.5.0-SNAPSHOT.jar,\
$JARS_DIR/xtable-core_2.12-0.5.0-SNAPSHOT.jar,\
$JARS_DIR/xtable-iceberg-pluggable-tf_2.12-0.5.0-SNAPSHOT.jar

spark-sql --master "local[2]" \
  --jars $PTF_JARS \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions'
```

</TabItem>
</Tabs>

:::note
Pass the Hudi bundle and the XTable jars with `--jars`, not `spark.driver.extraClassPath`. They must load in the
same class loader. If your Spark already ships Hudi or Iceberg jars, keep those off the classpath.
:::

The SQL in this guide runs as is in `spark-sql`. In `spark-shell` or `pyspark`, wrap each statement in
`spark.sql("...")`. Reading by path uses the DataFrame API, so those steps show Scala and Python.

The outputs shown come from running this guide on Spark 3.5.5 and Java 17 with a format version 3 table. Commit
times, snapshot ids and file names differ on every run.

## Create a table

Create a copy-on-write table partitioned by `city`, with `uuid` as the primary key.
`hoodie.table.format = 'ICEBERG'` is the one setting that makes it an Iceberg table.

```sql
CREATE TABLE trips (
    ts BIGINT,
    uuid STRING,
    rider STRING,
    driver STRING,
    fare DOUBLE,
    city STRING
) USING hudi
PARTITIONED BY (city)
LOCATION 'file:///tmp/hudi_iceberg/trips'
TBLPROPERTIES (
    'type' = 'cow',
    'primaryKey' = 'uuid',
    'hoodie.table.format' = 'ICEBERG',
    'hoodie.table.format.iceberg.format-version' = '3'
);
```

- `hoodie.table.format`: write it as `ICEBERG`, in upper case.
- `hoodie.table.format.iceberg.format-version`: `3` (the default) or `2`. See [Iceberg format version](#iceberg-format-version).
- The table has no ordering field, so Hudi uses commit-time ordering: the latest write of a key wins.

Hudi keeps the settings in `.hoodie/hoodie.properties`, so every later writer and table service publishes to the
same Iceberg table. With no catalog configured, the Iceberg metadata lives under `metadata/` in the table's base
path, next to Hudi's `.hoodie/` folder. To commit through a catalog instead, see
[Commit through an Iceberg catalog](#commit-through-an-iceberg-catalog).

## Insert data {#inserts}

```sql
INSERT INTO trips VALUES
(1695159649087,'334e26e9-8355-45cc-97c6-c31daf0df330','rider-A','driver-K',19.10,'san_francisco'),
(1695091554788,'e96c4396-3fad-413a-a942-4cb36106d721','rider-C','driver-M',27.70,'san_francisco'),
(1695046462179,'9909a8b1-2d15-4d3d-8ec9-efc48c536a00','rider-D','driver-L',33.90,'san_francisco'),
(1695332066204,'1dced545-862b-4ceb-8b43-d2a568f6616b','rider-E','driver-O',93.50,'san_francisco'),
(1695516137016,'e3cf430c-889d-4015-bc98-59bdce1e530c','rider-F','driver-P',34.15,'sao_paulo'),
(1695376420876,'7a84095f-737f-40bc-b62f-6b69664712d2','rider-G','driver-Q',43.40,'sao_paulo'),
(1695173887231,'3eeb61f7-c2b0-4636-99bd-5d7a5a1d2c04','rider-I','driver-S',41.06,'chennai'),
(1695115999911,'c8abbe79-8d89-47ea-b4ce-4d224bae5bfa','rider-J','driver-T',17.85,'chennai');
```

This first commit also creates the Iceberg table.

## Read the table as Iceberg {#read-as-iceberg}

Read the base path with the Iceberg source. Then read it with Hudi to compare.

<Tabs
groupId="programming-language"
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
]}
>
<TabItem value="scala">

```scala
// spark-shell
val basePath = "file:///tmp/hudi_iceberg/trips"

// Iceberg reader, path-based metadata
spark.read.format("iceberg").load(basePath).
  select("uuid", "rider", "fare", "city").orderBy("uuid").show(false)

// Hudi reader, same files
spark.read.format("hudi").load(basePath).
  select("uuid", "rider", "fare", "city").orderBy("uuid").show(false)
```

</TabItem>
<TabItem value="python">

```python
# pyspark
base_path = "file:///tmp/hudi_iceberg/trips"

# Iceberg reader, path-based metadata
spark.read.format("iceberg").load(base_path) \
    .select("uuid", "rider", "fare", "city").orderBy("uuid").show(truncate=False)

# Hudi reader, same files
spark.read.format("hudi").load(base_path) \
    .select("uuid", "rider", "fare", "city").orderBy("uuid").show(truncate=False)
```

</TabItem>
</Tabs>

Both readers return the same rows:

```
+------------------------------------+-------+-----+-------------+
|uuid                                |rider  |fare |city         |
+------------------------------------+-------+-----+-------------+
|1dced545-862b-4ceb-8b43-d2a568f6616b|rider-E|93.5 |san_francisco|
|334e26e9-8355-45cc-97c6-c31daf0df330|rider-A|19.1 |san_francisco|
|3eeb61f7-c2b0-4636-99bd-5d7a5a1d2c04|rider-I|41.06|chennai      |
|7a84095f-737f-40bc-b62f-6b69664712d2|rider-G|43.4 |sao_paulo    |
|9909a8b1-2d15-4d3d-8ec9-efc48c536a00|rider-D|33.9 |san_francisco|
|c8abbe79-8d89-47ea-b4ce-4d224bae5bfa|rider-J|17.85|chennai      |
|e3cf430c-889d-4015-bc98-59bdce1e530c|rider-F|34.15|sao_paulo    |
|e96c4396-3fad-413a-a942-4cb36106d721|rider-C|27.7 |san_francisco|
+------------------------------------+-------+-----+-------------+
```

Iceberg readers also see Hudi's meta columns, such as `_hoodie_commit_time` and `_hoodie_record_key`, because they
are stored in the data files.

List the Iceberg snapshots. Each one carries the Hudi instant it was published for.

<Tabs
groupId="programming-language"
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
]}
>
<TabItem value="scala">

```scala
spark.read.format("iceberg").load(s"$basePath#snapshots").
  selectExpr("committed_at", "snapshot_id", "operation",
    "get_json_object(summary['XTABLE_METADATA'], '$.sourceIdentifier') AS hudi_instant").
  orderBy("committed_at").show(false)
```

</TabItem>
<TabItem value="python">

```python
spark.read.format("iceberg").load(base_path + "#snapshots") \
    .selectExpr("committed_at", "snapshot_id", "operation",
                "get_json_object(summary['XTABLE_METADATA'], '$.sourceIdentifier') AS hudi_instant") \
    .orderBy("committed_at").show(truncate=False)
```

</TabItem>
</Tabs>

```
+-----------------------+-------------------+---------+-----------------+
|committed_at           |snapshot_id        |operation|hudi_instant     |
+-----------------------+-------------------+---------+-----------------+
|2026-11-20 09:52:49.209|3222430336366022149|append   |20261120095244428|
+-----------------------+-------------------+---------+-----------------+
```

Compare with Hudi's view of the same history:

```sql
CALL show_commits(table => 'trips', limit => 10);
```

The `commit_time` matches the snapshot's `hudi_instant` (output trimmed to a few columns):

```
+-----------------+------+---------------------+-----------------+-------------------+
|commit_time      |action|total_records_written|total_files_added|total_files_updated|
+-----------------+------+---------------------+-----------------+-------------------+
|20261120095244428|commit|8                    |3                |0                  |
+-----------------+------+---------------------+-----------------+-------------------+
```

## Update data {#upserts}

```sql
UPDATE trips SET fare = 25.0 WHERE rider = 'rider-D';
```

Hudi locates the record with its index and rewrites the file that holds it. The new Iceberg snapshot swaps the old
file for the new one.

## Delete data {#deletes}

```sql
DELETE FROM trips WHERE rider = 'rider-F';
```

## Merge data {#merge}

Use `MERGE INTO` to apply a batch of changes: update the rows that match and insert the rest.

```sql
CREATE OR REPLACE TEMP VIEW trip_changes AS
SELECT * FROM VALUES
  (1695091554788L, 'e96c4396-3fad-413a-a942-4cb36106d721', 'rider-C', 'driver-M', 31.40D, 'san_francisco'),
  (1695517776122L, 'f1a2b3c4-0000-4000-8000-000000000001', 'rider-K', 'driver-U', 12.25D, 'chennai')
  AS t(ts, uuid, rider, driver, fare, city);

MERGE INTO trips AS target
USING trip_changes AS source
ON target.uuid = source.uuid
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

Read the table again, with Iceberg and with Hudi, as in [Read the table as Iceberg](#read-as-iceberg). Both show
the same rows: `rider-D` has the new fare, `rider-F` is gone, `rider-C` has the merged fare and `rider-K` is new.

```
+------------------------------------+-------+-----+-------------+
|uuid                                |rider  |fare |city         |
+------------------------------------+-------+-----+-------------+
|1dced545-862b-4ceb-8b43-d2a568f6616b|rider-E|93.5 |san_francisco|
|334e26e9-8355-45cc-97c6-c31daf0df330|rider-A|19.1 |san_francisco|
|3eeb61f7-c2b0-4636-99bd-5d7a5a1d2c04|rider-I|41.06|chennai      |
|7a84095f-737f-40bc-b62f-6b69664712d2|rider-G|43.4 |sao_paulo    |
|9909a8b1-2d15-4d3d-8ec9-efc48c536a00|rider-D|25.0 |san_francisco|
|c8abbe79-8d89-47ea-b4ce-4d224bae5bfa|rider-J|17.85|chennai      |
|e96c4396-3fad-413a-a942-4cb36106d721|rider-C|31.4 |san_francisco|
|f1a2b3c4-0000-4000-8000-000000000001|rider-K|12.25|chennai      |
+------------------------------------+-------+-----+-------------+
```

The snapshot list now has one snapshot per commit. Each update, delete and merge replaces data files, so Iceberg
records it as an `overwrite`:

```
+-----------------------+-------------------+---------+-----------------+
|committed_at           |snapshot_id        |operation|hudi_instant     |
+-----------------------+-------------------+---------+-----------------+
|2026-11-20 09:52:49.209|3222430336366022149|append   |20261120095244428|
|2026-11-20 09:52:54.039|8734336489192891270|overwrite|20261120095253045|
|2026-11-20 09:52:56.825|7653147752624864460|overwrite|20261120095255574|
|2026-11-20 09:52:59.751|7604068301687746896|overwrite|20261120095257464|
+-----------------------+-------------------+---------+-----------------+
```

## Time travel

Every Hudi commit is an Iceberg snapshot, so Iceberg time travel works on the table. Read the table as of the
first snapshot, before the update, delete and merge:

<Tabs
groupId="programming-language"
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
]}
>
<TabItem value="scala">

```scala
val firstSnapshot = spark.read.format("iceberg").load(s"$basePath#snapshots").
  orderBy("committed_at").select("snapshot_id").head.getLong(0)

spark.read.format("iceberg").
  option("snapshot-id", firstSnapshot).
  load(basePath).
  select("uuid", "rider", "fare", "city").orderBy("uuid").show(false)
```

</TabItem>
<TabItem value="python">

```python
first_snapshot = spark.read.format("iceberg").load(base_path + "#snapshots") \
    .orderBy("committed_at").select("snapshot_id").head()[0]

spark.read.format("iceberg") \
    .option("snapshot-id", first_snapshot) \
    .load(base_path) \
    .select("uuid", "rider", "fare", "city").orderBy("uuid").show(truncate=False)
```

</TabItem>
</Tabs>

The first snapshot still has the original eight rows, with `rider-F` and the old fares of `rider-C` and `rider-D`:

```
+------------------------------------+-------+-----+-------------+
|uuid                                |rider  |fare |city         |
+------------------------------------+-------+-----+-------------+
|1dced545-862b-4ceb-8b43-d2a568f6616b|rider-E|93.5 |san_francisco|
|334e26e9-8355-45cc-97c6-c31daf0df330|rider-A|19.1 |san_francisco|
|3eeb61f7-c2b0-4636-99bd-5d7a5a1d2c04|rider-I|41.06|chennai      |
|7a84095f-737f-40bc-b62f-6b69664712d2|rider-G|43.4 |sao_paulo    |
|9909a8b1-2d15-4d3d-8ec9-efc48c536a00|rider-D|33.9 |san_francisco|
|c8abbe79-8d89-47ea-b4ce-4d224bae5bfa|rider-J|17.85|chennai      |
|e3cf430c-889d-4015-bc98-59bdce1e530c|rider-F|34.15|sao_paulo    |
|e96c4396-3fad-413a-a942-4cb36106d721|rider-C|27.7 |san_francisco|
+------------------------------------+-------+-----+-------------+
```

How far back you can travel follows Hudi's retention:

- The [cleaner](cleaning.md) deletes old file versions. A snapshot whose files were cleaned cannot be read.
- [Archival](#archival) expires the snapshots of archived instants.
- A rolled-back commit keeps its snapshot in the Iceberg history, but its files are gone. Do not time travel to it.

## Roll back a commit {#rollback}

Find the latest commit and roll it back. Here that is the `MERGE INTO` commit.

```sql
CALL show_commits(table => 'trips', limit => 5);

-- use the commit_time of the latest commit from the output above
CALL rollback_to_instant(table => 'trips', instant_time => '<commit_time>');
```

```
+-----------------+------+---------------------+
|commit_time      |action|total_records_written|
+-----------------+------+---------------------+
|20261120095257464|commit|7                    |
|20261120095255574|commit|1                    |
|20261120095253045|commit|4                    |
|20261120095244428|commit|8                    |
+-----------------+------+---------------------+
```

The rollback does not rewind Iceberg's history. It commits a new snapshot that restores the files from before the
merge. Readers see the rows as they were before the merge: `rider-C` has its old fare and `rider-K` is gone.

```
+------------------------------------+-------+-----+-------------+
|uuid                                |rider  |fare |city         |
+------------------------------------+-------+-----+-------------+
|1dced545-862b-4ceb-8b43-d2a568f6616b|rider-E|93.5 |san_francisco|
|334e26e9-8355-45cc-97c6-c31daf0df330|rider-A|19.1 |san_francisco|
|3eeb61f7-c2b0-4636-99bd-5d7a5a1d2c04|rider-I|41.06|chennai      |
|7a84095f-737f-40bc-b62f-6b69664712d2|rider-G|43.4 |sao_paulo    |
|9909a8b1-2d15-4d3d-8ec9-efc48c536a00|rider-D|25.0 |san_francisco|
|c8abbe79-8d89-47ea-b4ce-4d224bae5bfa|rider-J|17.85|chennai      |
|e96c4396-3fad-413a-a942-4cb36106d721|rider-C|27.7 |san_francisco|
+------------------------------------+-------+-----+-------------+
```

The snapshot list gains one more `overwrite`, for the rollback instant:

```
+-----------------------+-------------------+---------+-----------------+
|committed_at           |snapshot_id        |operation|hudi_instant     |
+-----------------------+-------------------+---------+-----------------+
|2026-11-20 09:52:49.209|3222430336366022149|append   |20261120095244428|
|2026-11-20 09:52:54.039|8734336489192891270|overwrite|20261120095253045|
|2026-11-20 09:52:56.825|7653147752624864460|overwrite|20261120095255574|
|2026-11-20 09:52:59.751|7604068301687746896|overwrite|20261120095257464|
|2026-11-20 09:53:02.016|6552271230735103426|overwrite|20261120095301160|
+-----------------------+-------------------+---------+-----------------+
```

:::note
Roll back the latest commit. Hudi refuses to roll back a commit that comes before a completed clustering, or
before the latest compaction of Hudi's metadata table. In this guide, roll back first, then run table services.
:::

## Run table services

Table services run from Hudi, as on any Hudi table. Each one that changes files commits an Iceberg snapshot.

### Clustering

Clustering rewrites small files into larger ones, sorted by the columns you pick.

```sql
CALL run_clustering(table => 'trips', order => 'uuid');
```

```
+-----------------+----------------+---------+-------------------+
|timestamp        |input_group_size|state    |involved_partitions|
+-----------------+----------------+---------+-------------------+
|20261120095302528|3               |COMPLETED|*                  |
+-----------------+----------------+---------+-------------------+
```

The Iceberg snapshot replaces the old files with the clustered ones. Rows and their values do not change, and
neither do their row ids (see [Row lineage](#row-lineage)).

### Clean

The cleaner deletes file versions that are no longer needed.

```sql
CALL run_clean(table => 'trips', retain_commits => 1);
```

On a table this small and new, the cleaner may find nothing to delete yet. It then returns no rows and commits
nothing. When it does delete files, it commits an Iceberg snapshot with no file changes: the deleted files belonged
to older snapshots only.

### Archival {#archival}

Archival moves old instants out of Hudi's active timeline. The plugin then expires the Iceberg snapshots of the
archived instants. Hudi archives automatically after commits, once the active timeline is longer than
`hoodie.keep.max.commits` (30 by default), so the few commits in this guide archive nothing.

:::caution
Keep `hoodie.keep.min.commits` (20 by default) above `hoodie.metadata.compact.max.delta.commits` (10 by default).
If archival runs before Hudi's metadata table has compacted the archived commits, for example through
`archive_commits` with small `min_commits` and `max_commits` on a new table, the plugin can no longer list those
commits' files and stops publishing until the metadata table is rebuilt. The defaults are safe.
:::

See [Cleaning](cleaning.md), [Clustering](clustering.md) and [Compaction](compaction.md).

## Merge-on-read tables with deletion vectors {#merge-on-read}

A merge-on-read (MoR) table is published as Iceberg format version 3 with deletion vectors. For that, Hudi writes
each update as a positional delete against the file that holds the old row, plus an insert of the new row. Log
files then hold only deletes. The plugin turns those deletes into one deletion vector per data file.

Turn this on with the update-as-delete-insert write mode. The DataFrame writer below is the tested path for
merge-on-read tables.

<Tabs
groupId="programming-language"
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
]}
>
<TabItem value="scala">

```scala
// spark-shell
import spark.implicits._

val morPath = "file:///tmp/hudi_iceberg/trips_mor"
val morOptions = Map(
  "hoodie.table.name" -> "trips_mor",
  "hoodie.datasource.write.table.type" -> "MERGE_ON_READ",
  "hoodie.datasource.write.recordkey.field" -> "uuid",
  "hoodie.datasource.write.partitionpath.field" -> "city",
  "hoodie.table.format" -> "ICEBERG",
  "hoodie.write.updates.as.deletes.and.inserts" -> "true",
  "hoodie.write.record.positions" -> "true",
  "hoodie.index.type" -> "SIMPLE",
  "hoodie.record.merge.mode" -> "COMMIT_TIME_ORDERING"
)
val columns = Seq("ts", "uuid", "rider", "driver", "fare", "city")

// insert
Seq(
  (1695159649087L, "334e26e9-8355-45cc-97c6-c31daf0df330", "rider-A", "driver-K", 19.10, "san_francisco"),
  (1695091554788L, "e96c4396-3fad-413a-a942-4cb36106d721", "rider-C", "driver-M", 27.70, "san_francisco"),
  (1695516137016L, "e3cf430c-889d-4015-bc98-59bdce1e530c", "rider-F", "driver-P", 34.15, "sao_paulo"),
  (1695115999911L, "c8abbe79-8d89-47ea-b4ce-4d224bae5bfa", "rider-J", "driver-T", 17.85, "chennai")
).toDF(columns: _*).
  write.format("hudi").options(morOptions).
  option("hoodie.datasource.write.operation", "insert").
  mode("append").save(morPath)

// upsert: one update and one new row
Seq(
  (1695091554788L, "e96c4396-3fad-413a-a942-4cb36106d721", "rider-C", "driver-M", 31.40, "san_francisco"),
  (1695517776122L, "f1a2b3c4-0000-4000-8000-000000000001", "rider-K", "driver-U", 12.25, "chennai")
).toDF(columns: _*).
  write.format("hudi").options(morOptions).
  option("hoodie.datasource.write.operation", "upsert").
  mode("append").save(morPath)

// delete
Seq(
  (1695516137016L, "e3cf430c-889d-4015-bc98-59bdce1e530c", "rider-F", "driver-P", 34.15, "sao_paulo")
).toDF(columns: _*).
  write.format("hudi").options(morOptions).
  option("hoodie.datasource.write.operation", "delete").
  mode("append").save(morPath)
```

</TabItem>
<TabItem value="python">

```python
# pyspark
mor_path = "file:///tmp/hudi_iceberg/trips_mor"
mor_options = {
    "hoodie.table.name": "trips_mor",
    "hoodie.datasource.write.table.type": "MERGE_ON_READ",
    "hoodie.datasource.write.recordkey.field": "uuid",
    "hoodie.datasource.write.partitionpath.field": "city",
    "hoodie.table.format": "ICEBERG",
    "hoodie.write.updates.as.deletes.and.inserts": "true",
    "hoodie.write.record.positions": "true",
    "hoodie.index.type": "SIMPLE",
    "hoodie.record.merge.mode": "COMMIT_TIME_ORDERING",
}
columns = ["ts", "uuid", "rider", "driver", "fare", "city"]

def write(rows, operation):
    (spark.createDataFrame(rows, columns).write.format("hudi").options(**mor_options)
     .option("hoodie.datasource.write.operation", operation).mode("append").save(mor_path))

write([(1695159649087, "334e26e9-8355-45cc-97c6-c31daf0df330", "rider-A", "driver-K", 19.10, "san_francisco"),
       (1695091554788, "e96c4396-3fad-413a-a942-4cb36106d721", "rider-C", "driver-M", 27.70, "san_francisco"),
       (1695516137016, "e3cf430c-889d-4015-bc98-59bdce1e530c", "rider-F", "driver-P", 34.15, "sao_paulo"),
       (1695115999911, "c8abbe79-8d89-47ea-b4ce-4d224bae5bfa", "rider-J", "driver-T", 17.85, "chennai")],
      "insert")
write([(1695091554788, "e96c4396-3fad-413a-a942-4cb36106d721", "rider-C", "driver-M", 31.40, "san_francisco"),
       (1695517776122, "f1a2b3c4-0000-4000-8000-000000000001", "rider-K", "driver-U", 12.25, "chennai")],
      "upsert")
write([(1695516137016, "e3cf430c-889d-4015-bc98-59bdce1e530c", "rider-F", "driver-P", 34.15, "sao_paulo")],
      "delete")
```

</TabItem>
</Tabs>

The settings that matter:

- `hoodie.write.updates.as.deletes.and.inserts=true` turns on the write mode.
- `hoodie.write.record.positions=true` (the default) keeps the row position of each delete.
- `hoodie.index.type`: `SIMPLE` or `BLOOM`. The index must locate records by base file position.
- `hoodie.record.merge.mode=COMMIT_TIME_ORDERING`: the latest write wins.
- The table needs Iceberg format version 3. The plugin refuses a merge-on-read table set to version 2.

Look at the deletion vectors that the update and the delete produced. They are stored in Puffin files.

<Tabs
groupId="programming-language"
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
]}
>
<TabItem value="scala">

```scala
spark.read.format("iceberg").load(s"$morPath#delete_files").
  selectExpr("file_format", "substring_index(referenced_data_file, '/', -2) AS data_file", "record_count").
  show(false)
```

</TabItem>
<TabItem value="python">

```python
spark.read.format("iceberg").load(mor_path + "#delete_files") \
    .selectExpr("file_format", "substring_index(referenced_data_file, '/', -2) AS data_file", "record_count") \
    .show(truncate=False)
```

</TabItem>
</Tabs>

There is one deletion vector for each data file that lost a row: the old version of `rider-C` in `san_francisco`,
and the deleted `rider-F` in `sao_paulo`.

```
+-----------+--------------------------------------------------------------------------------------+------------+
|file_format|data_file                                                                             |record_count|
+-----------+--------------------------------------------------------------------------------------+------------+
|PUFFIN     |sao_paulo/66c20059-1f4d-4284-8b5d-f61b4f8679dd-0_1-21-23_20261120095344049.parquet    |1           |
|PUFFIN     |san_francisco/a0dc6d07-0745-4250-bd62-c69bcc0e226d-0_0-21-22_20261120095344049.parquet|1           |
+-----------+--------------------------------------------------------------------------------------+------------+
```

Iceberg and Hudi read the same four rows:

```
+------------------------------------+-------+-----+-------------+
|uuid                                |rider  |fare |city         |
+------------------------------------+-------+-----+-------------+
|334e26e9-8355-45cc-97c6-c31daf0df330|rider-A|19.1 |san_francisco|
|c8abbe79-8d89-47ea-b4ce-4d224bae5bfa|rider-J|17.85|chennai      |
|e96c4396-3fad-413a-a942-4cb36106d721|rider-C|31.4 |san_francisco|
|f1a2b3c4-0000-4000-8000-000000000001|rider-K|12.25|chennai      |
+------------------------------------+-------+-----+-------------+
```

The snapshots show the three delta commits. The upsert adds files and deletion vectors, so it is an `overwrite`;
the delete only adds a deletion vector, so it is a `delete`:

```
+-----------------------+-------------------+---------+-----------------+
|committed_at           |snapshot_id        |operation|hudi_instant     |
+-----------------------+-------------------+---------+-----------------+
|2026-11-20 09:53:49.528|6103377539092146917|append   |20261120095344049|
|2026-11-20 09:53:51.439|3502704673457355792|overwrite|20261120095349764|
|2026-11-20 09:53:52.878|1941006874835087320|delete   |20261120095351534|
+-----------------------+-------------------+---------+-----------------+
```

### Compaction

Compaction rewrites the data files that have deletion vectors. The Iceberg snapshot replaces those files and drops
their vectors.

```sql
CALL run_compaction(op => 'run', path => 'file:///tmp/hudi_iceberg/trips_mor',
  options => 'hoodie.compact.inline.max.delta.commits=1');
```

```
+-----------------+--------------+---------+
|timestamp        |operation_size|state    |
+-----------------+--------------+---------+
|20261120095355069|2             |COMPLETED|
+-----------------+--------------+---------+
```

`hoodie.compact.inline.max.delta.commits=1` lets Hudi schedule a compaction after a single delta commit, so it
runs on this small example. It compacts the two file groups with deletion vectors. After it, the `delete_files`
query above returns no rows, and the table still reads the same four rows.

:::note
Spark SQL `UPDATE`, `DELETE` and `MERGE INTO` on merge-on-read tables in this write mode are at an early stage.
Use the DataFrame writer or Hudi Streamer for merge-on-read tables for now.
:::

## Commit through an Iceberg catalog

By default the Iceberg metadata is path-based. To register the table in an Iceberg catalog, name the catalog in the
`hoodie.table.format.iceberg.*` settings when you create the table. Every Hudi commit then commits to the catalog,
and engines find the table by name. The Iceberg table name is the Hudi table name (`hoodie.table.name`), in the
namespace you set.

Locally, Iceberg's `JdbcCatalog` on a SQLite file works as a stand-in for a real catalog. Restart the shell with
the SQLite JDBC driver and a Spark catalog named `ice` that points at the same database:

```shell
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

`cache-enabled=false` matters: Hudi commits through its own catalog client, so a cached table in Spark's catalog
would go stale.

Create the namespace, then write a table whose settings name the same catalog:

<Tabs
groupId="programming-language"
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
]}
>
<TabItem value="scala">

```scala
import spark.implicits._

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
  "hoodie.table.format.iceberg.namespace" -> "db"
)

Seq(
  (1695159649087L, "334e26e9-8355-45cc-97c6-c31daf0df330", "rider-A", "driver-K", 19.10, "san_francisco"),
  (1695115999911L, "c8abbe79-8d89-47ea-b4ce-4d224bae5bfa", "rider-J", "driver-T", 17.85, "chennai")
).toDF("ts", "uuid", "rider", "driver", "fare", "city").
  write.format("hudi").options(catOptions).
  option("hoodie.datasource.write.operation", "upsert").
  mode("append").save(catPath)
```

</TabItem>
<TabItem value="python">

```python
spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.db")

cat_path = "file:///tmp/hudi_iceberg/trips_cat"
cat_options = {
    "hoodie.table.name": "trips_cat",
    "hoodie.datasource.write.recordkey.field": "uuid",
    "hoodie.datasource.write.partitionpath.field": "city",
    "hoodie.table.format": "ICEBERG",
    "hoodie.table.format.iceberg.catalog.name": "ice",
    "hoodie.table.format.iceberg.catalog.impl": "org.apache.iceberg.jdbc.JdbcCatalog",
    "hoodie.table.format.iceberg.catalog.option.uri": "jdbc:sqlite:/tmp/hudi_iceberg/catalog.db",
    "hoodie.table.format.iceberg.catalog.option.warehouse": "file:///tmp/hudi_iceberg/warehouse",
    "hoodie.table.format.iceberg.namespace": "db",
}

(spark.createDataFrame(
    [(1695159649087, "334e26e9-8355-45cc-97c6-c31daf0df330", "rider-A", "driver-K", 19.10, "san_francisco"),
     (1695115999911, "c8abbe79-8d89-47ea-b4ce-4d224bae5bfa", "rider-J", "driver-T", 17.85, "chennai")],
    ["ts", "uuid", "rider", "driver", "fare", "city"])
 .write.format("hudi").options(**cat_options)
 .option("hoodie.datasource.write.operation", "upsert")
 .mode("append").save(cat_path))
```

</TabItem>
</Tabs>

Now any Iceberg engine connected to the catalog finds `db.trips_cat`. In Spark:

```sql
SELECT uuid, rider, fare, city FROM ice.db.trips_cat;

SELECT snapshot_id, operation, committed_at FROM ice.db.trips_cat.snapshots;
```

```
+------------------------------------+-------+-----+-------------+
|uuid                                |rider  |fare |city         |
+------------------------------------+-------+-----+-------------+
|334e26e9-8355-45cc-97c6-c31daf0df330|rider-A|19.1 |san_francisco|
|c8abbe79-8d89-47ea-b4ce-4d224bae5bfa|rider-J|17.85|chennai      |
+------------------------------------+-------+-----+-------------+

+-------------------+---------+-----------------------+
|snapshot_id        |operation|committed_at           |
+-------------------+---------+-----------------------+
|5322157445761210656|append   |2026-11-20 09:53:49.613|
+-------------------+---------+-----------------------+
```

For time travel, use the DataFrame reader with a `snapshot_id` from the query above. SQL `VERSION AS OF` does
not work on Iceberg tables in a session that loads Hudi's SQL extension, because Hudi's parser takes over the
clause.

```scala
spark.read.option("snapshot-id", <snapshot_id>L).table("ice.db.trips_cat").
  select("uuid", "rider", "fare", "city").show(false)
```

The table's location in the catalog is the Hudi base path. The catalog's `warehouse` setting is required by the
catalog but does not hold this table. For AWS Glue, see the
[demo guide](pluggable-table-format-iceberg-aws-demo.md).

The catalog settings:

| Setting | Meaning |
|:--------|:--------|
| `hoodie.table.format.iceberg.catalog.name` | Catalog name. Defaults to `iceberg` when only the implementation is set. |
| `hoodie.table.format.iceberg.catalog.impl` | Iceberg catalog class, for example `org.apache.iceberg.aws.glue.GlueCatalog` or `org.apache.iceberg.jdbc.JdbcCatalog`. Required when a catalog is named. |
| `hoodie.table.format.iceberg.catalog.option.<key>` | Passed to the catalog as `<key>`, for example `warehouse` or `uri`. |
| `hoodie.table.format.iceberg.namespace` | Namespace of the table, dot separated, for example `db` or `a.b`. |

If a writer cannot set table settings, the plugin also reads the catalog from the Hadoop configuration, under
`xtable.iceberg.catalog.name`, `xtable.iceberg.catalog.impl` and `xtable.iceberg.catalog.option.<key>`. It does so
only when `hoodie.properties` names no catalog.

## Iceberg format version {#iceberg-format-version}

`hoodie.table.format.iceberg.format-version` sets the Iceberg format version of a new table:

- `3` is the default. Use it unless a reader you need cannot read version 3.
- `2` is for engines that read only version 2, for example Amazon Athena at the time of writing. Version 2
  works for copy-on-write tables only.
- Other values are refused. If the table config does not set it, the plugin falls back to
  `xtable.iceberg.format-version` in the Hadoop configuration (for example
  `--conf spark.hadoop.xtable.iceberg.format-version=2`), then to `3`.
- An existing Iceberg table keeps its version. Choose it when you create the table.

With format version 3 you get:

- **Row lineage** (see [below](#row-lineage)). Every row has a `_row_id` and a `_last_updated_sequence_number`. When Hudi rewrites a file,
  the rows it carries over unchanged keep both values. That holds for copy-on-write upserts, inserts that fill
  small files, compaction and clustering. A new or updated row gets a new row id: Hudi writes an update as a
  delete plus an insert, which the Iceberg spec allows.
- **Deletion vectors** for merge-on-read tables, one per data file.

### Row lineage {#row-lineage}

Read the lineage columns like any other column:

<Tabs
groupId="programming-language"
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
]}
>
<TabItem value="scala">

```scala
spark.read.format("iceberg").load(basePath).
  select("uuid", "fare", "_row_id", "_last_updated_sequence_number").orderBy("uuid").show(false)
```

</TabItem>
<TabItem value="python">

```python
spark.read.format("iceberg").load(base_path) \
    .select("uuid", "fare", "_row_id", "_last_updated_sequence_number").orderBy("uuid").show(truncate=False)
```

</TabItem>
</Tabs>

After the update, delete and merge in this guide, the rows that were never changed keep the ids and sequence
number 1 from the insert. `rider-D` (updated), `rider-C` (merged) and `rider-K` (inserted by the merge) have new ids
and the sequence number of the commit that wrote them:

```
+------------------------------------+-----+-------+-----------------------------+
|uuid                                |fare |_row_id|_last_updated_sequence_number|
+------------------------------------+-----+-------+-----------------------------+
|1dced545-862b-4ceb-8b43-d2a568f6616b|93.5 |3      |1                            |
|334e26e9-8355-45cc-97c6-c31daf0df330|19.1 |0      |1                            |
|3eeb61f7-c2b0-4636-99bd-5d7a5a1d2c04|41.06|4      |1                            |
|7a84095f-737f-40bc-b62f-6b69664712d2|43.4 |7      |1                            |
|9909a8b1-2d15-4d3d-8ec9-efc48c536a00|25.0 |10     |2                            |
|c8abbe79-8d89-47ea-b4ce-4d224bae5bfa|17.85|5      |1                            |
|e96c4396-3fad-413a-a942-4cb36106d721|31.4 |20     |4                            |
|f1a2b3c4-0000-4000-8000-000000000001|12.25|25     |4                            |
+------------------------------------+-----+-------+-----------------------------+
```

After the [rollback](#rollback) and [clustering](#clustering), every row has the same values as before the merge.
Clustering rewrote all three files and kept every row id. The rollback restored `rider-C`'s old id, but gave new
ids to three rows whose files it restored (`rider-D`, `rider-I` and `rider-J`):

```
+------------------------------------+-----+-------+-----------------------------+
|uuid                                |fare |_row_id|_last_updated_sequence_number|
+------------------------------------+-----+-------+-----------------------------+
|1dced545-862b-4ceb-8b43-d2a568f6616b|93.5 |3      |1                            |
|334e26e9-8355-45cc-97c6-c31daf0df330|19.1 |0      |1                            |
|3eeb61f7-c2b0-4636-99bd-5d7a5a1d2c04|41.06|30     |5                            |
|7a84095f-737f-40bc-b62f-6b69664712d2|43.4 |7      |1                            |
|9909a8b1-2d15-4d3d-8ec9-efc48c536a00|25.0 |28     |5                            |
|c8abbe79-8d89-47ea-b4ce-4d224bae5bfa|17.85|31     |5                            |
|e96c4396-3fad-413a-a942-4cb36106d721|27.7 |1      |1                            |
+------------------------------------+-----+-------+-----------------------------+
```

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
  compaction is not available.
- **Rollback can give restored rows new row ids.**
- **SQL `VERSION AS OF` fails on Iceberg tables** in a session that loads Hudi's SQL extension. Use the DataFrame
  reader with the `snapshot-id` option instead.
- **Arrays with null elements fail to insert** with Hudi's default Parquet list layout. Setting
  `spark.hadoop.parquet.avro.write-old-list-structure=false` writes the standard layout, which both Hudi and
  Iceberg read.
- **In merge-on-read `MERGE INTO`, a `WHEN MATCHED` condition cannot reference target columns** (for example
  `WHEN MATCHED AND target.fare > 10`). Conditions on source columns work.
- **Time travel** reaches back only as far as cleaning and archival allow, and not to a rolled-back commit.
- **Every Hudi reader and writer of the table needs the plugin jars.** Hudi refuses to load a table whose table
  format plugin is missing. Iceberg readers do not need them.
- **Iceberg readers see Hudi's meta columns** (`_hoodie_commit_time`, `_hoodie_commit_seqno`, `_hoodie_record_key`,
  `_hoodie_partition_path`, `_hoodie_file_name`).
- **Hudi's catalog sync (Hive, Glue) does not know about the table format.** To register the table, use the Iceberg
  catalog settings above.
- **Unpartitioned merge-on-read tables** scan the whole Iceberg table on each delta commit to find the current
  deletion vectors. Partitioned tables scan only the partitions a commit touches.

## Next steps

- Run the same table on AWS: [Hudi writes Iceberg on AWS](pluggable-table-format-iceberg-aws-demo.md).
- Hudi's own features on this table: [table services](compaction.md), [indexes](indexes.md),
  [write operations](write_operations.md).
- For translating existing tables between formats after the fact, see [Apache XTable](syncing_xtable.md).
