/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.hudi.dml.others

import org.apache.hudi.HoodieCLIUtils
import org.apache.hudi.common.model.{FileSlice, HoodieLogFile}
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.log.HoodieLogFileReader
import org.apache.hudi.common.table.log.block.HoodieLogBlock.{HeaderMetadataType, HoodieLogBlockType}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.testutils.DataSourceTestUtils

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase.getMetaClientAndFileSystemView

import java.util.Collections

import scala.collection.JavaConverters._

/**
 * Creates 4 MOR tables with specific file slice layouts, validates each layout
 * via the same HoodieTableFileSystemView + getLatestMergedFileSlicesBeforeOrOn
 * code path that SELECT uses, then saves gold data alongside each table.
 */
class TestMORFileSliceLayouts extends HoodieSparkSqlTestBase {

  val BASE_OUTPUT_PATH = "/home/ubuntu/ws3/hudi-rs/tables/fdss"

  private def cleanPath(path: String): Unit = {
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(spark.sessionState.newHadoopConf())
    if (fs.exists(hadoopPath)) {
      fs.delete(hadoopPath, true)
    }
  }

  // ─── Core validation helper ───
  // Returns the file slices exactly as the SELECT query path would see them:
  //   HoodieTableFileSystemView → getLatestMergedFileSlicesBeforeOrOn(partition, queryInstant)
  // This mirrors BaseHoodieTableFileIndex.filterFiles (line 342-355).
  private def getQueryFileSlices(basePath: String): Seq[FileSlice] = {
    val (metaClient, fsView) = getMetaClientAndFileSystemView(basePath)
    val queryInstant = metaClient.getActiveTimeline
      .filterCompletedInstants().lastInstant().get().requestedTime()
    fsView.getLatestMergedFileSlicesBeforeOrOn("", queryInstant)
      .iterator().asScala.toSeq
  }

  // ─── Validate file slice properties ───
  private def assertFileSliceLayout(
      basePath: String,
      expectedFileGroups: Int,
      expectBaseFile: Boolean,
      expectLogFiles: Boolean,
      expectedBlockTypes: Set[HoodieLogBlockType] = Set.empty,
      expectCompactedBlock: Boolean = false,
      keyField: String = "key"
  ): Unit = {
    val slices = getQueryFileSlices(basePath)
    println(s"  Query file slices: ${slices.size}")
    slices.foreach { fs =>
      println(s"    FileSlice(fileGroupId=${fs.getFileGroupId}, " +
        s"baseInstant=${fs.getBaseInstantTime}, " +
        s"hasBase=${fs.getBaseFile.isPresent}, " +
        s"logFiles=${fs.getLogFiles.count()})")
    }

    // Assert file group count
    assert(slices.size == expectedFileGroups,
      s"Expected $expectedFileGroups file group(s), got ${slices.size}")

    // For each file slice, assert base file and log file expectations
    slices.foreach { fs =>
      if (expectBaseFile) {
        assert(fs.getBaseFile.isPresent,
          s"File group ${fs.getFileGroupId} should have a base file")
      } else {
        assert(!fs.getBaseFile.isPresent,
          s"File group ${fs.getFileGroupId} should NOT have a base file (log-only)")
      }

      if (expectLogFiles) {
        assert(fs.getLogFiles.count() > 0,
          s"File group ${fs.getFileGroupId} should have log files")
      }
    }

    // Assert log block types and compacted block across all file slices
    if (expectedBlockTypes.nonEmpty || expectCompactedBlock) {
      val (metaClient, _) = getMetaClientAndFileSystemView(basePath)
      val schema = new TableSchemaResolver(metaClient).getTableSchema
      val allBlockTypes = scala.collection.mutable.ArrayBuffer[HoodieLogBlockType]()
      var foundCompactedBlock = false

      slices.foreach { fs =>
        val logFilePathList: java.util.List[String] =
          HoodieTestUtils.getLogFileListFromFileSlice(fs)
        Collections.sort(logFilePathList)
        for (i <- 0 until logFilePathList.size()) {
          val reader = new HoodieLogFileReader(
            metaClient.getStorage, new HoodieLogFile(logFilePathList.get(i)),
            schema, 1024 * 1024, false, false, keyField, null)
          while (reader.hasNext) {
            val block = reader.next()
            allBlockTypes += block.getBlockType()
            if (block.getLogBlockHeader.containsKey(HeaderMetadataType.COMPACTED_BLOCK_TIMES)) {
              foundCompactedBlock = true
            }
          }
          reader.close()
        }
      }

      println(s"  Log block types found: $allBlockTypes")

      expectedBlockTypes.foreach { bt =>
        assert(allBlockTypes.contains(bt),
          s"Expected log block type $bt not found. Found: $allBlockTypes")
      }

      if (expectCompactedBlock) {
        assert(foundCompactedBlock,
          s"Expected compacted log block (COMPACTED_BLOCK_TIMES header) not found")
      }
    }
  }

  // ─── Validate table properties ───
  private def assertTableProperties(basePath: String): Unit = {
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(HoodieTestUtils.getDefaultStorageConf)
      .setBasePath(basePath).build()
    val tableConfig = metaClient.getTableConfig
    assert(tableConfig.getTableVersion.versionCode() == 9,
      s"Expected table version 9, got ${tableConfig.getTableVersion.versionCode()}")
    assert(tableConfig.getTableType.toString == "MERGE_ON_READ",
      s"Expected MERGE_ON_READ, got ${tableConfig.getTableType}")
  }

  private def saveGoldData(tableName: String, tablePath: String): Unit = {
    val goldPath = s"$tablePath/gold_data"
    val df = spark.sql(s"SELECT * FROM $tableName ORDER BY key")
    df.coalesce(1).write.mode(SaveMode.Overwrite).parquet(goldPath)
    println(s"  Gold data saved to $goldPath (${df.count()} records)")
  }

  // ─── Full schema CREATE TABLE SQL ───
  private def fullSchemaCreateSQL(tableName: String, tablePath: String): String = {
    s"""
       | CREATE TABLE $tableName (
       |   key STRING,
       |   ts LONG,
       |   level STRING,
       |   severity INT,
       |   double_field DOUBLE,
       |   float_field FLOAT,
       |   int_field INT,
       |   long_field LONG,
       |   boolean_field BOOLEAN,
       |   string_field STRING,
       |   bytes_field BINARY,
       |   decimal_field DECIMAL(20,2),
       |   nested_record STRUCT<nested_int: INT, level: STRING>,
       |   nullable_map_field MAP<STRING, STRUCT<nested_int: INT, level: STRING>>,
       |   array_field ARRAY<STRUCT<nested_int: INT, level: STRING>>,
       |   enum_field STRING,
       |   date_nullable_field DATE,
       |   timestamp_millis_nullable_field TIMESTAMP,
       |   timestamp_micros_nullable_field TIMESTAMP,
       |   timestamp_local_millis_nullable_field TIMESTAMP,
       |   timestamp_local_micros_nullable_field TIMESTAMP,
       |   partition STRING,
       |   round INT
       | ) USING hudi
       | TBLPROPERTIES (
       |   type = 'mor',
       |   primaryKey = 'key',
       |   hoodie.record.merge.mode = 'COMMIT_TIME_ORDERING',
       |   hoodie.write.table.version = '9'
       | )
       | LOCATION '$tablePath'
     """.stripMargin
  }

  private def fullSchemaInsertSQL(key: String, round: Int, suffix: Int): String = {
    val month = Math.min(suffix, 9)
    val sec = suffix % 10
    s"""
       | SELECT
       |   '$key' as key,
       |   ${100 + suffix} as ts,
       |   'INFO_$suffix' as level,
       |   $suffix as severity,
       |   ${suffix}.${suffix} as double_field,
       |   CAST(${suffix}.${suffix + 1} AS FLOAT) as float_field,
       |   ${suffix * 10} as int_field,
       |   ${suffix * 100L} as long_field,
       |   ${suffix % 2 == 0} as boolean_field,
       |   'str_$suffix' as string_field,
       |   CAST('bytes_$suffix' AS BINARY) as bytes_field,
       |   CAST(${suffix * 100}.$suffix AS DECIMAL(20,2)) as decimal_field,
       |   named_struct('nested_int', $suffix, 'level', 'NL_$suffix') as nested_record,
       |   map('mk_$suffix', named_struct('nested_int', $suffix, 'level', 'ML_$suffix')) as nullable_map_field,
       |   array(named_struct('nested_int', $suffix, 'level', 'AL_$suffix')) as array_field,
       |   'ENUM_$suffix' as enum_field,
       |   DATE '2024-0$month-01' as date_nullable_field,
       |   TIMESTAMP '2024-0$month-01 00:00:0$sec' as timestamp_millis_nullable_field,
       |   TIMESTAMP '2024-0$month-01 00:00:0$sec' as timestamp_micros_nullable_field,
       |   TIMESTAMP '2024-0$month-01 00:00:0$sec' as timestamp_local_millis_nullable_field,
       |   TIMESTAMP '2024-0$month-01 00:00:0$sec' as timestamp_local_micros_nullable_field,
       |   '' as partition,
       |   $round as round
     """.stripMargin
  }

  private def fullSchemaUpdateSQL(tableName: String, round: Int, whereClause: String): String = {
    s"""
       | UPDATE $tableName SET
       |   ts = ${200 * round},
       |   level = 'LVL_R$round',
       |   severity = ${20 * round},
       |   double_field = ${99.9 * round},
       |   float_field = CAST(${88.8 * round} AS FLOAT),
       |   int_field = ${200 * round},
       |   long_field = ${2000L * round},
       |   boolean_field = ${round % 2 == 0},
       |   string_field = 'updated_r$round',
       |   bytes_field = CAST('upd_bytes_r$round' AS BINARY),
       |   decimal_field = CAST(${999 + round}.99 AS DECIMAL(20,2)),
       |   nested_record = named_struct('nested_int', ${20 * round}, 'level', 'NL_R$round'),
       |   nullable_map_field = map('mk_r$round', named_struct('nested_int', ${20 * round}, 'level', 'ML_R$round')),
       |   array_field = array(named_struct('nested_int', ${20 * round}, 'level', 'AL_R$round')),
       |   enum_field = 'ENUM_R$round',
       |   date_nullable_field = DATE '2025-0${Math.min(round, 9)}-15',
       |   timestamp_millis_nullable_field = TIMESTAMP '2025-0${Math.min(round, 9)}-15 12:00:00',
       |   timestamp_micros_nullable_field = TIMESTAMP '2025-0${Math.min(round, 9)}-15 12:00:00',
       |   timestamp_local_millis_nullable_field = TIMESTAMP '2025-0${Math.min(round, 9)}-15 12:00:00',
       |   timestamp_local_micros_nullable_field = TIMESTAMP '2025-0${Math.min(round, 9)}-15 12:00:00',
       |   round = $round
       | WHERE $whereClause
     """.stripMargin
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Test 1: File slice with compacted log block
  //
  // Requirements:
  //   - v9 MOR table
  //   - Exactly 1 file group
  //   - File slice has NO base file (log-only via INMEMORY index)
  //   - File slice contains a compacted log block (COMPACTED_BLOCK_TIMES header)
  //   - SELECT reads through this file slice
  // ═══════════════════════════════════════════════════════════════════════════
  test("1. File slice with compacted log block") {
    val tablePath = s"$BASE_OUTPUT_PATH/table_log_compaction_${System.currentTimeMillis()}"
    cleanPath(tablePath)

    withSparkSqlSessionConfigWithCondition(
      ("hoodie.compact.inline" -> "false", true),
      ("hoodie.compact.schedule.inline" -> "false", true)
    ) {
      withRecordType(Seq(HoodieRecordType.AVRO))({
        val tableName = generateTableName
        println(s"=== Test 1: compacted log block === path=$tablePath")

        spark.sql(
          s"""
             | CREATE TABLE $tableName (
             |   key STRING, ts LONG, level STRING, severity INT,
             |   partition STRING, round INT
             | ) USING hudi
             | TBLPROPERTIES (
             |   type = 'mor', primaryKey = 'key',
             |   hoodie.record.merge.mode = 'COMMIT_TIME_ORDERING',
             |   hoodie.index.type = 'INMEMORY',
             |   hoodie.write.table.version = '9'
             | )
             | LOCATION '$tablePath'
           """.stripMargin)

        // Insert + updates to create multiple log blocks
        spark.sql(
          s"""INSERT INTO $tableName
             | SELECT 'k1', 100, 'INFO', 1, '', 1 UNION ALL
             | SELECT 'k2', 100, 'WARN', 2, '', 1 UNION ALL
             | SELECT 'k3', 100, 'ERROR', 3, '', 1""".stripMargin)
        spark.sql(s"UPDATE $tableName SET level='U1', round=2 WHERE key='k1'")
        spark.sql(s"UPDATE $tableName SET level='U2', round=3 WHERE key='k2'")
        spark.sql(s"UPDATE $tableName SET severity=99, round=4 WHERE key='k3'")

        // Trigger log compaction via write client API (no SQL procedure exists)
        val client = HoodieCLIUtils.createHoodieWriteClient(spark, tablePath,
          Map("hoodie.log.compaction.enable" -> "true",
              "hoodie.log.compaction.blocks.threshold" -> "1"),
          Option(tableName))
        try {
          val lcTime = client.scheduleLogCompaction(HOption.empty())
          assert(lcTime.isPresent, "Log compaction should be scheduled")
          val result = client.logCompact(lcTime.get())
          client.commitLogCompaction(lcTime.get(), result, HOption.empty())
          println(s"  Log compaction committed at: ${lcTime.get()}")
        } finally {
          client.close()
        }

        // ─── Validate ───
        assertTableProperties(tablePath)
        assertFileSliceLayout(tablePath,
          expectedFileGroups = 1,
          expectBaseFile = false,          // log-only (INMEMORY index)
          expectLogFiles = true,
          expectCompactedBlock = true      // must have COMPACTED_BLOCK_TIMES header
        )

        checkAnswer(s"SELECT key, level, severity, round FROM $tableName ORDER BY key")(
          Seq("k1", "U1", 1, 2),
          Seq("k2", "U2", 2, 3),
          Seq("k3", "ERROR", 99, 4)
        )
        saveGoldData(tableName, tablePath)
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
      })
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Test 2: File slice with log file only
  //
  // Requirements:
  //   - v9 MOR table
  //   - Exactly 1 file group
  //   - File slice has NO base file (log-only)
  //   - File slice contains data blocks AND delete blocks
  //   - SELECT reads through this file slice
  // ═══════════════════════════════════════════════════════════════════════════
  test("2. File slice with log file only") {
    val tablePath = s"$BASE_OUTPUT_PATH/table_log_only_${System.currentTimeMillis()}"
    cleanPath(tablePath)

    withSparkSqlSessionConfigWithCondition(
      ("hoodie.compact.inline" -> "false", true),
      ("hoodie.compact.schedule.inline" -> "false", true)
    ) {
      withRecordType(Seq(HoodieRecordType.AVRO))({
        val tableName = generateTableName
        println(s"=== Test 2: log file only === path=$tablePath")

        spark.sql(
          s"""
             | CREATE TABLE $tableName (
             |   key STRING, ts LONG, level STRING, severity INT,
             |   partition STRING, round INT
             | ) USING hudi
             | TBLPROPERTIES (
             |   type = 'mor', primaryKey = 'key',
             |   hoodie.record.merge.mode = 'COMMIT_TIME_ORDERING',
             |   hoodie.index.type = 'INMEMORY',
             |   hoodie.write.table.version = '9'
             | )
             | LOCATION '$tablePath'
           """.stripMargin)

        spark.sql(
          s"""INSERT INTO $tableName
             | SELECT 'k1', 100, 'INFO', 1, '', 1 UNION ALL
             | SELECT 'k2', 100, 'WARN', 2, '', 1 UNION ALL
             | SELECT 'k3', 100, 'ERROR', 3, '', 1""".stripMargin)
        spark.sql(s"UPDATE $tableName SET level='UPD', round=2 WHERE key='k1'")
        spark.sql(s"DELETE FROM $tableName WHERE key='k3'")

        // ─── Validate ───
        assertTableProperties(tablePath)
        assert(DataSourceTestUtils.isLogFileOnly(tablePath),
          "No base files should exist on disk (log-only)")
        assertFileSliceLayout(tablePath,
          expectedFileGroups = 1,
          expectBaseFile = false,
          expectLogFiles = true,
          expectedBlockTypes = Set(HoodieLogBlockType.DELETE_BLOCK)
              // data blocks can be AVRO_DATA_BLOCK or PARQUET_DATA_BLOCK depending on record type
        )

        checkAnswer(s"SELECT key, level, severity, round FROM $tableName ORDER BY key")(
          Seq("k1", "UPD", 1, 2),
          Seq("k2", "WARN", 2, 1)
        )
        saveGoldData(tableName, tablePath)
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
      })
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Test 3: Column projection
  //
  // Requirements:
  //   - v9 MOR table
  //   - Exactly 1 file group
  //   - File slice has base file + log files (data block + delete block)
  //   - SELECT with column subsets reads through this file slice correctly
  // ═══════════════════════════════════════════════════════════════════════════
  test("3. Column projection") {
    val tablePath = s"$BASE_OUTPUT_PATH/table_column_projection_${System.currentTimeMillis()}"
    cleanPath(tablePath)

    withSparkSqlSessionConfigWithCondition(
      ("hoodie.compact.inline" -> "false", true),
      ("hoodie.compact.schedule.inline" -> "false", true)
    ) {
      withRecordType(Seq(HoodieRecordType.AVRO))({
        val tableName = generateTableName
        println(s"=== Test 3: column projection === path=$tablePath")

        spark.sql(fullSchemaCreateSQL(tableName, tablePath))

        // Insert 3 records
        spark.sql(
          s"""INSERT INTO $tableName
             | ${fullSchemaInsertSQL("k1", 1, 1)} UNION ALL
             | ${fullSchemaInsertSQL("k2", 1, 2)} UNION ALL
             | ${fullSchemaInsertSQL("k3", 1, 3)}""".stripMargin)

        // Update all columns for k1
        spark.sql(fullSchemaUpdateSQL(tableName, 2, "key = 'k1'"))

        // Delete k3
        spark.sql(s"DELETE FROM $tableName WHERE key='k3'")

        // ─── Validate layout ───
        assertTableProperties(tablePath)
        assertFileSliceLayout(tablePath,
          expectedFileGroups = 1,
          expectBaseFile = true,
          expectLogFiles = true,
          expectedBlockTypes = Set(HoodieLogBlockType.DELETE_BLOCK)
        )

        // ─── Validate column projections ───
        // k1 updated (round=2), k2 original (suffix=2, round=1), k3 deleted

        // Projection 1: Primitive scalar columns
        checkAnswer(
          s"SELECT key, ts, int_field, boolean_field, string_field FROM $tableName ORDER BY key")(
          Seq("k1", 400L, 400, true, "updated_r2"),
          Seq("k2", 102L, 20, true, "str_2")
        )

        // Projection 2: Nested struct columns
        checkAnswer(
          s"SELECT key, nested_record.nested_int, nested_record.level FROM $tableName ORDER BY key")(
          Seq("k1", 40, "NL_R2"),
          Seq("k2", 2, "NL_2")
        )

        // Projection 3: Temporal columns (cast to string)
        checkAnswer(
          s"""SELECT key, CAST(date_nullable_field AS STRING),
             |       CAST(timestamp_millis_nullable_field AS STRING)
             |FROM $tableName ORDER BY key""".stripMargin)(
          Seq("k1", "2025-02-15", "2025-02-15 12:00:00"),
          Seq("k2", "2024-02-01", "2024-02-01 00:00:02")
        )

        // Projection 4: Decimal + enum
        checkAnswer(
          s"SELECT key, CAST(decimal_field AS STRING), enum_field FROM $tableName ORDER BY key")(
          Seq("k1", "1001.99", "ENUM_R2"),
          Seq("k2", "200.20", "ENUM_2")
        )

        saveGoldData(tableName, tablePath)
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
      })
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Test 4: All data types with comprehensive log blocks
  //
  // Requirements:
  //   - v9 MOR table
  //   - Exactly 1 file group
  //   - File slice has base file + log files
  //   - Log blocks contain update for EVERY data type column
  //   - Log blocks contain delete block
  //   - No new-key insert in a separate commit (would create 2nd file group)
  // ═══════════════════════════════════════════════════════════════════════════
  test("4. All data types") {
    val tablePath = s"$BASE_OUTPUT_PATH/table_all_data_types_${System.currentTimeMillis()}"
    cleanPath(tablePath)

    withSparkSqlSessionConfigWithCondition(
      ("hoodie.compact.inline" -> "false", true),
      ("hoodie.compact.schedule.inline" -> "false", true)
    ) {
      withRecordType(Seq(HoodieRecordType.AVRO))({
        val tableName = generateTableName
        println(s"=== Test 4: all data types === path=$tablePath")

        spark.sql(fullSchemaCreateSQL(tableName, tablePath))

        // ── Round 1: Insert 5 records with all data types ──
        spark.sql(
          s"""INSERT INTO $tableName
             | ${fullSchemaInsertSQL("k1", 1, 1)} UNION ALL
             | ${fullSchemaInsertSQL("k2", 1, 2)} UNION ALL
             | ${fullSchemaInsertSQL("k3", 1, 3)} UNION ALL
             | ${fullSchemaInsertSQL("k4", 1, 4)} UNION ALL
             | ${fullSchemaInsertSQL("k5", 1, 5)}""".stripMargin)

        // ── Round 2: Update ALL columns for k1, k2, k3 ──
        // Single UPDATE modifying every data type column → one data log block covering all types
        spark.sql(fullSchemaUpdateSQL(tableName, 2, "key IN ('k1', 'k2', 'k3')"))

        // ── Round 3: Delete k4 ──
        spark.sql(s"DELETE FROM $tableName WHERE key = 'k4'")

        // ── Round 4: Update k5 to exercise another data block (instead of inserting new key) ──
        // This avoids creating a 2nd file group which INSERT of a new key would cause
        spark.sql(fullSchemaUpdateSQL(tableName, 4, "key = 'k5'"))

        // ─── Validate layout ───
        assertTableProperties(tablePath)
        assertFileSliceLayout(tablePath,
          expectedFileGroups = 1,
          expectBaseFile = true,
          expectLogFiles = true,
          expectedBlockTypes = Set(HoodieLogBlockType.DELETE_BLOCK)
        )

        // ─── Full data validation across all data types ───
        // Expected: k1/k2/k3 updated at round=2, k5 updated at round=4, k4 deleted

        // Scalar fields: int, long, boolean, string
        checkAnswer(
          s"""SELECT key, ts, level, severity, int_field, long_field, boolean_field,
             |       string_field, enum_field, partition, round
             |FROM $tableName ORDER BY key""".stripMargin)(
          Seq("k1", 400L, "LVL_R2", 40, 400, 4000L, true, "updated_r2", "ENUM_R2", "", 2),
          Seq("k2", 400L, "LVL_R2", 40, 400, 4000L, true, "updated_r2", "ENUM_R2", "", 2),
          Seq("k3", 400L, "LVL_R2", 40, 400, 4000L, true, "updated_r2", "ENUM_R2", "", 2),
          Seq("k5", 800L, "LVL_R4", 80, 800, 8000L, true, "updated_r4", "ENUM_R4", "", 4)
        )

        // Double, float, decimal
        checkAnswer(
          s"""SELECT key, double_field, float_field, CAST(decimal_field AS STRING)
             |FROM $tableName ORDER BY key""".stripMargin)(
          Seq("k1", 199.8, 177.6f, "1001.99"),
          Seq("k2", 199.8, 177.6f, "1001.99"),
          Seq("k3", 199.8, 177.6f, "1001.99"),
          Seq("k5", 399.6, 355.2f, "1003.99")
        )

        // Binary (cast to string)
        checkAnswer(
          s"""SELECT key, CAST(bytes_field AS STRING)
             |FROM $tableName ORDER BY key""".stripMargin)(
          Seq("k1", "upd_bytes_r2"),
          Seq("k2", "upd_bytes_r2"),
          Seq("k3", "upd_bytes_r2"),
          Seq("k5", "upd_bytes_r4")
        )

        // Nested struct (flattened via dot notation)
        checkAnswer(
          s"""SELECT key, nested_record.nested_int, nested_record.level
             |FROM $tableName ORDER BY key""".stripMargin)(
          Seq("k1", 40, "NL_R2"),
          Seq("k2", 40, "NL_R2"),
          Seq("k3", 40, "NL_R2"),
          Seq("k5", 80, "NL_R4")
        )

        // Map field (flattened via map_keys/map_values)
        checkAnswer(
          s"""SELECT key, map_keys(nullable_map_field)[0],
             |       map_values(nullable_map_field)[0].nested_int,
             |       map_values(nullable_map_field)[0].level
             |FROM $tableName ORDER BY key""".stripMargin)(
          Seq("k1", "mk_r2", 40, "ML_R2"),
          Seq("k2", "mk_r2", 40, "ML_R2"),
          Seq("k3", "mk_r2", 40, "ML_R2"),
          Seq("k5", "mk_r4", 80, "ML_R4")
        )

        // Array field (flattened via index)
        checkAnswer(
          s"""SELECT key, array_field[0].nested_int, array_field[0].level
             |FROM $tableName ORDER BY key""".stripMargin)(
          Seq("k1", 40, "AL_R2"),
          Seq("k2", 40, "AL_R2"),
          Seq("k3", 40, "AL_R2"),
          Seq("k5", 80, "AL_R4")
        )

        // Temporal fields (cast to string)
        checkAnswer(
          s"""SELECT key, CAST(date_nullable_field AS STRING),
             |       CAST(timestamp_millis_nullable_field AS STRING),
             |       CAST(timestamp_micros_nullable_field AS STRING),
             |       CAST(timestamp_local_millis_nullable_field AS STRING),
             |       CAST(timestamp_local_micros_nullable_field AS STRING)
             |FROM $tableName ORDER BY key""".stripMargin)(
          Seq("k1", "2025-02-15", "2025-02-15 12:00:00", "2025-02-15 12:00:00", "2025-02-15 12:00:00", "2025-02-15 12:00:00"),
          Seq("k2", "2025-02-15", "2025-02-15 12:00:00", "2025-02-15 12:00:00", "2025-02-15 12:00:00", "2025-02-15 12:00:00"),
          Seq("k3", "2025-02-15", "2025-02-15 12:00:00", "2025-02-15 12:00:00", "2025-02-15 12:00:00", "2025-02-15 12:00:00"),
          Seq("k5", "2025-04-15", "2025-04-15 12:00:00", "2025-04-15 12:00:00", "2025-04-15 12:00:00", "2025-04-15 12:00:00")
        )

        saveGoldData(tableName, tablePath)
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
      })
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Test 5: Fixed-width primitive elements in ARRAY and MAP columns
  //
  // Validates UnsafeRow 8-byte alignment for collection types whose element
  // size is NOT a multiple of 8 (INT=4, FLOAT=4).  Types that are already
  // 8-byte aligned (BIGINT, DOUBLE) are included as controls.
  //
  // Each row has an ODD number of elements (1 or 3) so that
  // count * elementBytes is NOT 8-byte aligned for the affected types.
  //
  // Note: MAP value types are limited to INT and STRING by hudi-rs's
  // build_map_array (BOOLEAN, TINYINT, SMALLINT, FLOAT, DATE etc. are not
  // yet supported there).  ARRAY elements go through a different path and
  // support all primitive types.
  //
  // Requirements:
  //   - v9 MOR table, 1 file group
  //   - Base file from initial INSERT
  //   - Log files from UPDATE (exercises merge-on-read with these types)
  //   - SELECT reads through the merged file slice
  // ═══════════════════════════════════════════════════════════════════════════
  test("5. Fixed-width primitives in ARRAY and MAP columns") {
    val tablePath = s"$BASE_OUTPUT_PATH/table_fixedwidth_collections_${System.currentTimeMillis()}"
    cleanPath(tablePath)

    withSparkSqlSessionConfigWithCondition(
      ("hoodie.compact.inline" -> "false", true),
      ("hoodie.compact.schedule.inline" -> "false", true)
    ) {
      withRecordType(Seq(HoodieRecordType.AVRO))({
        val tableName = generateTableName
        println(s"=== Test 5: fixed-width primitives in collections === path=$tablePath")

        spark.sql(
          s"""
             | CREATE TABLE $tableName (
             |   key STRING,
             |   ts LONG,
             |   -- MAP with fixed-width primitive values (4-byte → misaligns on odd count)
             |   map_int     MAP<STRING, INT>,
             |   -- ARRAY with sub-8-byte fixed-width elements (triggers alignment bug)
             |   arr_bool    ARRAY<BOOLEAN>,
             |   arr_tinyint ARRAY<TINYINT>,
             |   arr_small   ARRAY<SMALLINT>,
             |   arr_int     ARRAY<INT>,
             |   arr_float   ARRAY<FLOAT>,
             |   -- ARRAY with 8-byte-aligned elements (controls)
             |   arr_bigint  ARRAY<BIGINT>,
             |   arr_double  ARRAY<DOUBLE>,
             |   partition STRING
             | ) USING hudi
             | TBLPROPERTIES (
             |   type = 'mor',
             |   primaryKey = 'key',
             |   hoodie.record.merge.mode = 'COMMIT_TIME_ORDERING',
             |   hoodie.write.table.version = '9'
             | )
             | LOCATION '$tablePath'
           """.stripMargin)

        // ── Round 1: INSERT 3 records with 1 element per collection (odd count) ──
        spark.sql(
          s"""INSERT INTO $tableName SELECT
             |  'k1' as key, 1 as ts,
             |  map('a', 100) as map_int,
             |  array(true) as arr_bool,
             |  array(cast(1 as tinyint)) as arr_tinyint,
             |  array(cast(10 as smallint)) as arr_small,
             |  array(100) as arr_int,
             |  array(cast(1.5 as float)) as arr_float,
             |  array(1000L) as arr_bigint,
             |  array(1.5D) as arr_double,
             |  '' as partition
             |UNION ALL SELECT
             |  'k2', 2,
             |  map('a', 200),
             |  array(false),
             |  array(cast(2 as tinyint)),
             |  array(cast(20 as smallint)),
             |  array(200),
             |  array(cast(2.5 as float)),
             |  array(2000L),
             |  array(2.5D),
             |  ''
             |UNION ALL SELECT
             |  'k3', 3,
             |  map('a', 300),
             |  array(true),
             |  array(cast(3 as tinyint)),
             |  array(cast(30 as smallint)),
             |  array(300),
             |  array(cast(3.5 as float)),
             |  array(3000L),
             |  array(3.5D),
             |  ''
           """.stripMargin)

        // Verify base insert
        assert(spark.sql(s"SELECT * FROM $tableName").count() == 3)

        // ── Round 2: UPDATE k1 and k2 — creates log files with fixed-width
        //    collection data, exercising the MOR merge path ──
        // Use 3 elements per collection (still odd → misalignment for sub-8-byte types)
        spark.sql(
          s"""UPDATE $tableName SET
             |  ts = 10,
             |  map_int     = map('x', 1100, 'y', 1200, 'z', 1300),
             |  arr_bool    = array(false, true, false),
             |  arr_tinyint = array(cast(11 as tinyint), cast(12 as tinyint), cast(13 as tinyint)),
             |  arr_small   = array(cast(110 as smallint), cast(120 as smallint), cast(130 as smallint)),
             |  arr_int     = array(1100, 1200, 1300),
             |  arr_float   = array(cast(11.5 as float), cast(12.5 as float), cast(13.5 as float)),
             |  arr_bigint  = array(11000L, 12000L, 13000L),
             |  arr_double  = array(11.5D, 12.5D, 13.5D)
             |WHERE key IN ('k1', 'k2')
           """.stripMargin)

        // ── Validate layout: 1 file group, base + log files ──
        assertTableProperties(tablePath)
        assertFileSliceLayout(tablePath,
          expectedFileGroups = 1,
          expectBaseFile = true,
          expectLogFiles = true,
          expectedBlockTypes = Set.empty
        )

        // ── Validate MAP columns ──
        // k1 and k2 updated (3 entries), k3 unchanged (1 entry)
        checkAnswer(
          s"""SELECT key,
             |  map_int['x'], map_int['y'], map_int['z']
             |FROM $tableName WHERE key = 'k1'""".stripMargin)(
          Seq("k1", 1100, 1200, 1300)
        )

        // k3 should be unchanged (original 1-entry maps)
        checkAnswer(
          s"""SELECT key, map_int['a']
             |FROM $tableName WHERE key = 'k3'""".stripMargin)(
          Seq("k3", 300)
        )

        // ── Validate ARRAY columns ──
        checkAnswer(
          s"""SELECT key,
             |  arr_bool[0], arr_bool[1], arr_bool[2],
             |  arr_tinyint[0], arr_tinyint[1], arr_tinyint[2],
             |  arr_small[0], arr_small[1], arr_small[2],
             |  arr_int[0], arr_int[1], arr_int[2],
             |  arr_float[0], arr_float[1], arr_float[2],
             |  arr_bigint[0], arr_bigint[1], arr_bigint[2],
             |  arr_double[0], arr_double[1], arr_double[2]
             |FROM $tableName WHERE key = 'k1'""".stripMargin)(
          Seq("k1",
            false, true, false,
            11.toByte, 12.toByte, 13.toByte,
            110.toShort, 120.toShort, 130.toShort,
            1100, 1200, 1300,
            11.5f, 12.5f, 13.5f,
            11000L, 12000L, 13000L,
            11.5D, 12.5D, 13.5D)
        )

        // k3 arrays should be unchanged (1 element each)
        checkAnswer(
          s"""SELECT key,
             |  arr_bool[0],
             |  arr_tinyint[0],
             |  arr_small[0],
             |  arr_int[0],
             |  arr_float[0],
             |  arr_bigint[0],
             |  arr_double[0]
             |FROM $tableName WHERE key = 'k3'""".stripMargin)(
          Seq("k3", true, 3.toByte, 30.toShort, 300, 3.5f, 3000L, 3.5D)
        )

        // ── Full row count ──
        assert(spark.sql(s"SELECT * FROM $tableName").count() == 3)

        saveGoldData(tableName, tablePath)
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
      })
    }
  }
}
