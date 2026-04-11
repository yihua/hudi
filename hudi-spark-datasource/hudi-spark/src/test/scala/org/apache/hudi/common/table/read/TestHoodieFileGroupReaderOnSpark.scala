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

package org.apache.hudi.common.table.read

import org.apache.hudi.{AvroConversionUtils, DataSourceWriteOptions, DefaultSparkRecordMerger, HoodieSchemaConversionUtils, HoodieSparkUtils, SparkAdapterSupport, SparkFileFormatInternalRowReaderContext}
import org.apache.hudi.DataSourceWriteOptions.{OPERATION, RECORDKEY_FIELD, TABLE_TYPE}
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.{HoodieReaderConfig, RecordMergeMode, TypedProperties}
import org.apache.hudi.common.engine.HoodieReaderContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieFileFormat, HoodieRecord}
import org.apache.hudi.common.model.DefaultHoodieRecordPayload.{DELETE_KEY, DELETE_MARKER}
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.table.read.TestHoodieFileGroupReaderBase.{hoodieRecordsToIndexedRecords, supportedFileFormats}
import org.apache.hudi.common.table.read.TestHoodieFileGroupReaderOnSpark.getFileCount
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.testutils.{HoodieTestDataGenerator, HoodieTestUtils}
import org.apache.hudi.common.util.{Option => HOption, OrderingValues}
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.storage.{StorageConfiguration, StoragePath}
import org.apache.hudi.testutils.{GlutenTestUtils, SparkClientFunctionalTestHarness}

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{HoodieSparkKryoRegistrar, SparkConf}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{Dataset, HoodieInternalRowUtils, Row, SaveMode, SparkSession}
import org.apache.spark.sql.avro.HoodieSparkSchemaConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.execution.datasources.SparkColumnarFileReader
import org.apache.spark.sql.hudi.MultipleColumnarFileFormatReader
import org.apache.spark.sql.internal.SQLConf.LEGACY_RESPECT_NULLABILITY_IN_TEXT_DATASET_CONVERSION
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String
import org.junit.jupiter.api.{AfterEach, BeforeEach, Disabled, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}
import org.mockito.Mockito
import org.mockito.Mockito.when

import java.sql.{Date, Timestamp}
import java.util
import java.util.Collections
import java.util.stream.Collectors

import scala.collection.JavaConverters._

/**
 * Tests {@link HoodieFileGroupReader} with {@link SparkFileFormatInternalRowReaderContext}
 * on Spark
 */
class TestHoodieFileGroupReaderOnSpark extends TestHoodieFileGroupReaderBase[InternalRow] with SparkAdapterSupport {
  var spark: SparkSession = _

  @BeforeEach
  def setup() {
    val sparkConf = new SparkConf
    sparkConf.set("spark.app.name", getClass.getName)
    sparkConf.set("spark.master", "local[8]")
    sparkConf.set("spark.default.parallelism", "4")
    sparkConf.set("spark.sql.shuffle.partitions", "4")
    sparkConf.set("spark.driver.maxResultSize", "2g")
    sparkConf.set("spark.hadoop.mapred.output.compress", "true")
    sparkConf.set("spark.hadoop.mapred.output.compression.codec", "true")
    sparkConf.set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
    sparkConf.set("spark.hadoop.mapred.output.compression.type", "BLOCK")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
    sparkConf.set("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    sparkConf.set("spark.sql.parquet.enableVectorizedReader", "false")
    sparkConf.set("spark.sql.orc.enableVectorizedReader", "false")
    sparkConf.set(LEGACY_RESPECT_NULLABILITY_IN_TEXT_DATASET_CONVERSION.key, "true")
    sparkConf.set("spark.sql.codegen.logging.maxLines", "1000000")
    sparkConf.set("spark.sql.codegen.comments", "true")
    HoodieSparkKryoRegistrar.register(sparkConf)
    GlutenTestUtils.applyGlutenConf(sparkConf)
    spark = SparkSession.builder.config(sparkConf).getOrCreate
    supportedFileFormats = util.Arrays.asList(HoodieFileFormat.PARQUET)
  }

  @AfterEach
  def teardown() {
    if (spark != null) {
      spark.stop()
    }
  }

  override def getStorageConf: StorageConfiguration[_] = {
    HoodieTestUtils.getDefaultStorageConf.getInline
  }

  override def getBasePath: String = {
    tempDir.toAbsolutePath.toUri.toString
  }

  override def getHoodieReaderContext(tablePath: String, schema: HoodieSchema, storageConf: StorageConfiguration[_], metaClient: HoodieTableMetaClient): HoodieReaderContext[InternalRow] = {
    val parquetReader = sparkAdapter.createParquetFileReader(vectorized = false, spark.sessionState.conf, Map.empty, storageConf.unwrapAs(classOf[Configuration]))
    val dataSchema = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(schema)
    val orcReader = sparkAdapter.createOrcFileReader(vectorized = false, spark.sessionState.conf, Map.empty, storageConf.unwrapAs(classOf[Configuration]), dataSchema)
    val lanceReader = sparkAdapter.createLanceFileReader(vectorized = false, spark.sessionState.conf, Map.empty, storageConf.unwrapAs(classOf[Configuration])).orNull
    val multiFormatReader = new MultipleColumnarFileFormatReader(parquetReader, orcReader, lanceReader)
    new SparkFileFormatInternalRowReaderContext(multiFormatReader, Seq.empty, Seq.empty, getStorageConf, metaClient.getTableConfig)
  }

  override def commitProcessedRecordsToTable(recordList: util.List[HoodieRecord[_]],
                             operation: String,
                             firstCommit: Boolean,
                             options: util.Map[String, String],
                             schemaStr: String): Unit = {
    val schema = HoodieSchema.parse(schemaStr)
    val genericRecords = spark.sparkContext.parallelize((hoodieRecordsToIndexedRecords(recordList, schema)
      .stream().map[GenericRecord](entry => entry.getValue.asInstanceOf[GenericRecord])
      .collect(Collectors.toList[GenericRecord])).asScala.toSeq, 2)
    val inputDF: Dataset[Row] = AvroConversionUtils.createDataFrame(genericRecords, schemaStr, spark);

    // Check if Lance format is being used and add required configuration
    val isLanceFormat = options.getOrDefault(HoodieTableConfig.BASE_FILE_FORMAT.key(), "").equalsIgnoreCase("LANCE")

    var writer = inputDF.write.format("hudi")
      .options(options)
      .option("hoodie.compact.inline", "false") // else fails due to compaction & deltacommit instant times being same
      .option("hoodie.datasource.write.operation", operation)
      .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")

    // Lance requires DefaultSparkRecordMerger for Spark InternalRow compatibility
    if (isLanceFormat) {
      writer = writer.option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
    }

    writer.mode(if (firstCommit) SaveMode.Overwrite else SaveMode.Append)
      .save(getBasePath)
  }

  override def getCustomPayload: String = classOf[CustomPayloadForTesting].getName

  override def assertRecordsEqual(schema: HoodieSchema, expected: InternalRow, actual: InternalRow): Unit = {
    assertEquals(expected.numFields, actual.numFields)
    val expectedStruct = HoodieSparkSchemaConverters.toSqlType(schema)._1.asInstanceOf[StructType]

    expected.toSeq(expectedStruct).zip(actual.toSeq(expectedStruct)).zipWithIndex.foreach {
      case ((v1, v2), i) =>
        val fieldType = expectedStruct(i).dataType

        (v1, v2, fieldType) match {
          case (a1: Array[Byte], a2: Array[Byte], _) =>
            assert(java.util.Arrays.equals(a1, a2), s"Mismatch at field $i: expected ${a1.mkString(",")} but got ${a2.mkString(",")}")

          case (m1: MapData, m2: MapData, MapType(keyType, valueType, _)) =>
            val map1 = mapDataToScalaMap(m1, keyType, valueType)
            val map2 = mapDataToScalaMap(m2, keyType, valueType)
            assertEquals(map1, map2, s"Mismatch at field $i: maps not equal")

          case _ =>
            assertEquals(v1, v2, s"Mismatch at field $i")
        }
    }
  }

  def mapDataToScalaMap(mapData: MapData, keyType: DataType, valueType: DataType): Map[Any, Any] = {
    val keys = mapData.keyArray()
    val values = mapData.valueArray()
    (0 until mapData.numElements()).map { i =>
      val k = extractValue(keys, i, keyType)
      val v = extractValue(values, i, valueType)
      k -> v
    }.toMap
  }

  def extractValue(array: ArrayData, index: Int, dt: DataType): Any = dt match {
    case IntegerType => array.getInt(index)
    case LongType    => array.getLong(index)
    case StringType  => array.getUTF8String(index).toString
    case DoubleType  => array.getDouble(index)
    case FloatType   => array.getFloat(index)
    case BooleanType => array.getBoolean(index)
    case BinaryType  => array.getBinary(index)
    // Extend this to support StructType, ArrayType, etc. if needed
    case other       => throw new UnsupportedOperationException(s"Unsupported type: $other")
  }

  /**
   * Tests MOR file group reading across all Spark SQL data types.
   *
   * Schema has 16 typed columns → initial insert of 2*16=32 records.
   * Phase 2: update records 1-16, one UPDATE per typed column (exercises log-file writes for each type).
   * Phase 3: delete records 17-32, one DELETE per typed column equality predicate (exercises predicate
   *          evaluation for each type during log merging).
   * Phase 4: insert one new record (id=33) which lands in a fresh file group.
   *
   * hoodie.parquet.small.file.limit=0 prevents data from being packed into existing base files,
   * keeping each commit in its own log file so the file group reader must merge across all of them.
   */
  @Test
  def testReadMORTableWithAllDataTypes(): Unit = {
    val tableName = "test_all_data_types_mor"
    val tablePath = getBasePath
    val dtypeCount = 16 // number of typed columns

    // Schema shared by both inserts
    val insertSchema = StructType(Seq(
      StructField("id",            IntegerType),
      StructField("col_string",    StringType),
      StructField("col_int",       IntegerType),
      StructField("col_bigint",    LongType),
      StructField("col_long",      LongType),
      StructField("col_smallint",  ShortType),
      StructField("col_tinyint",   ByteType),
      StructField("col_float",     FloatType),
      StructField("col_double",    DoubleType),
      StructField("col_boolean",   BooleanType),
      StructField("col_decimal",   DecimalType(10, 2)),
      StructField("col_timestamp", TimestampType),
      StructField("col_date",      DateType),
      StructField("col_binary",    BinaryType),
      StructField("col_array",     ArrayType(StringType)),
      StructField("col_map",       MapType(StringType, IntegerType)),
      StructField("col_struct",    StructType(Seq(
        StructField("field1", StringType), StructField("field2", IntegerType)))),
      StructField("ts",            LongType)
    ))

    // Common write options for both inserts
    val hudiWriteOpts = Map(
      "hoodie.datasource.write.table.type"              -> "MERGE_ON_READ",
      "hoodie.datasource.write.table.name"              -> tableName,
      "hoodie.datasource.write.recordkey.field"         -> "id",
      "hoodie.datasource.write.precombine.field"        -> "ts",
      "hoodie.datasource.write.operation"               -> "insert",
      "hoodie.write.table.version"                      -> "9",
      "hoodie.compact.inline"                           -> "false",
      "hoodie.parquet.small.file.limit"                 -> "0",
      "hoodie.merge.small.file.group.candidates.limit"  -> "0",
      "hoodie.record.merge.mode"                        -> "EVENT_TIME_ORDERING"
    )

    val baseDate = Date.valueOf("2020-01-01")

    // Helper: compute col_date for a given id (= 2020-01-01 + id days)
    def addDays(n: Int): Date = {
      val c = java.util.Calendar.getInstance()
      c.setTime(baseDate)
      c.add(java.util.Calendar.DATE, n)
      new Date(c.getTimeInMillis)
    }

    // ---- Phase 1: insert 2 * dtypeCount = 32 records (ids 1..32) via DataFrame API
    // Using parallelize (row-based) instead of range() to avoid Gluten columnar shuffle.
    val initRows = spark.sparkContext.parallelize((1 to dtypeCount * 2).map { id =>
      Row(
        id,
        s"str_$id",
        id * 100,
        id.toLong * 10000L,
        id.toLong * 20000L,
        id.toShort,
        id.toByte,
        id.toFloat,
        id.toDouble,
        id % 2 == 0,
        java.math.BigDecimal.valueOf(id).setScale(2),
        new Timestamp(id.toLong * 100000L * 1000L),
        addDays(id),
        s"bin_$id".getBytes("UTF-8"),
        Seq(s"a$id"),
        Map(s"k$id" -> id),
        Row(s"f$id", id),
        id.toLong
      )
    })
    spark.createDataFrame(initRows, insertSchema)
      .write.format("hudi")
      .options(hudiWriteOpts)
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Register the Hudi table in the SQL catalog so UPDATE/DELETE statements work
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING HUDI LOCATION '$tablePath'")

    // Helper: scan the table without shuffle-based aggregation (filter+collect avoids Exchange)
    def hudiRows(condition: String = ""): Array[Row] = {
      val df = spark.read.format("hudi").load(tablePath)
      if (condition.isEmpty) df.collect() else df.filter(condition).collect()
    }

    // ---- File-group view helpers ----
    // Builds a fresh HoodieTableFileSystemView (new metaClient + completed timeline) so
    // each call reflects the latest committed state without caching stale snapshots.
    def buildFsView(): HoodieTableFileSystemView = {
      val mc = HoodieTableMetaClient.builder()
        .setConf(getStorageConf)
        .setBasePath(tablePath)
        .build()
      val view = HoodieTableFileSystemView.fileListingBasedFileSystemView(
        new HoodieSparkEngineContext(new JavaSparkContext(spark.sparkContext)),
        mc, mc.getActiveTimeline.filterCompletedInstants())
      view.loadAllPartitions()
      view
    }

    // Returns (baseFileCount, logFileCount) summed across the latest slice of every file group.
    def fgFileCounts(): (Long, Long) = {
      val view = buildFsView()
      try {
        val slices = view.getAllFileGroups().iterator().asScala
          .flatMap { fg =>
            val opt = fg.getLatestFileSlice
            if (opt.isPresent) Some(opt.get()) else None
          }
          .toSeq
        val baseCount = slices.count(_.getBaseFile.isPresent)
        val logCount  = slices.map(_.getLogFiles.count()).sum
        (baseCount, logCount)
      } finally {
        view.close()
      }
    }

    // Validate initial insert
    assertEquals(dtypeCount * 2, hudiRows().length)
    assertEquals(1, hudiRows("id = 1 AND col_string = 'str_1' AND col_int = 100").length)
    assertEquals(1, hudiRows("id = 17 AND col_string = 'str_17'").length)

    // Phase 1 – file-group layout: every file group has a base parquet file, no log files yet.
    val (baseCountAfterInsert, logCountPhase1) = fgFileCounts()
    assertTrue(baseCountAfterInsert > 0,
      s"Phase 1: expected >=1 file group with a base parquet file after initial insert, found 0")
    assertEquals(0L, logCountPhase1,
      "Phase 1: expected zero log files across all file groups after initial insert")

    // ---- Phase 2: update records 1-16, one UPDATE per typed column ----
    spark.sql(s"UPDATE $tableName SET col_string    = 'updated_str',                                  ts = 101 WHERE id = 1")
    spark.sql(s"UPDATE $tableName SET col_int       = -200,                                           ts = 102 WHERE id = 2")
    spark.sql(s"UPDATE $tableName SET col_bigint    = -30000,                                         ts = 103 WHERE id = 3")
    spark.sql(s"UPDATE $tableName SET col_long      = -40000,                                         ts = 104 WHERE id = 4")
    spark.sql(s"UPDATE $tableName SET col_smallint  = cast(-5 AS SMALLINT),                          ts = 105 WHERE id = 5")
    spark.sql(s"UPDATE $tableName SET col_tinyint   = cast(-6 AS TINYINT),                           ts = 106 WHERE id = 6")
    spark.sql(s"UPDATE $tableName SET col_float     = -7.0,                                          ts = 107 WHERE id = 7")
    spark.sql(s"UPDATE $tableName SET col_double    = -8.0,                                          ts = 108 WHERE id = 8")
    spark.sql(s"UPDATE $tableName SET col_boolean   = true,                                          ts = 109 WHERE id = 9")  // 9 % 2 = 1 -> original false, flip to true
    spark.sql(s"UPDATE $tableName SET col_decimal   = cast(-10.00 AS DECIMAL(10,2)),                 ts = 110 WHERE id = 10")
    spark.sql(s"UPDATE $tableName SET col_timestamp = timestamp_seconds(-1000000),                   ts = 111 WHERE id = 11")
    spark.sql(s"UPDATE $tableName SET col_date      = date('2000-01-01'),                            ts = 112 WHERE id = 12")
    spark.sql(s"UPDATE $tableName SET col_binary    = cast('updated_bin' AS BINARY),                 ts = 113 WHERE id = 13")
    spark.sql(s"UPDATE $tableName SET col_array     = array('x', 'y', 'z'),                         ts = 114 WHERE id = 14")
    spark.sql(s"UPDATE $tableName SET col_map       = map('new_k', -1),                              ts = 115 WHERE id = 15")
    spark.sql(s"UPDATE $tableName SET col_struct    = named_struct('field1', 'updated', 'field2', -1), ts = 116 WHERE id = 16")

    // Validate after updates: 32 records, updated values visible, set-2 records untouched
    assertEquals(dtypeCount * 2, hudiRows().length)
    assertEquals(1, hudiRows("id = 1  AND col_string    = 'updated_str'").length)
    assertEquals(1, hudiRows("id = 2  AND col_int       = -200").length)
    assertEquals(1, hudiRows("id = 3  AND col_bigint    = -30000").length)
    assertEquals(1, hudiRows("id = 4  AND col_long      = -40000").length)
    assertEquals(1, hudiRows("id = 5  AND col_smallint  = cast(-5 AS SMALLINT)").length)
    assertEquals(1, hudiRows("id = 6  AND col_tinyint   = cast(-6 AS TINYINT)").length)
    assertEquals(1, hudiRows("id = 7  AND col_float     = cast(-7.0 AS FLOAT)").length)
    assertEquals(1, hudiRows("id = 8  AND col_double    = -8.0").length)
    assertEquals(1, hudiRows("id = 9  AND col_boolean   = true").length)
    assertEquals(1, hudiRows("id = 10 AND col_decimal   = cast(-10.00 AS DECIMAL(10,2))").length)
    assertEquals(1, hudiRows("id = 11 AND col_timestamp = timestamp_seconds(-1000000)").length)
    assertEquals(1, hudiRows("id = 12 AND col_date      = date('2000-01-01')").length)
    assertEquals(1, hudiRows("id = 13 AND col_binary    = cast('updated_bin' AS BINARY)").length)
    // MAP/ARRAY/STRUCT: Spark SQL '=' doesn't support ordering on these types;
    // use element/field access predicates instead.
    assertEquals(1, hudiRows("id = 14 AND col_array[0] = 'x' AND col_array[1] = 'y' AND col_array[2] = 'z'").length)
    assertEquals(1, hudiRows("id = 15 AND col_map['new_k'] = -1").length)
    assertEquals(1, hudiRows("id = 16 AND col_struct.field1 = 'updated' AND col_struct.field2 = -1").length)
    // Set-2 records untouched
    assertEquals(1, hudiRows("id = 17 AND col_string = 'str_17'").length)

    // Phase 2 – file-group layout: UPDATE delta commits append log files to existing file groups;
    // no compaction, so the number of file groups with base files must not change.
    val (baseCountPhase2, logCountAfterUpdates) = fgFileCounts()
    assertTrue(logCountAfterUpdates > 0,
      s"Phase 2: expected >=1 log file across file groups after updates, found 0")
    assertEquals(baseCountAfterInsert, baseCountPhase2,
      "Phase 2: file-group count with base files must not change after updates (compaction is disabled)")

    // ---- Phase 3: delete records 17-32, one DELETE per typed column equality predicate ----
    // Delete values match original insert formula: col_string='str_N', col_int=N*100, etc.
    // id=25 has col_boolean=(25%2=0)=false; boolean isn't unique so AND id=25 guards correctness.
    // For MAP/ARRAY/STRUCT, use element/field access (direct equality not supported by Spark SQL).
    spark.sql(s"DELETE FROM $tableName WHERE col_string    = 'str_17'")
    spark.sql(s"DELETE FROM $tableName WHERE col_int       = 1800")                                    // 18*100
    spark.sql(s"DELETE FROM $tableName WHERE col_bigint    = 190000")                                  // 19*10000
    spark.sql(s"DELETE FROM $tableName WHERE col_long      = 400000")                                  // 20*20000
    spark.sql(s"DELETE FROM $tableName WHERE col_smallint  = cast(21 AS SMALLINT)")
    spark.sql(s"DELETE FROM $tableName WHERE col_tinyint   = cast(22 AS TINYINT)")
    spark.sql(s"DELETE FROM $tableName WHERE col_float     = cast(23 AS FLOAT)")
    spark.sql(s"DELETE FROM $tableName WHERE col_double    = cast(24 AS DOUBLE)")
    spark.sql(s"DELETE FROM $tableName WHERE col_boolean   = false AND id = 25")
    spark.sql(s"DELETE FROM $tableName WHERE col_decimal   = cast(26 AS DECIMAL(10,2))")
    spark.sql(s"DELETE FROM $tableName WHERE col_timestamp = timestamp_seconds(2700000)")              // 27*100000
    spark.sql(s"DELETE FROM $tableName WHERE col_date      = date_add(date('2020-01-01'), 28)")
    spark.sql(s"DELETE FROM $tableName WHERE col_binary    = cast('bin_29' AS BINARY)")
    spark.sql(s"DELETE FROM $tableName WHERE col_array[0]  = 'a30' AND size(col_array) = 1")
    spark.sql(s"DELETE FROM $tableName WHERE col_map['k31'] = 31")
    spark.sql(s"DELETE FROM $tableName WHERE col_struct.field1 = 'f32' AND col_struct.field2 = 32")

    // Validate after deletes: only records 1-16 remain with their updated values
    assertEquals(dtypeCount, hudiRows().length)
    assertEquals(0, hudiRows("id >= 17").length)
    // Updated values from phase 2 still correct after deletes
    assertEquals(1, hudiRows("id = 1  AND col_string = 'updated_str'").length)
    assertEquals(1, hudiRows("id = 16 AND col_struct.field1 = 'updated' AND col_struct.field2 = -1").length)

    // Phase 3 – file-group layout: DELETE delta commits append further log files on existing file groups.
    val (_, logCountAfterDeletes) = fgFileCounts()
    assertTrue(logCountAfterDeletes > logCountAfterUpdates,
      s"Phase 3: expected more log files after deletes ($logCountAfterDeletes) than after updates ($logCountAfterUpdates)")

    // ---- Phase 4: insert one new record (id=33) into a fresh file group ----
    // DataFrame API to avoid Gluten columnar shuffle (same reason as Phase 1)
    val newRow = spark.sparkContext.parallelize(Seq(Row(
      33, "new_record", 3300, 330000L, 660000L,
      33.toShort, 33.toByte, 33.0f, 33.0, true,
      java.math.BigDecimal.valueOf(33).setScale(2),
      new Timestamp(3300000L * 1000L),
      addDays(33),
      "bin_33".getBytes("UTF-8"),
      Seq("new_a", "new_b"),
      Map("nk" -> 33),
      Row("nf", 33),
      133L
    )))
    spark.createDataFrame(newRow, insertSchema)
      .write.format("hudi")
      .options(hudiWriteOpts)
      .mode(SaveMode.Append)
      .save(tablePath)

    // Validate final state: 17 records, new record readable with correct values
    assertEquals(dtypeCount + 1, hudiRows().length)
    assertEquals(1, hudiRows("id = 33 AND col_string = 'new_record' AND col_int = 3300").length)
    assertEquals(1, hudiRows("id = 33 AND col_array[0] = 'new_a' AND col_array[1] = 'new_b'").length)
    assertEquals(1, hudiRows("id = 33 AND col_struct.field1 = 'nf' AND col_struct.field2 = 33").length)

    // Phase 4 – file-group layout: inserting id=33 with small.file.limit=0 must allocate a
    // brand-new file group (new parquet base file, no logs) rather than reuse an existing one.
    val (baseCountPhase4, _) = fgFileCounts()
    assertTrue(baseCountPhase4 > baseCountAfterInsert,
      s"Phase 4: expected a new file group with a base parquet file for id=33 " +
      s"(small.file.limit=0 forces new file group); file groups after Phase 1=$baseCountAfterInsert, after Phase 4=$baseCountPhase4")

    spark.sql(s"DROP TABLE IF EXISTS $tableName")
  }

  // ---------------------------------------------------------------------------
  // Focused single-column tests to isolate the Gluten/Velox WSCG sizeInBytes
  // assertion. Each test uses a minimal schema (id, col_str, ONE complex type,
  // ts) and exercises insert → update → delete → insert so that HoodieSparkUtils
  // .createRdd is exercised for each type in isolation.
  // ---------------------------------------------------------------------------

  private def morSingleTypeHudiOpts(tableName: String): Map[String, String] = Map(
    "hoodie.datasource.write.table.type"             -> "MERGE_ON_READ",
    "hoodie.datasource.write.table.name"             -> tableName,
    "hoodie.datasource.write.recordkey.field"        -> "id",
    "hoodie.datasource.write.precombine.field"       -> "ts",
    "hoodie.datasource.write.operation"              -> "insert",
    "hoodie.write.table.version"                     -> "9",
    "hoodie.compact.inline"                          -> "false",
    "hoodie.parquet.small.file.limit"                -> "0",
    "hoodie.merge.small.file.group.candidates.limit" -> "0",
    "hoodie.record.merge.mode"                       -> "EVENT_TIME_ORDERING"
  )

  @Test
  def testMORWithDecimal(): Unit = {
    val tableName = "test_mor_decimal"
    val tablePath = getBasePath
    val schema = StructType(Seq(
      StructField("id",          IntegerType),
      StructField("col_str",     StringType),
      StructField("col_decimal", DecimalType(10, 2)),
      StructField("ts",          LongType)
    ))
    val opts = morSingleTypeHudiOpts(tableName)
    val rows = spark.sparkContext.parallelize((1 to 4).map { id =>
      Row(id, s"s$id", java.math.BigDecimal.valueOf(id * 10).setScale(2), id.toLong)
    })
    spark.createDataFrame(rows, schema).write.format("hudi").options(opts).mode(SaveMode.Overwrite).save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING HUDI LOCATION '$tablePath'")
    def hudiRows(cond: String = ""): Array[Row] = {
      val df = spark.read.format("hudi").load(tablePath)
      if (cond.isEmpty) df.collect() else df.filter(cond).collect()
    }
    assertEquals(4, hudiRows().length)
    spark.sql(s"UPDATE $tableName SET col_decimal = cast(-99.99 AS DECIMAL(10,2)), ts = 100 WHERE id = 1")
    assertEquals(1, hudiRows("id = 1 AND col_decimal = cast(-99.99 AS DECIMAL(10,2))").length)
    spark.sql(s"DELETE FROM $tableName WHERE id = 2")
    assertEquals(3, hudiRows().length)
    val newRow = spark.sparkContext.parallelize(Seq(Row(5, "s5", java.math.BigDecimal.valueOf(50).setScale(2), 5L)))
    spark.createDataFrame(newRow, schema).write.format("hudi").options(opts).mode(SaveMode.Append).save(tablePath)
    assertEquals(4, hudiRows().length)
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
  }

  @Test
  def testMORWithBinary(): Unit = {
    val tableName = "test_mor_binary"
    val tablePath = getBasePath
    val schema = StructType(Seq(
      StructField("id",         IntegerType),
      StructField("col_str",    StringType),
      StructField("col_binary", BinaryType),
      StructField("ts",         LongType)
    ))
    val opts = morSingleTypeHudiOpts(tableName)
    val rows = spark.sparkContext.parallelize((1 to 4).map { id =>
      Row(id, s"s$id", s"bin_$id".getBytes("UTF-8"), id.toLong)
    })
    spark.createDataFrame(rows, schema).write.format("hudi").options(opts).mode(SaveMode.Overwrite).save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING HUDI LOCATION '$tablePath'")
    def hudiRows(cond: String = ""): Array[Row] = {
      val df = spark.read.format("hudi").load(tablePath)
      if (cond.isEmpty) df.collect() else df.filter(cond).collect()
    }
    assertEquals(4, hudiRows().length)
    spark.sql(s"UPDATE $tableName SET col_binary = cast('updated_bin' AS BINARY), ts = 100 WHERE id = 1")
    assertEquals(1, hudiRows("id = 1 AND col_binary = cast('updated_bin' AS BINARY)").length)
    spark.sql(s"DELETE FROM $tableName WHERE id = 2")
    assertEquals(3, hudiRows().length)
    val newRow = spark.sparkContext.parallelize(Seq(Row(5, "s5", "bin_5".getBytes("UTF-8"), 5L)))
    spark.createDataFrame(newRow, schema).write.format("hudi").options(opts).mode(SaveMode.Append).save(tablePath)
    assertEquals(4, hudiRows().length)
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
  }

  @Test
  def testMORWithArray(): Unit = {
    val tableName = "test_mor_array"
    val tablePath = getBasePath
    val schema = StructType(Seq(
      StructField("id",        IntegerType),
      StructField("col_str",   StringType),
      StructField("col_array", ArrayType(StringType)),
      StructField("ts",        LongType)
    ))
    spark.sparkContext.hadoopConfiguration.set("parquet.avro.write-old-list-structure", "false")
    val opts = morSingleTypeHudiOpts(tableName)
    val rows = spark.sparkContext.parallelize((1 to 4).map { id =>
      Row(id, s"s$id", Seq(s"a$id", s"b$id"), id.toLong)
    })
    spark.createDataFrame(rows, schema).write.format("hudi").options(opts).mode(SaveMode.Overwrite).save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING HUDI LOCATION '$tablePath'")
    def hudiRows(cond: String = ""): Array[Row] = {
      val df = spark.read.format("hudi").load(tablePath)
      if (cond.isEmpty) df.collect() else df.filter(cond).collect()
    }
    assertEquals(4, hudiRows().length)
    spark.sql(s"UPDATE $tableName SET col_array = array('x', 'y', 'z'), ts = 100 WHERE id = 1")
    assertEquals(1, hudiRows("id = 1 AND col_array[0] = 'x' AND col_array[1] = 'y'").length)
    spark.sql(s"DELETE FROM $tableName WHERE id = 2")
    assertEquals(3, hudiRows().length)
    val newRow = spark.sparkContext.parallelize(Seq(Row(5, "s5", Seq("new_a", "new_b"), 5L)))
    spark.createDataFrame(newRow, schema).write.format("hudi").options(opts).mode(SaveMode.Append).save(tablePath)
    assertEquals(4, hudiRows().length)
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    spark.sparkContext.hadoopConfiguration.unset("parquet.avro.write-old-list-structure")
  }

  @Test
  def testMORWithMap(): Unit = {
    val tableName = "test_mor_map"
    val tablePath = getBasePath
    val schema = StructType(Seq(
      StructField("id",      IntegerType),
      StructField("col_str", StringType),
      StructField("col_map", MapType(StringType, IntegerType)),
      StructField("ts",      LongType)
    ))
    val opts = morSingleTypeHudiOpts(tableName)
    val rows = spark.sparkContext.parallelize((1 to 4).map { id =>
      Row(id, s"s$id", Map(s"k$id" -> id), id.toLong)
    })
    spark.createDataFrame(rows, schema).write.format("hudi").options(opts).mode(SaveMode.Overwrite).save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING HUDI LOCATION '$tablePath'")
    def hudiRows(cond: String = ""): Array[Row] = {
      val df = spark.read.format("hudi").load(tablePath)
      if (cond.isEmpty) df.collect() else df.filter(cond).collect()
    }
    assertEquals(4, hudiRows().length)
    spark.sql(s"UPDATE $tableName SET col_map = map('new_k', -1), ts = 100 WHERE id = 1")
    assertEquals(1, hudiRows("id = 1 AND col_map['new_k'] = -1").length)
    spark.sql(s"DELETE FROM $tableName WHERE id = 2")
    assertEquals(3, hudiRows().length)
    val newRow = spark.sparkContext.parallelize(Seq(Row(5, "s5", Map("nk" -> 5), 5L)))
    spark.createDataFrame(newRow, schema).write.format("hudi").options(opts).mode(SaveMode.Append).save(tablePath)
    assertEquals(4, hudiRows().length)
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
  }

  @Test
  def testMORWithStruct(): Unit = {
    val tableName = "test_mor_struct"
    val tablePath = getBasePath
    val schema = StructType(Seq(
      StructField("id",         IntegerType),
      StructField("col_str",    StringType),
      StructField("col_struct", StructType(Seq(
        StructField("field1", StringType), StructField("field2", IntegerType)))),
      StructField("ts",         LongType)
    ))
    val opts = morSingleTypeHudiOpts(tableName)
    val rows = spark.sparkContext.parallelize((1 to 4).map { id =>
      Row(id, s"s$id", Row(s"f$id", id), id.toLong)
    })
    spark.createDataFrame(rows, schema).write.format("hudi").options(opts).mode(SaveMode.Overwrite).save(tablePath)
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING HUDI LOCATION '$tablePath'")
    def hudiRows(cond: String = ""): Array[Row] = {
      val df = spark.read.format("hudi").load(tablePath)
      if (cond.isEmpty) df.collect() else df.filter(cond).collect()
    }
    assertEquals(4, hudiRows().length)
    spark.sql(s"UPDATE $tableName SET col_struct = named_struct('field1', 'updated', 'field2', -1), ts = 100 WHERE id = 1")
    assertEquals(1, hudiRows("id = 1 AND col_struct.field1 = 'updated' AND col_struct.field2 = -1").length)
    spark.sql(s"DELETE FROM $tableName WHERE id = 2")
    assertEquals(3, hudiRows().length)
    val newRow = spark.sparkContext.parallelize(Seq(Row(5, "s5", Row("nf", 5), 5L)))
    spark.createDataFrame(newRow, schema).write.format("hudi").options(opts).mode(SaveMode.Append).save(tablePath)
    assertEquals(4, hudiRows().length)
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
  }

  @Test
  def testGetOrderingValue(): Unit = {
    val reader = Mockito.mock(classOf[SparkColumnarFileReader])
    val tableConfig = Mockito.mock(classOf[HoodieTableConfig])
    Mockito.when(tableConfig.populateMetaFields()).thenReturn(true)
    val sparkReaderContext = new SparkFileFormatInternalRowReaderContext(reader, Seq.empty, Seq.empty, getStorageConf, tableConfig)
    val orderingFieldName = "col2"
    val avroSchema = new Schema.Parser().parse(
      "{\"type\": \"record\",\"name\": \"test\",\"namespace\": \"org.apache.hudi\",\"fields\": ["
        + "{\"name\": \"col1\", \"type\": \"string\" },"
        + "{\"name\": \"col2\", \"type\": \"long\" },"
        + "{ \"name\": \"col3\", \"type\": [\"null\", \"string\"], \"default\": null}]}")
    val row = InternalRow("item", 1000L, UTF8String.fromString("blue"))
    testGetOrderingValue(sparkReaderContext, row, avroSchema, orderingFieldName, 1000L)
    testGetOrderingValue(
      sparkReaderContext, row, avroSchema, "col3", sparkAdapter.getUTF8StringFactory.wrapUTF8String(UTF8String.fromString("blue")))
    testGetOrderingValue(
      sparkReaderContext, row, avroSchema, "non_existent_col", OrderingValues.getDefault)
  }

  val expectedEventTimeBased: Seq[(Int, String, String, String, Double, String)] = Seq(
    (10, "5", "rider-E", "driver-E", 17.85, "i"),
    (10, "3", "rider-C", "driver-C", 33.9, "i"),
    (10, "2", "rider-B", "driver-B", 27.7, "i"),
    (20, "1", "rider-Z", "driver-Z", 27.7, "i"))
  val expectedCommitTimeBased: Seq[(Int, String, String, String, Double, String)] = Seq(
    (10, "5", "rider-E", "driver-E", 17.85, "i"),
    (10, "3", "rider-C", "driver-C", 33.9, "i"),
    (20, "1", "rider-Z", "driver-Z", 27.7, "i"))

  @Disabled("Custom delete payload not supported")
  @ParameterizedTest
  @MethodSource(Array("customDeleteTestParams"))
  def testCustomDelete(useFgReader: String,
                       tableType: String,
                       positionUsed: String,
                       mergeMode: String): Unit = {
    val payloadClass = "org.apache.hudi.common.table.read.CustomPayloadForTesting"
    val fgReaderOpts: Map[String, String] = Map(
      HoodieWriteConfig.MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key -> "0",
      HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key -> useFgReader,
      HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS.key -> positionUsed,
      HoodieWriteConfig.RECORD_MERGE_MODE.key -> mergeMode
    )
    val deleteOpts: Map[String, String] = Map(
      DELETE_KEY -> "op", DELETE_MARKER -> "d")
    val readOpts = if (mergeMode.equals("CUSTOM")) {
      fgReaderOpts ++ deleteOpts ++ Map(
        HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key -> payloadClass)
    } else {
      fgReaderOpts ++ deleteOpts
    }
    val opts = readOpts
    val columns = Seq("ts", "key", "rider", "driver", "fare", "op")

    val data = Seq(
      (10, "1", "rider-A", "driver-A", 19.10, "i"),
      (10, "2", "rider-B", "driver-B", 27.70, "i"),
      (10, "3", "rider-C", "driver-C", 33.90, "i"),
      (10, "4", "rider-D", "driver-D", 34.15, "i"),
      (10, "5", "rider-E", "driver-E", 17.85, "i"))
    val inserts = spark.createDataFrame(data).toDF(columns: _*)
    inserts.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(HoodieTableConfig.ORDERING_FIELDS.key(), "ts").
      option(TABLE_TYPE.key(), tableType).
      option(DataSourceWriteOptions.TABLE_NAME.key(), "test_table").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      options(opts).
      mode(SaveMode.Overwrite).
      save(getBasePath)
    val metaClient = HoodieTableMetaClient
      .builder().setConf(getStorageConf).setBasePath(getBasePath).build
    assertEquals((1, 0), getFileCount(metaClient, getBasePath))

    // Delete using delete markers.
    val updateData = Seq(
      (11, "1", "rider-X", "driver-X", 19.10, "d"),
      (9, "2", "rider-Y", "driver-Y", 27.70, "d"))
    val updates = spark.createDataFrame(updateData).toDF(columns: _*)
    updates.write.format("hudi").
      option(OPERATION.key(), "upsert").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      options(opts).
      mode(SaveMode.Append).
      save(getBasePath)
    assertEquals((1, 1), getFileCount(metaClient, getBasePath))

    // Delete from operation.
    val deletesData = Seq((-5, "4", "rider-D", "driver-D", 34.15, 6))
    val deletes = spark.createDataFrame(deletesData).toDF(columns: _*)
    deletes.write.format("hudi").
      option(OPERATION.key(), "DELETE").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      options(opts).
      mode(SaveMode.Append).
      save(getBasePath)
    assertEquals((1, 2), getFileCount(metaClient, getBasePath))

    // Add a record back to test ensure event time ordering work.
    val updateDataSecond = Seq(
      (20, "1", "rider-Z", "driver-Z", 27.70, "i"))
    val updatesSecond = spark.createDataFrame(updateDataSecond).toDF(columns: _*)
    updatesSecond.write.format("hudi").
      option(OPERATION.key(), "upsert").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      options(opts).
      mode(SaveMode.Append).
      save(getBasePath)
    // Validate data file number.
    assertEquals((1, 3), getFileCount(metaClient, getBasePath))

    // Validate in the end.
    val columnsToCompare = Set("ts", "key", "rider", "driver", "fare", "op")
    val df = spark.read.options(readOpts).format("hudi").load(getBasePath)
    val finalDf = df.select("ts", "key", "rider", "driver", "fare", "op").sort("key")
    val expected = if (mergeMode != RecordMergeMode.COMMIT_TIME_ORDERING.name()) {
      expectedEventTimeBased
    } else {
      expectedCommitTimeBased
    }
    val expectedDf = spark.createDataFrame(expected).toDF(columns: _*).sort("key")
    assertTrue(
      SparkClientFunctionalTestHarness.areDataframesEqual(expectedDf, finalDf, columnsToCompare.asJava))
  }

  private def testGetOrderingValue(sparkReaderContext: HoodieReaderContext[InternalRow],
                                   row: InternalRow,
                                   avroSchema: Schema,
                                   orderingColumn: String,
                                   expectedOrderingValue: Comparable[_]): Unit = {
    assertEquals(expectedOrderingValue, sparkReaderContext.getRecordContext.getOrderingValue(row, HoodieSchema.fromAvroSchema(avroSchema), Collections.singletonList(orderingColumn)))
  }

  @Test
  def getRecordKeyFromMetadataFields(): Unit = {
    val reader = Mockito.mock(classOf[SparkColumnarFileReader])
    val tableConfig = Mockito.mock(classOf[HoodieTableConfig])
    val storageConf = Mockito.mock(classOf[StorageConfiguration[_]])
    when(tableConfig.populateMetaFields()).thenReturn(true)
    val sparkReaderContext = new SparkFileFormatInternalRowReaderContext(reader, Seq.empty, Seq.empty, storageConf, tableConfig)
    val schema = SchemaBuilder.builder()
      .record("test")
      .fields()
      .requiredString(HoodieRecord.RECORD_KEY_METADATA_FIELD)
      .optionalString("field2")
      .endRecord()
    val key = "my_key"
    val row = InternalRow.fromSeq(Seq(UTF8String.fromString(key), UTF8String.fromString("value2")))
    assertEquals(key, sparkReaderContext.getRecordContext().getRecordKey(row, HoodieSchema.fromAvroSchema(schema)))
  }

  @Test
  def getRecordKeySingleKey(): Unit = {
    val reader = Mockito.mock(classOf[SparkColumnarFileReader])
    val tableConfig = Mockito.mock(classOf[HoodieTableConfig])
    when(tableConfig.populateMetaFields()).thenReturn(false)
    when(tableConfig.getRecordKeyFields).thenReturn(HOption.of(Array("field1")))
    val storageConf = Mockito.mock(classOf[StorageConfiguration[_]])
    val props = new TypedProperties
    props.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "field1,field2")
    when(tableConfig.getProps).thenReturn(props)
    val sparkReaderContext = new SparkFileFormatInternalRowReaderContext(reader, Seq.empty, Seq.empty, storageConf, tableConfig)
    val schema = SchemaBuilder.builder()
      .record("test")
      .fields()
      .requiredString("field1")
      .optionalString("field2")
      .endRecord()
    val key = "key"
    val row = InternalRow.fromSeq(Seq(UTF8String.fromString(key), UTF8String.fromString("other")))
    assertEquals(key, sparkReaderContext.getRecordContext().getRecordKey(row, HoodieSchema.fromAvroSchema(schema)))
  }

  @Test
  def getRecordKeyWithMultipleKeys(): Unit = {
    val reader = Mockito.mock(classOf[SparkColumnarFileReader])
    val tableConfig = Mockito.mock(classOf[HoodieTableConfig])
    when(tableConfig.populateMetaFields()).thenReturn(false)
    when(tableConfig.getRecordKeyFields).thenReturn(HOption.of(Array("outer1.field1", "outer1.field2", "outer1.field3")))
    val storageConf = Mockito.mock(classOf[StorageConfiguration[_]])
    val sparkReaderContext = new SparkFileFormatInternalRowReaderContext(reader, Seq.empty, Seq.empty, storageConf, tableConfig)
    val schema: _root_.org.apache.avro.Schema = buildMultiLevelSchema
    val key = "outer1.field1:compound,outer1.field2:__empty__,outer1.field3:__null__"
    val innerRow = InternalRow.fromSeq(Seq(UTF8String.fromString("compound"), UTF8String.fromString(""), null))
    val row = InternalRow.fromSeq(Seq(innerRow, UTF8String.fromString("value2")))
    assertEquals(key, sparkReaderContext.getRecordContext.getRecordKey(row, HoodieSchema.fromAvroSchema(schema)))
  }

  @Test
  def getNestedValue(): Unit = {
    val reader = Mockito.mock(classOf[SparkColumnarFileReader])
    val tableConfig = Mockito.mock(classOf[HoodieTableConfig])
    when(tableConfig.populateMetaFields()).thenReturn(true)
    val storageConf = Mockito.mock(classOf[StorageConfiguration[_]])
    val sparkReaderContext = new SparkFileFormatInternalRowReaderContext(reader, Seq.empty, Seq.empty, storageConf, tableConfig)
    val schema: Schema = buildMultiLevelSchema
    val innerRow = InternalRow.fromSeq(Seq(UTF8String.fromString("nested_value"), UTF8String.fromString(""), null))
    val row = InternalRow.fromSeq(Seq(innerRow, UTF8String.fromString("value2")))
    assertEquals("nested_value", sparkReaderContext.getRecordContext().getValue(row, HoodieSchema.fromAvroSchema(schema), "outer1.field1").toString)
  }

  private def buildMultiLevelSchema = {
    val innerSchema = SchemaBuilder.builder()
      .record("inner")
      .fields()
      .requiredString("field1")
      .optionalString("field2")
      .optionalString("field3")
      .endRecord()
    val schema = Schema.createRecord("outer", null, null, false);
    schema.setFields(util.Arrays.asList(
      new Schema.Field("outer1", innerSchema, null, null),
      new Schema.Field("outer2", Schema.create(Schema.Type.STRING), null, null)
    ))
    schema
  }

  override def assertRecordMatchesSchema(schema: HoodieSchema, record: InternalRow): Unit = {
    val structType = HoodieInternalRowUtils.getCachedSchema(schema)
    assertRecordMatchesSchema(structType, record)
  }

  private def assertRecordMatchesSchema(structType: StructType, record: InternalRow): Unit = {
    val values = record.toSeq(structType)
    structType.zip(values).foreach { r =>
      r._1.dataType match {
        case struct: StructType => assertRecordMatchesSchema(struct, r._2.asInstanceOf[InternalRow])
        case array: ArrayType => assertArrayMatchesSchema(array.elementType, r._2.asInstanceOf[ArrayData])
        case map: MapType => asserMapMatchesSchema(map, r._2.asInstanceOf[MapData])
        case _ =>
      }
    }
  }

  private def assertArrayMatchesSchema(schema: DataType, array: ArrayData): Unit = {
    val arrayValues = array.toSeq[Any](schema)
    schema match {
      case structType: StructType =>
        arrayValues.foreach(v => assertRecordMatchesSchema(structType, v.asInstanceOf[InternalRow]))
      case arrayType: ArrayType =>
        arrayValues.foreach(v => assertArrayMatchesSchema(arrayType.elementType, v.asInstanceOf[ArrayData]))
      case mapType: MapType =>
        arrayValues.foreach(v => asserMapMatchesSchema(mapType, v.asInstanceOf[MapData]))
      case _ =>
    }
  }

  private def asserMapMatchesSchema(schema: MapType, map: MapData): Unit = {
    assertArrayMatchesSchema(schema.keyType, map.keyArray())
    assertArrayMatchesSchema(schema.valueType, map.valueArray())
  }

  override def getSchemaEvolutionConfigs: HoodieTestDataGenerator.SchemaEvolutionConfigs = {
    new HoodieTestDataGenerator.SchemaEvolutionConfigs()
  }
}

object TestHoodieFileGroupReaderOnSpark {
  def customDeleteTestParams(): java.util.List[Arguments] = {
    java.util.Arrays.asList(
      Arguments.of("true", "MERGE_ON_READ", "false", "EVENT_TIME_ORDERING"),
      Arguments.of("true", "MERGE_ON_READ", "true", "EVENT_TIME_ORDERING"),
      Arguments.of("true", "MERGE_ON_READ", "false", "COMMIT_TIME_ORDERING"),
      Arguments.of("true", "MERGE_ON_READ", "true", "COMMIT_TIME_ORDERING"))
  }

  def getFileCount(metaClient: HoodieTableMetaClient, basePath: String): (Long, Long) = {
    val newMetaClient = HoodieTableMetaClient.reload(metaClient)
    val files = newMetaClient.getStorage.listFiles(new StoragePath(basePath))
    (files.stream().filter(f =>
      f.getPath.getParent.equals(new StoragePath(basePath))
        && FSUtils.isBaseFile(f.getPath)).count(),
      files.stream().filter(f =>
        f.getPath.getParent.equals(new StoragePath(basePath))
          && FSUtils.isLogFile(f.getPath)).count())
  }
}
