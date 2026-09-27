/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hudi.{HoodieFileIndex, HoodieSchemaConversionUtils, HoodieSparkUtils, HoodieTableSchema, SparkAdapterSupport, SparkFileFormatInternalRowReaderContext}
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.client.utils.SparkInternalSchemaConverter
import org.apache.hudi.common.config.{HoodieMemoryConfig, TypedProperties}
import org.apache.hudi.common.model.HoodieFileFormat
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.schema.HoodieSchemaUtils
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.table.read.{FileGroupReaderTableState, HoodieFileGroupReader}
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.exception.HoodieNotSupportedException
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.io.IOUtils
import org.apache.hudi.io.storage.HoodieSparkParquetReader.ENABLE_LOGICAL_TIMESTAMP_REPAIR
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.schema.HoodieSchemaRepair
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile, SparkColumnarFileReader}
import org.apache.spark.sql.execution.datasources.orc.OrcUtils
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.hudi.MultipleColumnarFileFormatReader
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}

trait HoodieFormatTrait {

  // Used so that the planner only projects once and does not stack overflow
  var isProjected: Boolean = false
  def getRequiredFilters: Seq[Filter]
}

/**
 * This class utilizes {@link HoodieFileGroupReader} and its related classes to support reading
 * from Parquet or ORC formatted base files and their log files.
 */
class HoodieFileGroupReaderBasedFileFormat(tablePath: String,
                                           tableSchema: HoodieTableSchema,
                                           tableName: String,
                                           queryTimestamp: String,
                                           mandatoryFields: Seq[String],
                                           isMOR: Boolean,
                                           isBootstrap: Boolean,
                                           isIncremental: Boolean,
                                           validCommits: String,
                                           shouldUseRecordPosition: Boolean,
                                           requiredFilters: Seq[Filter],
                                           isMultipleBaseFileFormatsEnabled: Boolean,
                                           hoodieFileFormat: HoodieFileFormat,
                                           @transient tableMetaClient: Option[HoodieTableMetaClient] = None)
  extends ParquetFileFormat with SparkAdapterSupport with HoodieFormatTrait with Logging with Serializable {

  private lazy val schema = tableSchema.schema

  private lazy val hasTimestampMillisFieldInTableSchema = HoodieSchemaRepair.hasTimestampMillisField(schema)
  private lazy val supportBatchWithTableSchema = HoodieSparkUtils.gteqSpark3_5 || !hasTimestampMillisFieldInTableSchema
  override def shortName(): String = "HudiFileGroup"

  override def toString: String = "HoodieFileGroupReaderBasedFileFormat"

  def getRequiredFilters: Seq[Filter] = requiredFilters

  private val sanitizedTableName = HoodieSchemaUtils.getRecordQualifiedName(tableName)

  /**
   * Cached result of vector column detection keyed by schema identity.
   * Avoids re-parsing metadata on repeated supportBatch calls with the same schema.
   */
  @transient private var cachedVectorDetection: (StructType, Map[Int, HoodieSchema.Vector]) = _

  private def detectVectorColumnsCached(schema: StructType): Map[Int, HoodieSchema.Vector] = {
    // Read once: a format instance can be shared by concurrent planners.
    val cached = cachedVectorDetection
    if (cached != null && (cached._1 eq schema)) {
      cached._2
    } else {
      val result = detectVectorColumns(schema)
      cachedVectorDetection = (schema, result)
      result
    }
  }

  /**
   * Checks if the scan can return columnar batches, please refer to SPARK-40918. It has no side
   * effects: Spark does not call it for scans wider than spark.sql.codegen.maxFields or with
   * spark.sql.codegen.wholeStage off, so [[buildReaderWithPartitionValues]] decides vectorized
   * decoding on its own.
   *
   * NOTE: for mor read, even for file-slice with only base file, we can read parquet file with vectorized read,
   * but the return result of the whole data-source-scan phase cannot be batch,
   * because when there are any log file in a file slice, it needs to be read by the file group reader.
   * Since we are currently performing merges based on rows, the result returned by merging should be based on rows,
   * we cannot assume that all file slices have only base files.
   * So we need to set the batch result back to false.
   *
   */
  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
    val supportReturningBatch = !isMOR && supportVectorizedRead(sparkSession, schema)
    logDebug(s"supportReturningBatch: $supportReturningBatch, isMOR: $isMOR")
    supportReturningBatch
  }

  /**
   * Whether base files of a scan with output `schema` can be decoded by the vectorized reader,
   * whether the scan then returns batches or rows. Mirrors Spark's ParquetFileFormat, which
   * decodes vectorized whenever the schema allows it and returns batches only when
   * [[supportBatch]] says so.
   */
  private def supportVectorizedRead(sparkSession: SparkSession, schema: StructType): Boolean = {
    // Vector columns are stored as FIXED_LEN_BYTE_ARRAY in Parquet but read as ArrayType in Spark.
    // The binary→array conversion requires row-level access, so disable vectorized batch reading.
    if (detectVectorColumnsCached(schema).nonEmpty) {
      false
    } else if (schema.fields.exists(f => f.dataType.isInstanceOf[StructType]
        && sparkAdapter.isVariantProjectionStruct(f.dataType.asInstanceOf[StructType]))) {
      // Spark 4.1's PushVariantIntoScan rewrites a variant column to a struct of pushed-down
      // extractions. The Spark vectorized parquet reader treats this as a nested type change
      // (data column is VariantType, required is a struct): batch output fails on it
      // (ParquetSchemaEvolutionUtils throws) and row output would read it row-based per file
      // anyway. Force row-based reading on this path.
      false
    } else if (HoodieSparkUtils.gteqSpark4_1 && schema.fields.exists(f => sparkAdapter.isVariantType(f.dataType))) {
      // #18605: Spark 4.1's vectorized variant read produces UnsafeRow encodings that SIGBUS
      // during RangePartitioner sampling. Force row-based reads. Spark 4.0 unaffected.
      false
    } else {
      val conf = sparkSession.sessionState.conf
      val parquetBatchSupported = ParquetUtils.isBatchReadSupportedForSchema(conf, schema) && supportBatchWithTableSchema
      val orcBatchSupported = conf.orcVectorizedReaderEnabled &&
        schema.forall(s => OrcUtils.supportColumnarReads(
          s.dataType, sparkSession.sessionState.conf.orcVectorizedReaderNestedColumnEnabled))
      // TODO: Implement columnar batch reading https://github.com/apache/hudi/issues/17736
      val lanceBatchSupported = false

      val supportBatch = if (isMultipleBaseFileFormatsEnabled) {
        parquetBatchSupported && orcBatchSupported
      } else if (hoodieFileFormat == HoodieFileFormat.PARQUET) {
        parquetBatchSupported
      } else if (hoodieFileFormat == HoodieFileFormat.ORC) {
        orcBatchSupported
      } else if (hoodieFileFormat == HoodieFileFormat.LANCE) {
        lanceBatchSupported
      } else {
        throw new HoodieNotSupportedException("Unsupported file format: " + hoodieFileFormat)
      }
      val supportVectorizedRead = !isIncremental && !isBootstrap && supportBatch
      logDebug(s"supportVectorizedRead: $supportVectorizedRead, isIncremental: $isIncremental, " +
        s"isBootstrap: $isBootstrap, superSupportBatch: $supportBatch")
      supportVectorizedRead
    }
  }

  //for partition columns that we read from the file, we don't want them to be constant column vectors so we
  //modify the vector types in this scenario
  override def vectorTypes(requiredSchema: StructType,
                           partitionSchema: StructType,
                           sqlConf: SQLConf): Option[Seq[String]] = {
    val originalVectorTypes = super.vectorTypes(requiredSchema, partitionSchema, sqlConf)
    if (mandatoryFields.isEmpty) {
      originalVectorTypes
    } else {
      val regularVectorType = if (!sqlConf.offHeapColumnVectorEnabled) {
        classOf[OnHeapColumnVector].getName
      } else {
        classOf[OffHeapColumnVector].getName
      }
      originalVectorTypes.map {
        o: Seq[String] => o.zipWithIndex.map(a => {
          val isPartitionField = a._2 >= requiredSchema.length
          if (isPartitionField
            && {
              val fieldName = partitionSchema.fields(a._2 - requiredSchema.length).name
              mandatoryFields.contains(fieldName) && !isNestedPartitionField(fieldName)
            }) {
            regularVectorType
          } else {
            a._1
          }
        })
      }
    }
  }

  private lazy val internalSchemaOpt: HOption[InternalSchema] = if (tableSchema.internalSchema.isEmpty) {
    HOption.empty()
  } else {
    HOption.of(tableSchema.internalSchema.get)
  }

  override def isSplitable(sparkSession: SparkSession,
                           options: Map[String, String],
                           path: Path): Boolean = {
    // NOTE: When we have and only the base file that needs to be read with normal reading mode,
    // we can consider the current format to be equivalent to `org.apache.spark.sql.execution.datasources.parquet.ParquetFormat`.
    // Naturally, we can maintain the same `isSplitable` logic as the upper-level format.
    // This will enable us to take advantage of spark's file splitting capability.
    // For overly large single files, we can use multiple concurrent tasks to read them, thereby reducing the overall job reading time consumption
    val superSplitable = super.isSplitable(sparkSession, options, path)
    val isLance = hoodieFileFormat == HoodieFileFormat.LANCE
    val splitable = !isMOR && !isIncremental && !isBootstrap && !isLance && superSplitable
    logDebug(s"isSplitable: $splitable, super.isSplitable: $superSplitable, isMOR: $isMOR, isIncremental: $isIncremental, isBootstrap: $isBootstrap")
    splitable
  }

  override def buildReaderWithPartitionValues(spark: SparkSession,
                                              dataStructType: StructType,
                                              partitionSchema: StructType,
                                              requiredSchema: StructType,
                                              filters: Seq[Filter],
                                              options: Map[String, String],
                                              hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val outputSchema = StructType(requiredSchema.fields ++ partitionSchema.fields)
    val isCount = requiredSchema.isEmpty && !isMOR && !isIncremental
    // Spark planner only adds the user-provided predicates (from `WHERE` clause or `.filter()`)
    // to `filters`; the `requiredFilters` from `HoodieBaseHadoopFsRelationFactory#getRequiredFilters`
    // are not visible to the planner, thus the `requiredSchema` passed by Spark can miss the
    // columns in `requiredFilters`.  This happens for incremental query where `requiredFilters`
    // is present.  To allow correct projection and filtering, the columns from `requiredFilters`
    // are added back to the `readRequiredSchema` for reading the file.
    val filterOnlyFields = requiredFilters.flatMap(_.references).distinct
      .filterNot(name => requiredSchema.fieldNames.contains(name) || partitionSchema.fieldNames.contains(name))
      .flatMap(name => dataStructType.fields.find(_.name == name))
    val readRequiredSchema = StructType(requiredSchema.fields ++ filterOnlyFields)
    val augmentedStorageConf = new HadoopStorageConfiguration(hadoopConf).getInline
    augmentedStorageConf.set(ENABLE_LOGICAL_TIMESTAMP_REPAIR, hasTimestampMillisFieldInTableSchema.toString)
    // Nested partition columns (e.g. "nested_record.level") are never read from the data file: the
    // flattened dotted name is not a valid top-level field and the value is materialized from the
    // partition path. Always keep them in the appended ("remaining") partition fields so they are
    // not converted into a top-level Avro field below, which would fail Avro name validation.
    val (remainingPartitionSchemaArr, fixedPartitionIndexesArr) = partitionSchema.fields.toSeq.zipWithIndex.filter(p => !mandatoryFields.contains(p._1.name) || isNestedPartitionField(p._1.name)).unzip

    // The schema of the partition cols we want to append the value instead of reading from the file
    val remainingPartitionSchema = StructType(remainingPartitionSchemaArr)

    // index positions of the remainingPartitionSchema fields in partitionSchema
    val fixedPartitionIndexes = fixedPartitionIndexesArr.toSet

    // schema that we want fg reader to output to us
    val exclusionFields = new java.util.HashSet[String]()
    exclusionFields.add("op")
    partitionSchema.fields.foreach(f => exclusionFields.add(f.name))
    val requestedStructType = StructType(readRequiredSchema.fields ++ partitionSchema.fields.filter(f => mandatoryFields.contains(f.name) && !isNestedPartitionField(f.name)))
    val requestedSchema = HoodieSchemaUtils.pruneDataSchema(schema, HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(requestedStructType, sanitizedTableName), exclusionFields)
    val dataStructTypeWithMandatoryPartitionFields = StructType(dataStructType.fields ++ partitionSchema.fields.filter(f => mandatoryFields.contains(f.name) && !isNestedPartitionField(f.name)))
    val dataSchema = HoodieSchemaUtils.pruneDataSchema(schema, HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(dataStructTypeWithMandatoryPartitionFields, sanitizedTableName), exclusionFields)

    // Decided per scan, as Spark's ParquetFileFormat does: base files decode vectorized whenever the
    // output schema allows it, and the reader returns batches only when the scan asked for them
    // through FileFormat.OPTION_RETURNING_BATCH. The session conf is left untouched. A scan planned
    // for batches gets a vectorized reader even if the conf changed after planning.
    val returningBatch = options.get(FileFormat.OPTION_RETURNING_BATCH).contains("true")
    val vectorizedRead = returningBatch || supportVectorizedRead(spark, outputSchema)

    val baseFileReader = spark.sparkContext.broadcast(buildBaseFileReader(spark, options, augmentedStorageConf.unwrap(), dataStructType, vectorizedRead))
    val fileGroupBaseFileReader = if (isMOR && vectorizedRead) {
      // for file group reader to perform read, we always need to read the record without vectorized reader because our merging is based on row level.
      // TODO: please consider to support vectorized reader in file group reader
      spark.sparkContext.broadcast(buildBaseFileReader(spark, options, augmentedStorageConf.unwrap(), dataStructType, enableVectorizedRead = false))
    } else {
      baseFileReader
    }

    // The relation's meta client carries the timeline the scan was planned against; build one only when the
    // format is used without a relation. The field is null rather than None on a deserialized format.
    val metaClient: HoodieTableMetaClient = Option(tableMetaClient).flatten.getOrElse(HoodieTableMetaClient
      .builder().setConf(augmentedStorageConf).setBasePath(tablePath).build)
    val broadcastedStorageConf = spark.sparkContext.broadcast(
      new SerializableConfiguration(withSchemaEvolutionConfigs(augmentedStorageConf.unwrap(), metaClient)))
    val cdcProps: TypedProperties = HoodieFileIndex.getConfigProperties(spark, options, null)
    cdcProps.setProperty(HoodieTableConfig.HOODIE_TABLE_NAME_KEY, tableName)

    val engineContext = new HoodieSparkEngineContext(new JavaSparkContext(spark.sparkContext))
    val maxMemoryPerCompaction = IOUtils.getMaxMemoryPerCompaction(engineContext.getTaskContextSupplier, options.asJava)

    val tableState = FileGroupReaderTableState.snapshotOf(metaClient, internalSchemaOpt.isPresent)
    val readerProps = TypedProperties.copy(metaClient.getTableConfig.getProps)
    options.foreach(kv => readerProps.setProperty(kv._1, kv._2))
    readerProps.put(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.key(), String.valueOf(maxMemoryPerCompaction))

    val (baseReadRequiredSchema, readVectorColumns) = withVectorRewrite(readRequiredSchema)
    val baseFileReadSchemas = if (readVectorColumns.nonEmpty) {
      val (baseOutputSchema, outputVectorColumns) = withVectorRewrite(outputSchema)
      BaseFileReadSchemas(baseReadRequiredSchema, readVectorColumns, baseOutputSchema, outputVectorColumns,
        withVectorRewrite(requestedStructType)._1)
    } else {
      BaseFileReadSchemas(baseReadRequiredSchema, readVectorColumns, outputSchema, Map.empty, requestedStructType)
    }

    val state = new HoodieFileGroupReadState(tableState, tableSchema, queryTimestamp, readerProps, cdcProps,
      dataSchema, requestedSchema, internalSchemaOpt, shouldUseRecordPosition, isCount, filters,
      requiredFilters, requiredSchema, partitionSchema, remainingPartitionSchema, fixedPartitionIndexes, outputSchema,
      requestedStructType, baseFileReadSchemas)
    new HoodieFileGroupReaderFunction(baseFileReader, fileGroupBaseFileReader, broadcastedStorageConf,
      spark.sparkContext.broadcast(JavaSerializedValue(state)))
  }

  private def buildBaseFileReader(spark: SparkSession,
                                  options: Map[String, String],
                                  configuration: Configuration,
                                  dataSchema: StructType,
                                  enableVectorizedRead: Boolean): SparkColumnarFileReader = {
    if (isMultipleBaseFileFormatsEnabled) {
      val parquetReader = sparkAdapter.createParquetFileReader(enableVectorizedRead, spark.sessionState.conf, options, configuration)
      val orcReader = sparkAdapter.createOrcFileReader(enableVectorizedRead, spark.sessionState.conf, options, configuration, dataSchema)
      val lanceReader = sparkAdapter.createLanceFileReader(enableVectorizedRead, spark.sessionState.conf, options, configuration).orNull
      new MultipleColumnarFileFormatReader(parquetReader, orcReader, lanceReader)
    } else if (hoodieFileFormat == HoodieFileFormat.PARQUET) {
      sparkAdapter.createParquetFileReader(enableVectorizedRead, spark.sessionState.conf, options, configuration)
    } else if (hoodieFileFormat == HoodieFileFormat.ORC) {
      sparkAdapter.createOrcFileReader(enableVectorizedRead, spark.sessionState.conf, options, configuration, dataSchema)
    } else if (hoodieFileFormat == HoodieFileFormat.LANCE) {
      sparkAdapter.createLanceFileReader(enableVectorizedRead, spark.sessionState.conf, options, configuration).orNull
    } else {
      throw new HoodieNotSupportedException("Unsupported file format: " + hoodieFileFormat)
    }
  }

  /**
   * Returns a copy of the conf carrying what the base file readers need to resolve each file's schema
   * under schema-on-read, or the conf itself when schema-on-read is off.
   */
  private def withSchemaEvolutionConfigs(conf: Configuration, metaClient: HoodieTableMetaClient): Configuration = {
    if (internalSchemaOpt.isPresent) {
      val readerConf = new Configuration(conf)
      SparkInternalSchemaConverter.getSchemaEvolutionReadConfigs(metaClient, validCommits).asScala
        .foreach { case (key, value) => readerConf.set(key, value) }
      readerConf
    } else {
      conf
    }
  }

  private def detectVectorColumns(schema: StructType): Map[Int, HoodieSchema.Vector] =
    SparkFileFormatInternalRowReaderContext.detectVectorColumnsFromMetadata(schema)

  private def replaceVectorFieldsWithBinary(schema: StructType, vectorCols: Map[Int, HoodieSchema.Vector]): StructType =
    SparkFileFormatInternalRowReaderContext.replaceVectorColumnsWithBinary(schema, vectorCols)

  /**
   * Detects vector columns and replaces them with BinaryType in one step.
   *
   * <p>The BinaryType rewrite is Parquet-specific: Hudi stores VECTOR columns as
   * FIXED_LEN_BYTE_ARRAY in Parquet, so the reader must see BinaryType and the raw
   * bytes are post-converted back to ArrayType. Other formats (e.g. Lance) encode
   * vectors natively as Arrow FixedSizeList and return ArrayType directly, so the
   * rewrite would introduce a spurious ArrayType→BinaryType cast during schema
   * evolution and break the read. Skip the rewrite for those formats.
   *
   * @return (modified schema with BinaryType for vectors, vector column ordinal map)
   */
  private def withVectorRewrite(schema: StructType): (StructType, Map[Int, HoodieSchema.Vector]) = {
    // Only Parquet needs the BinaryType rewrite; other formats (Lance) return ArrayType natively.
    if (hoodieFileFormat != HoodieFileFormat.PARQUET) {
      (schema, Map.empty[Int, HoodieSchema.Vector])
    } else {
      val vecs = detectVectorColumns(schema)
      if (vecs.isEmpty) {
        (schema, vecs)
      } else {
        (replaceVectorFieldsWithBinary(schema, vecs), vecs)
      }
    }
  }

  /**
   * A partition column whose name is a nested field path (e.g. "nested_record.level") cannot be
   * read from the data file as a flat top-level column, nor converted into a top-level Avro field
   * (Avro rejects '.' in names). Its value is always materialized from the partition path, so such
   * fields are treated as appended partition fields rather than read from the file.
   */
  private def isNestedPartitionField(name: String): Boolean = name.contains(".")

  override def inferSchema(sparkSession: SparkSession, options: Map[String, String], files: Seq[FileStatus]): Option[StructType] = {
    if (isMultipleBaseFileFormatsEnabled || hoodieFileFormat == HoodieFileFormat.PARQUET) {
      ParquetUtils.inferSchema(sparkSession, options, files)
    } else {
      OrcUtils.inferSchema(sparkSession, files, options)
    }
  }

  override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = {
    throw new HoodieNotSupportedException("HoodieFileGroupReaderBasedFileFormat does not support writing")
  }
}
