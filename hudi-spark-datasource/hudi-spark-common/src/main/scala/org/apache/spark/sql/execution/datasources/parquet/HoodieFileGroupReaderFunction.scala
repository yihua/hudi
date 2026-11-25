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

import org.apache.hudi.{HoodiePartitionCDCFileGroupMapping, HoodiePartitionFileSliceMapping, HoodieSchemaConversionUtils, HoodieTableSchema, SparkAdapterSupport, SparkFileFormatInternalRowReaderContext}
import org.apache.hudi.cdc.{CDCFileGroupIterator, HoodieCDCFileGroupSplit, HoodieCDCFileIndex}
import org.apache.hudi.common.config.{HoodieReaderConfig, RecordMergeMode, TypedProperties}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{FileSlice, HoodieRecord}
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.schema.internal.InternalSchema
import org.apache.hudi.common.table.{ParquetTableSchemaResolver, PartialUpdateMode}
import org.apache.hudi.common.table.log.InstantRange
import org.apache.hudi.common.table.read.{BufferedRecord, BufferedRecordMergerFactory, BufferedRecords, HoodieFileGroupReader, HoodieRecordReader, TableState}
import org.apache.hudi.common.table.read.lsm.{HoodieLsmFileGroupReader, LsmReaderUtils}
import org.apache.hudi.common.util.{ConfigUtils, HoodieRecordUtils, Option => HOption}
import org.apache.hudi.common.util.collection.ClosableIterator
import org.apache.hudi.data.CloseableIteratorListener
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.io.storage.HoodieSparkParquetReader.ENABLE_LOGICAL_TIMESTAMP_REPAIR
import org.apache.hudi.io.storage.VectorConversionUtils
import org.apache.hudi.storage.StorageConfiguration
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.schema.MessageType
import org.apache.spark.SparkEnv
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.HoodieCatalystExpressionUtils
import org.apache.spark.sql.HoodieCatalystExpressionUtils.generateUnsafeProjection
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, In, InSet, JoinedRow, Literal, Predicate, UnsafeProjection}
import org.apache.spark.sql.execution.datasources.{PartitionedFile, SparkColumnarFileReader, SparkSchemaTransformUtils}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnarBatchUtils}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{SerializableConfiguration, Utils}

import java.io.Closeable
import java.nio.ByteBuffer

import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsJavaMapConverter}
import scala.reflect.ClassTag

/**
 * Read-only state of one scan of [[HoodieFileGroupReaderBasedFileFormat]], built on the driver and shared by every
 * task of an executor through [[HoodieFileGroupReaderFunction]]. Executors only fill thread-safe lazy caches in it;
 * per-file state such as reader properties is copied before use.
 */
private[parquet] class HoodieFileGroupReadState(val tableState: TableState,
                                                 val tableSchema: HoodieTableSchema,
                                                 val queryTimestamp: String,
                                                 val readerProps: TypedProperties,
                                                 val cdcProps: TypedProperties,
                                                 val dataSchema: HoodieSchema,
                                                 val requestedSchema: HoodieSchema,
                                                 val internalSchemaOpt: HOption[InternalSchema],
                                                 val instantRangeOpt: HOption[InstantRange],
                                                 val shouldUseRecordPosition: Boolean,
                                                 val isCount: Boolean,
                                                 val filters: Seq[Filter],
                                                 val requiredFilters: Seq[Filter],
                                                 val requiredSchema: StructType,
                                                 val partitionSchema: StructType,
                                                 val remainingPartitionSchema: StructType,
                                                 val fixedPartitionIndexes: Set[Int],
                                                 val outputSchema: StructType,
                                                 val projectionInputSchema: StructType,
                                                 val baseFileReadSchemas: BaseFileReadSchemas,
                                                 val evaluateRequiredFiltersPerRow: Boolean) extends Serializable {

  /**
   * Parquet form of the table schema for base file reads. The conversion reads Hadoop's default resources, so it is
   * done once per executor rather than per task or file.
   */
  @transient lazy val tableSchemaAsMessageType: HOption[MessageType] =
    HOption.ofNullable(ParquetTableSchemaResolver.convertAvroSchemaToParquet(tableSchema.schema, new Configuration()))
}

/**
 * Base file read schemas with VECTOR columns rewritten to BinaryType, plus the ordinals of those columns.
 */
private[parquet] case class BaseFileReadSchemas(readRequiredSchema: StructType,
                                                readVectorColumns: Map[Int, HoodieSchema.Vector],
                                                outputSchema: StructType,
                                                outputVectorColumns: Map[Int, HoodieSchema.Vector],
                                                requestedSchema: StructType)

/**
 * Holds a value as Java-serialized bytes and deserializes it at most once per JVM instance of the holder.
 *
 * <p>Broadcasting this holder instead of the value keeps the broadcast independent of `spark.serializer`: Kryo
 * would otherwise serialize the value field by field and ignore the custom Java serialization of types such as
 * [[HoodieSchema]].
 */
private[parquet] class JavaSerializedValue[T: ClassTag] private(bytes: Array[Byte]) extends Serializable {

  @transient lazy val value: T = SparkEnv.get.closureSerializer.newInstance()
    .deserialize[T](ByteBuffer.wrap(bytes), Utils.getContextOrSparkClassLoader)
}

private[parquet] object JavaSerializedValue {

  def apply[T: ClassTag](value: T): JavaSerializedValue[T] = {
    val buffer = SparkEnv.get.closureSerializer.newInstance().serialize(value)
    val bytes = new Array[Byte](buffer.remaining())
    buffer.get(bytes)
    new JavaSerializedValue[T](bytes)
  }
}

/**
 * The per-file read function returned by [[HoodieFileGroupReaderBasedFileFormat.buildReaderWithPartitionValues]].
 *
 * <p>Spark deserializes this function once per task, so it holds only broadcast handles. The scan state behind
 * `broadcastedState` is deserialized once per executor.
 */
private[parquet] class HoodieFileGroupReaderFunction(baseFileReader: Broadcast[SparkColumnarFileReader],
                                                     fileGroupBaseFileReader: Broadcast[SparkColumnarFileReader],
                                                     storageConf: Broadcast[SerializableConfiguration],
                                                     broadcastedState: Broadcast[JavaSerializedValue[HoodieFileGroupReadState]])
  extends (PartitionedFile => Iterator[InternalRow]) with Serializable {

  import HoodieFileGroupReaderFunction._

  private def sparkAdapter = SparkAdapterSupport.sparkAdapter

  override def apply(file: PartitionedFile): Iterator[InternalRow] = {
    val state = broadcastedState.value.value
    val conf = new HadoopStorageConfiguration(storageConf.value.value)
    val iter = file.partitionValues match {
      // Snapshot or incremental queries.
      case fileSliceMapping: HoodiePartitionFileSliceMapping =>
        val fileGroupName = FSUtils.getFileIdFromFilePath(sparkAdapter
          .getSparkPartitionedFileUtils.getPathFromPartitionedFile(file))
        fileSliceMapping.getSlice(fileGroupName) match {
          case Some(fileSlice) if scanReturnsBatch(state) =>
            if (fileSlice.getLogFiles.findAny().isPresent) {
              readFileSliceColumnar(file, fileSlice, fileSliceMapping, state, conf)
            } else {
              readBaseFile(file, state, conf)
            }

          case Some(fileSlice) if !state.isCount && (state.requiredSchema.nonEmpty || fileSlice.getLogFiles.findAny().isPresent) =>
            readFileSliceRows(file, fileSlice, fileSliceMapping, state, conf)

          case _ =>
            readBaseFile(file, state, conf)
        }
      // CDC queries.
      case cdcFileGroupMapping: HoodiePartitionCDCFileGroupMapping =>
        // The CDC iterator hands its props to the merger factory, which writes the payload class into them
        new CDCFileGroupIterator(
          HoodieCDCFileGroupSplit(cdcFileGroupMapping.getFileSplits().toArray),
          state.tableState,
          conf,
          fileGroupBaseFileReader.value,
          state.tableSchema,
          HoodieCDCFileIndex.FULL_CDC_SPARK_SCHEMA,
          state.requiredSchema,
          TypedProperties.copy(state.cdcProps))

      case _ =>
        readBaseFile(file, state, conf)
    }
    CloseableIteratorListener.addListener(iter)
  }

  private def scanReturnsBatch(state: HoodieFileGroupReadState): Boolean =
    state.readerProps.getBoolean(HoodieColumnarMorReader.SCAN_RETURNS_BATCH_KEY, false)

  private def newReaderContext(state: HoodieFileGroupReadState,
                               conf: StorageConfiguration[Configuration]): SparkFileFormatInternalRowReaderContext = {
    // requiredFilters preserve Spark's row-level filtering semantics, while instantRangeOpt
    // keeps out-of-range records from participating in the file-group merge itself.
    val readerContext = new SparkFileFormatInternalRowReaderContext(
      fileGroupBaseFileReader.value, state.filters, state.requiredFilters, conf, state.tableState.getTableConfig,
      sparkRequiredSchema = Some(state.requiredSchema), instantRangeOpt = state.instantRangeOpt)
    readerContext.enableLogicalTimestampFieldRepair(conf.getBoolean(ENABLE_LOGICAL_TIMESTAMP_REPAIR, true))
    readerContext
  }

  /**
   * Merges a file slice row by row with the file group reader and returns rows in the scan's output schema.
   */
  private def readFileSliceRows(file: PartitionedFile,
                                fileSlice: FileSlice,
                                fileSliceMapping: HoodiePartitionFileSliceMapping,
                                state: HoodieFileGroupReadState,
                                conf: StorageConfiguration[Configuration]): Iterator[InternalRow] = {
    val tableConfig = state.tableState.getTableConfig
    val readerContext = newReaderContext(state, conf)
    // The merger factory writes the payload class into the props it is given, so each file gets its own copy
    val props = TypedProperties.copy(state.readerProps)
    val baseFileLength = if (fileSlice.getBaseFile.isPresent) {
      fileSlice.getBaseFile.get.getFileSize
    } else {
      0
    }
    val reader: HoodieRecordReader[InternalRow] =
      if (LsmReaderUtils.shouldUseLsmReader(
        tableConfig,
        ConfigUtils.getStringWithAltKeys(props, HoodieReaderConfig.MERGE_TYPE, true))) {
        HoodieLsmFileGroupReader.builder[InternalRow]()
          .withReaderContext(readerContext)
          .withTableState(state.tableState)
          .withLatestCommitTime(state.queryTimestamp)
          .withBaseFileOption(fileSlice.getBaseFile)
          .withLogFiles(fileSlice.getLogFiles)
          .withPartitionPath(fileSlice.getPartitionPath)
          .withDataSchema(state.dataSchema)
          .withRequestedSchema(state.requestedSchema)
          .withInternalSchemaOpt(state.internalSchemaOpt)
          .withProps(props)
          .withStart(file.start)
          .withLength(baseFileLength)
          .build()
      } else {
        HoodieFileGroupReader.builder[InternalRow]()
          .withReaderContext(readerContext)
          .withTableState(state.tableState)
          .withLatestCommitTime(state.queryTimestamp)
          .withBaseFileOption(fileSlice.getBaseFile)
          .withLogFiles(fileSlice.getLogFiles)
          .withPartitionPath(fileSlice.getPartitionPath)
          .withDataSchema(state.dataSchema)
          .withRequestedSchema(state.requestedSchema)
          .withInternalSchemaOpt(state.internalSchemaOpt)
          .withProps(props)
          .withStart(file.start)
          .withLength(baseFileLength)
          .withShouldUseRecordPosition(state.shouldUseRecordPosition)
          .build()
      }
    // Append partition values to rows and project to output schema
    appendPartitionAndProject(
      reader.getClosableIterator,
      state.projectionInputSchema,
      state.remainingPartitionSchema,
      state.outputSchema,
      fileSliceMapping.getPartitionValues,
      state.fixedPartitionIndexes)
  }

  /**
   * Reads a file slice with log files as columnar batches for a scan that returns batches: the base file through
   * the vectorized reader, merged with the slice's log records by [[HoodieColumnarMorReader]]. A slice that the
   * columnar merge does not cover (skip-merge reads, the LSM layout, more log records than
   * [[HoodieColumnarMorReader.MAX_LOG_RECORDS_KEY]] allows) is merged row by row and then batched.
   */
  private def readFileSliceColumnar(file: PartitionedFile,
                                    fileSlice: FileSlice,
                                    fileSliceMapping: HoodiePartitionFileSliceMapping,
                                    state: HoodieFileGroupReadState,
                                    conf: StorageConfiguration[Configuration]): Iterator[InternalRow] = {
    def rowsAsBatches(): Iterator[InternalRow] = HoodieColumnarMorReader
      .toBatches(readFileSliceRows(file, fileSlice, fileSliceMapping, state, conf), state.outputSchema)
      .asInstanceOf[Iterator[InternalRow]]

    val tableConfig = state.tableState.getTableConfig
    val props = TypedProperties.copy(state.readerProps)
    val mergeType = ConfigUtils.getStringWithAltKeys(props, HoodieReaderConfig.MERGE_TYPE, true)
    if (mergeType.equalsIgnoreCase(HoodieReaderConfig.REALTIME_SKIP_MERGE) || LsmReaderUtils.shouldUseLsmReader(tableConfig, mergeType)) {
      rowsAsBatches()
    } else {
      val readerContext = newReaderContext(state, conf)
      loadLogRecords(fileSlice, state, readerContext, props) match {
        case None => rowsAsBatches()
        case Some(byKey) =>
          val mergeMode = readerContext.getMergeMode
          val orderingFieldNames = HoodieRecordUtils.getOrderingFieldNames(mergeMode, tableConfig).asScala.toArray
          val dataStructType = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(state.dataSchema)
          val extraFieldNames = (HoodieRecord.RECORD_KEY_METADATA_FIELD +: orderingFieldNames).distinct
            .filterNot(name => state.requiredSchema.fieldNames.contains(name))
          val extraFields = extraFieldNames.flatMap(name => dataStructType.fields.find(_.name == name))
          if (extraFields.length != extraFieldNames.length) {
            rowsAsBatches()
          } else {
            mergeColumnar(file, fileSlice, fileSliceMapping, state, conf, readerContext, props, byKey, mergeMode,
              orderingFieldNames, StructType(state.requiredSchema.fields ++ extraFields))
          }
      }
    }
  }

  /**
   * Reads the log records of a file slice keyed by record key, or None when there are more than the columnar
   * merge takes.
   */
  private def loadLogRecords(fileSlice: FileSlice,
                             state: HoodieFileGroupReadState,
                             readerContext: SparkFileFormatInternalRowReaderContext,
                             props: TypedProperties): Option[java.util.HashMap[UTF8String, BufferedRecord[InternalRow]]] = {
    // Log records only: the file group reader is given no base file, so it buffers the log records by key.
    val logReader = HoodieFileGroupReader.builder[InternalRow]()
      .withReaderContext(readerContext)
      .withTableState(state.tableState)
      .withLatestCommitTime(state.queryTimestamp)
      .withBaseFileOption(HOption.empty())
      .withLogFiles(fileSlice.getLogFiles)
      .withPartitionPath(fileSlice.getPartitionPath)
      .withDataSchema(state.dataSchema)
      .withRequestedSchema(state.requestedSchema)
      .withInternalSchemaOpt(state.internalSchemaOpt)
      .withProps(props)
      .build()
    val maxLogRecords = HoodieColumnarMorReader.maxLogRecords(state)
    val byKey = new java.util.HashMap[UTF8String, BufferedRecord[InternalRow]]()
    var tooManyLogRecords = false
    val logRecordIterator = logReader.getLogRecordsOnly
    try {
      while (!tooManyLogRecords && logRecordIterator.hasNext) {
        val record = logRecordIterator.next()
        if (byKey.size() >= maxLogRecords) {
          tooManyLogRecords = true
        } else {
          byKey.put(UTF8String.fromString(record.getRecordKey), record)
        }
      }
    } finally {
      logRecordIterator.close()
      logReader.close()
    }
    if (tooManyLogRecords) None else Some(byKey)
  }

  /**
   * Merges the base file batches of a file slice with its log records, reading the base file in `baseReadSchema`.
   */
  private def mergeColumnar(file: PartitionedFile,
                            fileSlice: FileSlice,
                            fileSliceMapping: HoodiePartitionFileSliceMapping,
                            state: HoodieFileGroupReadState,
                            conf: StorageConfiguration[Configuration],
                            readerContext: SparkFileFormatInternalRowReaderContext,
                            props: TypedProperties,
                            byKey: java.util.HashMap[UTF8String, BufferedRecord[InternalRow]],
                            mergeMode: RecordMergeMode,
                            orderingFieldNames: Array[String],
                            baseReadSchema: StructType): Iterator[InternalRow] = {
    val readerSchema = readerContext.getSchemaHandler.getRequiredSchema
    val merger = BufferedRecordMergerFactory.create(readerContext, mergeMode, false, readerContext.getRecordMerger(),
      readerSchema, readerContext.getPayloadClasses(props), props, HOption.empty[PartialUpdateMode]())
    val logRecords = HoodieColumnarMorReader.LogRecords(byKey,
      HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(readerSchema), merger, orderingFieldNames, mergeMode)

    val (baseBatches, baseCloseables) = if (fileSlice.getBaseFile.isPresent) {
      // A base row a filter skips under event-time ordering may still beat its log record, so the filters
      // only prune the base file under commit-time ordering, where the log record always wins.
      val baseFilters = if (mergeMode == RecordMergeMode.COMMIT_TIME_ORDERING) state.filters else Seq.empty
      val iter = baseFileReader.value.read(file, baseReadSchema, state.partitionSchema, state.internalSchemaOpt,
        baseFilters, conf, state.tableSchemaAsMessageType)
      val batches = iter.asInstanceOf[Iterator[Any]].map {
        case batch: ColumnarBatch => batch
        case other => throw new IllegalStateException(
          s"Expected columnar batches from the base file of a columnar merge-on-read scan, got ${other.getClass.getName}")
      }
      (batches, iter match {
        case closeable: Closeable => Seq(closeable)
        case _ => Seq.empty
      })
    } else {
      (Iterator.empty, Seq.empty)
    }

    val baseStructWithPartitions = StructType(baseReadSchema.fields ++ state.partitionSchema.fields)
    lazy val baseSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(baseStructWithPartitions, "base_row")
    val recordContext = readerContext.getRecordContext
    val toBufferedRecord: (InternalRow, StructType) => BufferedRecord[InternalRow] = (row, _) =>
      BufferedRecords.fromEngineRecord[InternalRow](row, baseSchema, recordContext, orderingFieldNames, null, false)

    HoodieColumnarMorReader.merge(baseBatches, baseReadSchema, state.requiredSchema, state.partitionSchema,
      fileSliceMapping.getPartitionValues, logRecords, toBufferedRecord, baseCloseables)
      .asInstanceOf[Iterator[InternalRow]]
  }

  private def readBaseFile(file: PartitionedFile,
                           state: HoodieFileGroupReadState,
                           conf: StorageConfiguration[Configuration]): Iterator[InternalRow] = {
    val schemas = state.baseFileReadSchemas
    val hasVectors = schemas.readVectorColumns.nonEmpty
    val parquetFileReader = baseFileReader.value
    val filters = state.filters ++ state.requiredFilters
    val requiredFilters = if (state.evaluateRequiredFiltersPerRow) state.requiredFilters else Seq.empty
    val partitionSchema = state.partitionSchema
    val remainingPartitionSchema = state.remainingPartitionSchema

    val rawIter = if (remainingPartitionSchema.fields.length == partitionSchema.fields.length) {
      //none of partition fields are read from the file, so the reader will do the appending for us
      val iter = parquetFileReader.read(file, schemas.readRequiredSchema, partitionSchema, state.internalSchemaOpt, filters, conf,
        state.tableSchemaAsMessageType)
      val readSchema = StructType(schemas.readRequiredSchema.fields ++ partitionSchema.fields)
      projectIfNeeded(applyRequiredFilters(iter, readSchema, requiredFilters), readSchema, schemas.outputSchema)
    } else if (remainingPartitionSchema.fields.length == 0) {
      //we read all of the partition fields from the file
      val pfileUtils = sparkAdapter.getSparkPartitionedFileUtils
      //we need to modify the partitioned file so that the partition values are empty
      val modifiedFile = pfileUtils.createPartitionedFile(InternalRow.empty, pfileUtils.getPathFromPartitionedFile(file), file.start, file.length)
      val readSchema = StructType(schemas.readRequiredSchema.fields ++ partitionSchema.fields)
      //and we pass an empty schema for the partition schema
      val iter = parquetFileReader.read(modifiedFile, readSchema, new StructType(), state.internalSchemaOpt, filters, conf,
        state.tableSchemaAsMessageType)
      projectIfNeeded(applyRequiredFilters(iter, readSchema, requiredFilters), readSchema, schemas.outputSchema)
    } else {
      //need to do an additional projection here. The case in mind is that partition schema is "a,b,c" mandatoryFields is "a,c",
      //then we will read (dataSchema + a + c) and append b. So the final schema will be (data schema + a + c +b)
      //but expected output is (data schema + a + b + c)
      val pfileUtils = sparkAdapter.getSparkPartitionedFileUtils
      val partitionValues = getFixedPartitionValues(file.partitionValues, partitionSchema, state.fixedPartitionIndexes)
      val modifiedFile = pfileUtils.createPartitionedFile(partitionValues, pfileUtils.getPathFromPartitionedFile(file), file.start, file.length)
      val iter = parquetFileReader.read(modifiedFile, schemas.requestedSchema, remainingPartitionSchema, state.internalSchemaOpt, filters, conf,
        state.tableSchemaAsMessageType)
      val readSchema = StructType(schemas.requestedSchema.fields ++ remainingPartitionSchema.fields)
      projectIter(applyRequiredFilters(iter, readSchema, requiredFilters), readSchema, schemas.outputSchema)
    }

    if (hasVectors) {
      // The raw iterator has BinaryType for vector columns; convert back to ArrayType.
      // All branches above produce rows in the rewritten output schema: filter-only columns from
      // readRequiredSchema are projected away by projectIfNeeded/projectIter.
      wrapWithVectorConversion(rawIter, schemas.outputSchema, state.outputSchema, schemas.outputVectorColumns)
    } else {
      rawIter
    }
  }
}

private[parquet] object HoodieFileGroupReaderFunction {

  private def appendPartitionAndProject(iter: ClosableIterator[InternalRow],
                                        inputSchema: StructType,
                                        partitionSchema: StructType,
                                        to: StructType,
                                        partitionValues: InternalRow,
                                        fixedPartitionIndexes: Set[Int]): Iterator[InternalRow] = {
    if (partitionSchema.isEmpty) {
      //'inputSchema' and 'to' should be the same so the projection will just be an identity func
      projectSchema(iter, inputSchema, to)
    } else {
      val fixedPartitionValues = if (partitionSchema.length == partitionValues.numFields) {
        //need to append all of the partition fields
        partitionValues
      } else {
        //some partition fields read from file, some were not
        getFixedPartitionValues(partitionValues, partitionSchema, fixedPartitionIndexes)
      }
      val unsafeProjection = generateOutputProjection(StructType(inputSchema.fields ++ partitionSchema.fields), to)
      val joinedRow = new JoinedRow()
      makeCloseableFileGroupMappingRecordIterator(iter, d => unsafeProjection(joinedRow(d, fixedPartitionValues)))
    }
  }

  private def projectSchema(iter: ClosableIterator[InternalRow],
                            from: StructType,
                            to: StructType): Iterator[InternalRow] = {
    val unsafeProjection = generateOutputProjection(from, to)
    makeCloseableFileGroupMappingRecordIterator(iter, d => unsafeProjection(d))
  }

  /**
   * The scan's output projection. Stays the by-name top-level projection unless a column comes out of the
   * reader with nested fields Spark did not ask for (see `projectionInputSchema` in
   * buildReaderWithPartitionValues), in which case those are dropped by name at every depth.
   */
  private def generateOutputProjection(from: StructType, to: StructType): UnsafeProjection = {
    val hasWiderNestedInput = to.fields.exists { f =>
      from.getFieldIndex(f.name).exists(i => SparkSchemaTransformUtils.needsNestedPruning(from.fields(i).dataType, f.dataType))
    }
    if (hasWiderNestedInput) {
      SparkSchemaTransformUtils.generateNestedPruningProjection(from, to)
    } else {
      generateUnsafeProjection(from, to)
    }
  }

  private def makeCloseableFileGroupMappingRecordIterator(closeableFileGroupRecordIterator: ClosableIterator[InternalRow],
                                                          mappingFunction: Function[InternalRow, InternalRow]): Iterator[InternalRow] = {
    CloseableIteratorListener.addListener(closeableFileGroupRecordIterator)
    new Iterator[InternalRow] with Closeable {
      override def hasNext: Boolean = closeableFileGroupRecordIterator.hasNext

      override def next(): InternalRow = mappingFunction(closeableFileGroupRecordIterator.next())

      override def close(): Unit = closeableFileGroupRecordIterator.close()
    }
  }

  /**
   * Wraps an iterator to convert binary VECTOR columns back to typed arrays.
   * The read schema has BinaryType for vector columns; the target schema has ArrayType.
   */
  private def wrapWithVectorConversion(iter: Iterator[InternalRow],
                                       readSchema: StructType,
                                       targetSchema: StructType,
                                       vectorCols: Map[Int, HoodieSchema.Vector]): Iterator[InternalRow] = {
    val vectorProjection = UnsafeProjection.create(targetSchema)
    val javaVectorCols: java.util.Map[Integer, HoodieSchema.Vector] =
      vectorCols.map { case (k, v) => (Integer.valueOf(k), v) }.asJava
    val mapper = VectorConversionUtils.buildRowMapper(readSchema, javaVectorCols, vectorProjection.apply(_))
    iter.map(mapper.apply(_))
  }

  /**
   * Evaluates `requiredFilters` on every row. Pushed-down filters only prune row groups in the vectorized reader,
   * so when the plan does not filter on them (the Hudi session extension is not registered), the commit-time
   * range of an incremental query is enforced here. The scan returns rows rather than batches in that case.
   */
  private def applyRequiredFilters(iter: Iterator[InternalRow],
                                   schema: StructType,
                                   requiredFilters: Seq[Filter]): Iterator[InternalRow] = {
    if (requiredFilters.isEmpty) {
      iter
    } else {
      val attrs = SparkAdapterSupport.sparkAdapter.getSchemaUtils.toAttributes(schema)
      val condition = requiredFilters.map { filter =>
        HoodieCatalystExpressionUtils.convertToCatalystExpression(filter, schema).getOrElse(
          throw new HoodieException(s"Cannot evaluate required filter: $filter"))
      }.reduce(And).transform {
        case a: AttributeReference => attrs(schema.fieldIndex(a.name))
      }.transform {
        // A set lookup rather than a chain of comparisons for the commit times of an incremental range
        case In(child, list) if list.forall(_.isInstanceOf[Literal]) && list.nonEmpty =>
          InSet(child, list.map(_.eval()).toSet)
      }
      val predicate = Predicate.create(condition, attrs)
      predicate.initialize(0)
      iter.asInstanceOf[Iterator[Any]].filter {
        case _: ColumnarBatch => throw new IllegalStateException("Required filters cannot be evaluated on columnar batches")
        case row: InternalRow => predicate.eval(row)
      }.asInstanceOf[Iterator[InternalRow]]
    }
  }

  private def projectIter(iter: Iterator[Any], from: StructType, to: StructType): Iterator[InternalRow] = {
    val unsafeProjection = generateUnsafeProjection(from, to)
    val batchProjection = ColumnarBatchUtils.generateProjection(from, to)
    iter.map {
      case ir: InternalRow => unsafeProjection(ir)
      case cb: ColumnarBatch => batchProjection(cb)
    }.asInstanceOf[Iterator[InternalRow]]
  }

  /**
   * Projects to `to` only when the read schema was augmented with filter-only columns;
   * otherwise returns the iterator as is, preserving columnar batches.
   */
  private def projectIfNeeded(iter: Iterator[InternalRow], from: StructType, to: StructType): Iterator[InternalRow] = {
    if (from.fieldNames.sameElements(to.fieldNames)) {
      iter
    } else {
      projectIter(iter, from, to)
    }
  }

  private def getFixedPartitionValues(allPartitionValues: InternalRow, partitionSchema: StructType, fixedPartitionIndexes: Set[Int]): InternalRow = {
    InternalRow.fromSeq(allPartitionValues.toSeq(partitionSchema).zipWithIndex.filter(p => fixedPartitionIndexes.contains(p._2)).map(p => p._1))
  }
}
