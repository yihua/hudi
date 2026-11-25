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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.common.config.RecordMergeMode
import org.apache.hudi.common.model.{DefaultHoodieRecordPayload, HoodieRecord, OverwriteWithLatestAvroPayload}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.table.read.{BufferedRecord, BufferedRecordMerger}
import org.apache.hudi.common.util.HoodieRecordUtils
import org.apache.hudi.sql.vectorized.{RowBatchWriter, RowIdMappedColumnVector}

import org.apache.spark.sql.HoodieCatalystExpressionUtils.generateUnsafeProjection
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.types.UTF8String

import java.io.Closeable
import java.util

import scala.collection.JavaConverters._

/**
 * Columnar read of merge-on-read file slices whose log records merge by commit time or event time.
 *
 * The base file is read with the vectorized Parquet reader. Each base batch keeps the rows that have no
 * log record as a row-id view of the batch's own vectors, so untouched rows are never copied or turned
 * into rows. The few rows a log record replaces, and log records with no base row, are written into
 * separate small batches. Custom mergers and payloads that need the whole row are not admitted here
 * and keep the row-based file group reader.
 */
private[parquet] object HoodieColumnarMorReader extends SparkAdapterSupport {

  /** Turns the columnar merge-on-read read off; it is on by default where it applies. */
  val ENABLED_KEY = "hoodie.datasource.read.mor.columnar.enabled"

  /**
   * Set by the planner in the reader props when the scan returns batches, so every file slice of the
   * scan must be returned as columnar batches.
   */
  val SCAN_RETURNS_BATCH_KEY = "hoodie.datasource.read.mor.columnar.scan.batch"

  /** Above this many log records in one file slice, the slice is merged row by row and then batched. */
  val MAX_LOG_RECORDS_KEY = "hoodie.datasource.read.mor.columnar.max.log.records"
  private val DEFAULT_MAX_LOG_RECORDS = 2000000

  private val OUTPUT_BATCH_ROWS = 4096

  private val SUPPORTED_PAYLOADS = Set(
    classOf[OverwriteWithLatestAvroPayload].getName,
    classOf[DefaultHoodieRecordPayload].getName)

  def isEnabled(spark: SparkSession): Boolean =
    spark.conf.getOption(ENABLED_KEY).forall(_.toBoolean)

  /**
   * Whether the table's log records can be merged into columnar base batches. Only commit-time and
   * event-time ordering qualify, because they pick one whole record by commit order or by the ordering
   * value and never read the other columns. Everything that needs the full row to merge stays on the
   * existing row-based file group reader, unchanged: custom merge mode and custom mergers, any payload
   * class other than the two built-in ones these modes stand for, and partial updates. Tables without
   * stored record keys, with nested ordering fields, or with partition columns read from the data files
   * stay row based as well.
   */
  def isTableSupported(tableConfig: HoodieTableConfig, mandatoryFields: Seq[String]): Boolean =
    !needsFullRowMerge(tableConfig) &&
      tableConfig.populateMetaFields() &&
      HoodieRecordUtils.getOrderingFieldNames(tableConfig.getRecordMergeMode, tableConfig).asScala.forall(!_.contains(".")) &&
      !partitionFieldsOf(tableConfig).exists(mandatoryFields.contains)

  /**
   * Whether merging a log record into a base record needs more than the record key and ordering value.
   */
  def needsFullRowMerge(tableConfig: HoodieTableConfig): Boolean = {
    val mergeMode = tableConfig.getRecordMergeMode
    val payload = Option(tableConfig.getPayloadClass).filter(_.nonEmpty)
    !(mergeMode == RecordMergeMode.COMMIT_TIME_ORDERING || mergeMode == RecordMergeMode.EVENT_TIME_ORDERING) ||
      !payload.forall(SUPPORTED_PAYLOADS.contains) ||
      tableConfig.getPartialUpdateMode.isPresent
  }

  private def partitionFieldsOf(tableConfig: HoodieTableConfig): Seq[String] =
    if (tableConfig.getPartitionFields.isPresent) tableConfig.getPartitionFields.get().toSeq else Seq.empty

  def maxLogRecords(state: HoodieFileGroupReadState): Int =
    state.readerProps.getInteger(MAX_LOG_RECORDS_KEY, DEFAULT_MAX_LOG_RECORDS)

  /**
   * Splits rows already in the scan's output schema into batches, for slices that cannot take the columnar
   * merge but belong to a scan that returns batches.
   */
  def toBatches(rows: Iterator[InternalRow], outputSchema: StructType): Iterator[ColumnarBatch] = {
    rows.grouped(OUTPUT_BATCH_ROWS).map { group =>
      val copied = new util.ArrayList[InternalRow](group.size)
      group.foreach(r => copied.add(r.copy()))
      sparkAdapter.makeColumnarBatch(RowBatchWriter.write(outputSchema, copied), copied.size())
    }
  }

  /**
   * Log records of one file slice, keyed by record key, with the schema their rows are laid out in.
   */
  case class LogRecords(byKey: util.HashMap[UTF8String, BufferedRecord[InternalRow]],
                        readerStructType: StructType,
                        merger: BufferedRecordMerger[InternalRow],
                        orderingFieldNames: Array[String],
                        mergeMode: RecordMergeMode)

  /**
   * Merges the base file batches of one file slice with its log records.
   *
   * @param baseBatches       batches of the base file in `baseReadSchema` followed by the partition columns,
   *                          empty when the slice has no base file
   * @param baseReadSchema    the scan's required schema followed by the record key and any ordering field
   *                          it lacks, in that order
   * @param requiredSchema    the scan's required schema
   * @param partitionSchema   partition columns appended to every output row
   * @param partitionValues   this slice's partition values
   * @param recordContextOps  extracts the ordering value of a base row for an event-time merge
   */
  def merge(baseBatches: Iterator[ColumnarBatch],
            baseReadSchema: StructType,
            requiredSchema: StructType,
            partitionSchema: StructType,
            partitionValues: InternalRow,
            logRecords: LogRecords,
            recordContextOps: (InternalRow, StructType) => BufferedRecord[InternalRow],
            closeables: Seq[Closeable]): Iterator[ColumnarBatch] with Closeable = {
    val outputSchema = StructType(requiredSchema.fields ++ partitionSchema.fields)
    val keyOrdinal = baseReadSchema.fieldIndex(HoodieRecord.RECORD_KEY_METADATA_FIELD)
    val baseStructWithPartitions = StructType(baseReadSchema.fields ++ partitionSchema.fields)
    // Output columns of a base batch: the required columns, then the partition columns after the extra ones
    val outputOrdinals: Array[Int] = (requiredSchema.indices ++
      partitionSchema.indices.map(_ + baseReadSchema.length)).toArray
    val logToRequired = generateUnsafeProjection(logRecords.readerStructType, requiredSchema)
    val joined = new JoinedRow()
    val pendingRows = new util.ArrayList[InternalRow]()

    def toOutputRow(logRow: InternalRow): InternalRow =
      joined(logToRequired(logRow), partitionValues).copy()

    def flushRows(): ColumnarBatch = {
      val batch = sparkAdapter.makeColumnarBatch(RowBatchWriter.write(outputSchema, pendingRows), pendingRows.size())
      pendingRows.clear()
      batch
    }

    val commitTimeOrdering = logRecords.mergeMode == RecordMergeMode.COMMIT_TIME_ORDERING
    new Iterator[ColumnarBatch] with Closeable {
      private val ready = new util.ArrayDeque[ColumnarBatch]()
      private var leftoverDone = false

      override def hasNext: Boolean = {
        while (ready.isEmpty && !(leftoverDone && pendingRows.isEmpty)) {
          if (baseBatches.hasNext) {
            mergeBaseBatch(baseBatches.next())
            if (pendingRows.size() >= OUTPUT_BATCH_ROWS) {
              ready.add(flushRows())
            }
          } else if (!leftoverDone) {
            // Log records left over have no base row: emit the ones that are not deletes.
            logRecords.byKey.values().asScala.foreach { record =>
              if (!record.isDelete && record.getRecord != null) {
                pendingRows.add(toOutputRow(record.getRecord))
                if (pendingRows.size() >= OUTPUT_BATCH_ROWS) {
                  ready.add(flushRows())
                }
              }
            }
            logRecords.byKey.clear()
            leftoverDone = true
          } else {
            ready.add(flushRows())
          }
        }
        !ready.isEmpty
      }

      override def next(): ColumnarBatch = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        ready.poll()
      }

      private def mergeBaseBatch(batch: ColumnarBatch): Unit = {
        val numRows = batch.numRows()
        val keyVector = batch.column(keyOrdinal)
        var keep: Array[Int] = null
        var kept = 0
        var i = 0
        while (i < numRows) {
          val logRecord = if (keyVector.isNullAt(i)) null else logRecords.byKey.remove(keyVector.getUTF8String(i))
          val keepBase = logRecord == null || {
            // Under commit-time ordering a log record always supersedes the base row
            val baseRecord = if (commitTimeOrdering) null else recordContextOps(batch.getRow(i), baseStructWithPartitions)
            val result = if (commitTimeOrdering) logRecord else logRecords.merger.finalMerge(baseRecord, logRecord)
            if (baseRecord != null && (result eq baseRecord)) {
              true
            } else {
              if (!result.isDelete && result.getRecord != null) {
                pendingRows.add(toOutputRow(result.getRecord))
              }
              false
            }
          }
          if (keepBase) {
            if (keep != null) {
              keep(kept) = i
            }
            kept += 1
          } else if (keep == null) {
            // First row dropped from this batch: start tracking the kept row ids
            keep = new Array[Int](numRows)
            var j = 0
            while (j < kept) {
              keep(j) = j
              j += 1
            }
          }
          i += 1
        }
        if (kept > 0) {
          val vectors: Array[ColumnVector] = outputOrdinals.map { ordinal =>
            val column = batch.column(ordinal)
            if (keep == null) column else new RowIdMappedColumnVector(column, keep, kept)
          }
          ready.add(sparkAdapter.makeColumnarBatch(vectors, kept))
        }
      }

      override def close(): Unit = {
        closeables.foreach(_.close())
      }
    }
  }
}
