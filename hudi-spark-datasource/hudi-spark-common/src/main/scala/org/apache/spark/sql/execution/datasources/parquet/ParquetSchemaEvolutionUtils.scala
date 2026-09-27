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

import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.client.utils.SparkInternalSchemaConverter
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.util
import org.apache.hudi.common.util.{InternalSchemaCache, InternalSchemaHistory}
import org.apache.hudi.common.util.collection.Pair
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.internal.schema.action.InternalSchemaMerger
import org.apache.hudi.internal.schema.utils.InternalSchemaUtils
import org.apache.hudi.storage.HoodieStorageUtils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.spark.sql.HoodieSchemaUtils
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, UnsafeProjection}
import org.apache.spark.sql.execution.datasources.SparkSchemaTransformUtils
import org.apache.spark.sql.execution.datasources.parquet.ParquetSchemaEvolutionUtils.pruneInternalSchema
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{AtomicType, DataType, StructType}

import java.time.ZoneId

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class ParquetSchemaEvolutionUtils(readConf: Configuration,
                                  filePath: Path,
                                  requiredSchema: StructType,
                                  partitionSchema: StructType,
                                  internalSchemaOpt: util.Option[InternalSchema]) extends SparkAdapterSupport {
  // Fetch internal schema
  private lazy val querySchemaOption: util.Option[InternalSchema] = pruneInternalSchema(internalSchemaOpt, requiredSchema)

  var shouldUseInternalSchema: Boolean = querySchemaOption.isPresent && tablePath != null

  private lazy val schemaUtils: HoodieSchemaUtils = sparkAdapter.getSchemaUtils

  private lazy val tablePath: String = readConf.get(SparkInternalSchemaConverter.HOODIE_TABLE_PATH)
  private lazy val fileSchema: InternalSchema = if (shouldUseInternalSchema) {
    ParquetSchemaEvolutionUtils.getFileSchema(readConf, tablePath, filePath)
  } else {
    null
  }

  def rebuildFilterFromParquet(filter: Filter): Filter = {
    rebuildFilterFromParquetHelper(filter, fileSchema, querySchemaOption.orElse(null))
  }

  private def rebuildFilterFromParquetHelper(oldFilter: Filter, fileSchema: InternalSchema, querySchema: InternalSchema): Filter = {
    if (fileSchema == null || querySchema == null) {
      oldFilter
    } else {
      oldFilter match {
        case eq: EqualTo =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(eq.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else eq.copy(attribute = newAttribute)
        case eqs: EqualNullSafe =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(eqs.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else eqs.copy(attribute = newAttribute)
        case gt: GreaterThan =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(gt.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else gt.copy(attribute = newAttribute)
        case gtr: GreaterThanOrEqual =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(gtr.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else gtr.copy(attribute = newAttribute)
        case lt: LessThan =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(lt.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else lt.copy(attribute = newAttribute)
        case lte: LessThanOrEqual =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(lte.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else lte.copy(attribute = newAttribute)
        case i: In =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(i.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else i.copy(attribute = newAttribute)
        case isn: IsNull =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(isn.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else isn.copy(attribute = newAttribute)
        case isnn: IsNotNull =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(isnn.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else isnn.copy(attribute = newAttribute)
        case And(left, right) =>
          And(rebuildFilterFromParquetHelper(left, fileSchema, querySchema), rebuildFilterFromParquetHelper(right, fileSchema, querySchema))
        case Or(left, right) =>
          Or(rebuildFilterFromParquetHelper(left, fileSchema, querySchema), rebuildFilterFromParquetHelper(right, fileSchema, querySchema))
        case Not(child) =>
          Not(rebuildFilterFromParquetHelper(child, fileSchema, querySchema))
        case ssw: StringStartsWith =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(ssw.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else ssw.copy(attribute = newAttribute)
        case ses: StringEndsWith =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(ses.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else ses.copy(attribute = newAttribute)
        case sc: StringContains =>
          val newAttribute = InternalSchemaUtils.reBuildFilterName(sc.attribute, fileSchema, querySchema)
          if (newAttribute.isEmpty) AlwaysTrue else sc.copy(attribute = newAttribute)
        case AlwaysTrue =>
          AlwaysTrue
        case AlwaysFalse =>
          AlwaysFalse
        case _ =>
          AlwaysTrue
      }
    }
  }

  protected var typeChangeInfos: java.util.Map[Integer, Pair[DataType, DataType]] = null

  /**
   * Sets the file's requested read schema on the read configuration and returns it for the task
   * attempt context. The configuration belongs to this read only (see [[SparkParquetReaderBase.read]]),
   * so it is updated in place. Also records the file's type changes.
   *
   * @param requireVectorizedRead true when the caller can only read the file vectorized (it returns
   *                              batches), so a nested type change fails here
   */
  def getHadoopAttemptConf(footerFileMetaData: FileMetaData, requireVectorizedRead: Boolean): Configuration = {
    val hadoopAttemptConf = readConf
    typeChangeInfos = if (shouldUseInternalSchema && requiredSchema.isEmpty) {
      // Empty projections (count(*), select 1) read no column data, so the requested schema stays
      // empty. querySchemaOption is the UNPRUNED table schema in that case (see pruneInternalSchema);
      // requesting it would decode every column, and the vectorized reader would fail on a nested
      // type change in a column the query never reads.
      new java.util.HashMap[Integer, Pair[DataType, DataType]]()
    } else if (shouldUseInternalSchema) {
      val mergedInternalSchema = new InternalSchemaMerger(fileSchema, querySchemaOption.get(), true, true).mergeSchema()
      val mergedSchema = SparkInternalSchemaConverter.constructSparkSchemaFromInternalSchema(mergedInternalSchema)

      hadoopAttemptConf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, mergedSchema.json)

      SparkInternalSchemaConverter.collectTypeChangedCols(querySchemaOption.get(), mergedInternalSchema)
    } else {
      val (implicitTypeChangeInfo, sparkRequestSchema) = HoodieParquetFileFormatHelper.buildImplicitSchemaChangeInfo(hadoopAttemptConf, footerFileMetaData, requiredSchema)
      if (!implicitTypeChangeInfo.isEmpty) {
        shouldUseInternalSchema = true
        hadoopAttemptConf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, sparkRequestSchema.json)
      }
      implicitTypeChangeInfo
    }

    if (requireVectorizedRead && !canReadVectorized) {
      throw new IllegalArgumentException(
        "Nested types with type changes(implicit or explicit) cannot be read in vectorized mode. " +
          "To workaround this issue, set spark.sql.parquet.enableVectorizedReader=false.")
    }

    hadoopAttemptConf
  }

  /**
   * Whether the vectorized reader can decode this file: a nested type change (implicit or
   * explicit) needs the row-based reader. Valid after [[getHadoopAttemptConf]].
   */
  private def canReadVectorized: Boolean =
    !shouldUseInternalSchema || typeChangeInfos.values().forall(_.getLeft.isInstanceOf[AtomicType])

  /**
   * Whether this file needs a type conversion (implicit or schema-on-read). A reader that returns
   * rows reads such a file row-based, where Cast converts the values; the vectorized reader's own
   * conversions cover fewer types. Valid after [[getHadoopAttemptConf]].
   */
  def hasTypeChange: Boolean = !typeChangeInfos.isEmpty

  def generateUnsafeProjection(fullSchema: Seq[AttributeReference], timeZoneId: Option[String]): UnsafeProjection = {
    SparkSchemaTransformUtils.generateUnsafeProjection(fullSchema, timeZoneId, typeChangeInfos, requiredSchema, partitionSchema, schemaUtils)
  }

  def buildVectorizedReader(convertTz: ZoneId,
                            datetimeRebaseMode: String,
                            datetimeRebaseTz: String,
                            int96RebaseMode: String,
                            int96RebaseTz: String,
                            useOffHeap: Boolean,
                            capacity: Int): VectorizedParquetRecordReader = {
    if (shouldUseInternalSchema) {
      new HoodieVectorizedParquetRecordReader(
        convertTz,
        datetimeRebaseMode,
        datetimeRebaseTz,
        int96RebaseMode,
        int96RebaseTz,
        useOffHeap,
        capacity,
        typeChangeInfos)
    } else {
      new VectorizedParquetRecordReader(
        convertTz,
        datetimeRebaseMode,
        datetimeRebaseTz,
        int96RebaseMode,
        int96RebaseTz,
        useOffHeap,
        capacity)
    }
  }
}

object ParquetSchemaEvolutionUtils {

  /**
   * Returns the schema the base file was written with, resolved from the schema history in the conf
   * (see [[SparkInternalSchemaConverter.getSchemaEvolutionReadConfigs]]) or, when the conf holds none,
   * from the table's timeline.
   */
  def getFileSchema(conf: Configuration, tablePath: String, filePath: Path): InternalSchema = {
    val versionId = FSUtils.getCommitTime(filePath.getName).toLong
    val getConfig = new java.util.function.Function[String, String] {
      override def apply(key: String): String = conf.get(key)
    }
    if (InternalSchemaHistory.isPresentIn(getConfig)) {
      InternalSchemaHistory.resolve(getConfig, versionId)
    } else {
      val validCommits = conf.get(SparkInternalSchemaConverter.HOODIE_VALID_COMMITS_LIST)
      InternalSchemaCache.getInternalSchemaByVersionId(versionId, tablePath,
        HoodieStorageUtils.getStorage(tablePath, HadoopFSUtils.getStorageConf(conf)), if (validCommits == null) "" else validCommits)
    }
  }

  def pruneInternalSchema(internalSchemaOpt:  util.Option[InternalSchema], requiredSchema: StructType): util.Option[InternalSchema] = {
    if (internalSchemaOpt.isPresent && requiredSchema.nonEmpty) {
      util.Option.of(SparkInternalSchemaConverter.convertAndPruneStructTypeToInternalSchema(requiredSchema, internalSchemaOpt.get()))
    } else {
      internalSchemaOpt
    }
  }
}
