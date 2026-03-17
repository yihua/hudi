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

package org.apache.spark.sql.hudi.analysis

import org.apache.hudi.common.schema.HoodieSchema

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.HoodieVectorSearchTableValuedFunction.{DistanceMetric, SearchAlgorithm}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{array, broadcast, col, lit, monotonically_increasing_id, row_number, udf}
import org.apache.spark.sql.hudi.command.exception.HoodieAnalysisException
import org.apache.spark.sql.types.{ArrayType, ByteType, DataType, DoubleType, FloatType}

/**
 * Extension point for vector search algorithms. Each implementation provides
 * the Spark logical plan for single-query and batch-query KNN search.
 *
 * To add a new algorithm (e.g. RowMatrix, HNSW):
 *  1. Create an object extending this trait
 *  2. Add a value to [[SearchAlgorithm]]
 *  3. Register the mapping in [[HoodieVectorSearchPlanBuilder.resolveAlgorithm]]
 *
 * Implementations can use the shared validation helpers on
 * [[HoodieVectorSearchPlanBuilder]] (validateEmbeddingColumn, validateBatchDimensions, etc.)
 * and the raw distance functions on [[VectorDistanceUtils]].
 *
 * The output schema contract:
 *  - Single-query: all corpus columns (minus the embedding column) + `_distance: Double`
 *  - Batch-query: all corpus columns (minus the embedding column) + renamed query columns
 *    (prefixed with `_query_`) + `_distance: Double` + `_query_id: Long`
 *  - Results are ordered by `_distance` ascending (lower = more similar)
 */
trait VectorSearchAlgorithm {

  /** Human-readable name for error messages and logging. */
  def name: String

  /**
   * Build a plan that finds the k nearest corpus rows to a single query vector.
   *
   * @param spark          active SparkSession
   * @param corpusDf       resolved corpus DataFrame (may be Hudi, Parquet, or temp view)
   * @param embeddingCol   name of the array-typed embedding column in corpusDf
   * @param queryVector    the query vector, normalized to Array[Double]
   * @param k              number of nearest neighbors to return
   * @param metric         distance metric (COSINE, L2, DOT_PRODUCT)
   * @return an analyzed LogicalPlan whose output matches the single-query schema contract
   */
  def buildSingleQueryPlan(
      spark: SparkSession,
      corpusDf: DataFrame,
      embeddingCol: String,
      queryVector: Array[Double],
      k: Int,
      metric: DistanceMetric.Value): LogicalPlan

  /**
   * Build a plan that finds the k nearest corpus rows for each row in the query table.
   *
   * @param spark              active SparkSession
   * @param corpusDf           resolved corpus DataFrame
   * @param corpusEmbeddingCol name of the embedding column in corpusDf
   * @param queryDf            resolved query DataFrame
   * @param queryEmbeddingCol  name of the embedding column in queryDf
   * @param k                  number of nearest neighbors per query
   * @param metric             distance metric (COSINE, L2, DOT_PRODUCT)
   * @return an analyzed LogicalPlan whose output matches the batch-query schema contract
   */
  def buildBatchQueryPlan(
      spark: SparkSession,
      corpusDf: DataFrame,
      corpusEmbeddingCol: String,
      queryDf: DataFrame,
      queryEmbeddingCol: String,
      k: Int,
      metric: DistanceMetric.Value): LogicalPlan
}

/**
 * Resolves [[SearchAlgorithm]] values to [[VectorSearchAlgorithm]] implementations
 * and provides shared validation helpers used across algorithms.
 */
object HoodieVectorSearchPlanBuilder {

  val DISTANCE_COL = "_distance"
  private[analysis] val QUERY_ID_COL = "_query_id"
  private[analysis] val QUERY_EMB_ALIAS = "_query_emb_internal"
  private[analysis] val RANK_COL = "_rank"
  private[analysis] val QUERY_COL_PREFIX = "_query_"

  /** Resolve a [[SearchAlgorithm]] enum value to its implementation. */
  def resolveAlgorithm(algorithm: SearchAlgorithm.Value): VectorSearchAlgorithm = algorithm match {
    case SearchAlgorithm.BRUTE_FORCE => BruteForceSearchAlgorithm
    case other => throw new HoodieAnalysisException(
      s"Unsupported search algorithm: $other")
  }

  // ======================== Shared Validation ========================

  private[analysis] def validateEmbeddingColumn(df: DataFrame, colName: String): Unit = {
    val fieldOpt = df.schema.fields.find(_.name == colName)
    val field = fieldOpt.getOrElse(
      throw new HoodieAnalysisException(
        s"Embedding column '$colName' not found in table schema. " +
          s"Available columns: ${df.schema.fieldNames.mkString(", ")}"))
    field.dataType match {
      case ArrayType(FloatType, _) | ArrayType(DoubleType, _) | ArrayType(ByteType, _) => // valid
      case other =>
        throw new HoodieAnalysisException(
          s"Embedding column '$colName' has type $other, " +
            "expected array<float>, array<double>, or array<byte>")
    }
  }

  /**
   * Validates that the query vector dimension matches the corpus embedding dimension
   * when the corpus column has VECTOR(dim) metadata.
   */
  private[analysis] def validateQueryVectorDimension(
      df: DataFrame, embeddingCol: String, queryDim: Int): Unit = {
    extractVectorDimension(df, embeddingCol).foreach { corpusDim =>
      if (corpusDim != queryDim) {
        throw new HoodieAnalysisException(
          s"Query vector dimension ($queryDim) does not match " +
            s"corpus embedding dimension ($corpusDim) for column '$embeddingCol'")
      }
    }
  }

  /**
   * Validates that corpus and query embedding columns have the same element type.
   */
  private[analysis] def validateElementTypeCompatibility(
      corpusDf: DataFrame, corpusCol: String,
      queryDf: DataFrame, queryCol: String): Unit = {
    val corpusElemType = getElementType(corpusDf, corpusCol)
    val queryElemType = getElementType(queryDf, queryCol)
    if (corpusElemType != queryElemType) {
      throw new HoodieAnalysisException(
        s"Corpus embedding column '$corpusCol' has element type $corpusElemType " +
          s"but query embedding column '$queryCol' has element type $queryElemType. " +
          "Both must use the same element type (e.g. array<float>).")
    }
  }

  /**
   * Validates that corpus and query embedding dimensions match when both have
   * VECTOR(dim) metadata.
   */
  private[analysis] def validateBatchDimensions(
      corpusDf: DataFrame, corpusCol: String,
      queryDf: DataFrame, queryCol: String): Unit = {
    (extractVectorDimension(corpusDf, corpusCol), extractVectorDimension(queryDf, queryCol)) match {
      case (Some(corpusDim), Some(queryDim)) if corpusDim != queryDim =>
        throw new HoodieAnalysisException(
          s"Corpus embedding dimension ($corpusDim) does not match " +
            s"query embedding dimension ($queryDim)")
      case _ => // dimensions match or metadata not available
    }
  }

  private[analysis] def getElementType(df: DataFrame, colName: String): DataType = {
    df.schema(colName).dataType match {
      case ArrayType(elemType, _) => elemType
      case other =>
        throw new HoodieAnalysisException(
          s"Embedding column '$colName' has type $other, expected an array type")
    }
  }

  /** Extracts VECTOR(dim) dimension from column metadata, if present. */
  private def extractVectorDimension(df: DataFrame, colName: String): Option[Int] = {
    val field = df.schema.fields.find(_.name == colName).get
    val meta = field.metadata
    if (meta.contains(HoodieSchema.TYPE_METADATA_FIELD)) {
      val typeDesc = meta.getString(HoodieSchema.TYPE_METADATA_FIELD)
      val dimPattern = """VECTOR\((\d+)""".r
      dimPattern.findFirstMatchIn(typeDesc).map(_.group(1).toInt)
    } else None
  }
}

/**
 * Brute-force KNN vector search: computes distance between every corpus row and
 * every query vector, then selects the top-K closest.
 *
 * <p>Complexity: O(|corpus| * |queries| * dimensions) — linear scan with no index.
 *
 * <p><b>Single-query mode:</b> applies a distance UDF per corpus row, then
 * {@code orderBy + limit(k)} (Spark optimizes this to a partial sort via TakeOrderedAndProject).
 *
 * <p><b>Batch-query mode:</b> broadcast cross-joins the (small) query table with
 * the corpus, computes pairwise distances, then uses a window function to rank
 * and select top-K per query. The cross-join produces O(|corpus| * |queries|)
 * intermediate rows, so this is suitable for small-to-medium query sets
 * (tens to low hundreds of queries) against moderate corpora.
 */
object BruteForceSearchAlgorithm extends VectorSearchAlgorithm {

  import HoodieVectorSearchPlanBuilder._

  override val name: String = "brute_force"

  override def buildSingleQueryPlan(
      spark: SparkSession,
      corpusDf: DataFrame,
      embeddingCol: String,
      queryVector: Array[Double],
      k: Int,
      metric: DistanceMetric.Value): LogicalPlan = {
    validateEmbeddingColumn(corpusDf, embeddingCol)
    validateQueryVectorDimension(corpusDf, embeddingCol, queryVector.length)

    val elemType = getElementType(corpusDf, embeddingCol)
    val distanceUdf = createDistanceUdf(metric, elemType)
    val filteredDf = corpusDf.filter(col(embeddingCol).isNotNull)

    // Convert query vector to match corpus element type.
    // ByteType requires array() constructor because lit(Array[Byte]) creates BinaryType.
    val queryLit = elemType match {
      case FloatType => lit(queryVector.map(_.toFloat))
      case DoubleType => lit(queryVector)
      case ByteType => array(queryVector.map(v => lit(v.toByte)): _*)
      case _ => lit(queryVector)
    }

    val result = filteredDf
      .withColumn(DISTANCE_COL, distanceUdf(col(embeddingCol), queryLit))
      .drop(embeddingCol)
      .orderBy(col(DISTANCE_COL).asc)
      .limit(k)

    result.queryExecution.analyzed
  }

  override def buildBatchQueryPlan(
      spark: SparkSession,
      corpusDf: DataFrame,
      corpusEmbeddingCol: String,
      queryDf: DataFrame,
      queryEmbeddingCol: String,
      k: Int,
      metric: DistanceMetric.Value): LogicalPlan = {
    validateEmbeddingColumn(corpusDf, corpusEmbeddingCol)
    validateEmbeddingColumn(queryDf, queryEmbeddingCol)
    validateElementTypeCompatibility(corpusDf, corpusEmbeddingCol, queryDf, queryEmbeddingCol)
    validateBatchDimensions(corpusDf, corpusEmbeddingCol, queryDf, queryEmbeddingCol)

    val corpusElemType = getElementType(corpusDf, corpusEmbeddingCol)
    val distanceUdf = createDistanceUdf(metric, corpusElemType)
    val filteredCorpus = corpusDf.filter(col(corpusEmbeddingCol).isNotNull)

    // Prefix every query column with "_query_" to avoid cross-join column ambiguity:
    //   1. when corpusEmbeddingCol == queryEmbeddingCol (both named "embedding")
    //   2. when corpus and query share other non-embedding columns (e.g. both have "id")
    val corpusCols = filteredCorpus.columns.toSet
    val internalCols = Set(QUERY_ID_COL, QUERY_EMB_ALIAS, DISTANCE_COL, RANK_COL)
    val queryWithId = queryDf.filter(col(queryEmbeddingCol).isNotNull)
      .withColumnRenamed(queryEmbeddingCol, QUERY_EMB_ALIAS)
      .withColumn(QUERY_ID_COL, monotonically_increasing_id())

    // Rename any query column that clashes with a corpus column or internal columns.
    // Uses a double prefix if the standard rename would itself clash (e.g. "id" -> "_query_id"
    // would collide with the internal _query_id column).
    val renamedQuery = queryWithId.columns.foldLeft(queryWithId) { (df, qCol) =>
      if (qCol != QUERY_ID_COL && qCol != QUERY_EMB_ALIAS && corpusCols.contains(qCol)) {
        val candidate = s"$QUERY_COL_PREFIX$qCol"
        val safeName = if (internalCols.contains(candidate)) s"${QUERY_COL_PREFIX}user_$qCol" else candidate
        df.withColumnRenamed(qCol, safeName)
      } else {
        df
      }
    }

    // Cross join corpus with broadcast queries, compute distance, then rank
    val scored = filteredCorpus.crossJoin(broadcast(renamedQuery))
      .withColumn(DISTANCE_COL,
        distanceUdf(col(corpusEmbeddingCol), col(QUERY_EMB_ALIAS)))
      .drop(corpusEmbeddingCol)
      .drop(QUERY_EMB_ALIAS)

    val window = Window.partitionBy(QUERY_ID_COL).orderBy(col(DISTANCE_COL).asc)
    val result = scored
      .withColumn(RANK_COL, row_number().over(window))
      .filter(col(RANK_COL) <= k)
      .drop(RANK_COL)
      .orderBy(col(QUERY_ID_COL), col(DISTANCE_COL))

    result.queryExecution.analyzed
  }

  // ======================== Distance UDFs ========================

  /**
   * Creates a distance UDF typed for the corpus element type. Arithmetic uses
   * Double precision for the final result while keeping per-element operations
   * in native precision where possible.
   */
  private def createDistanceUdf(metric: DistanceMetric.Value, elementType: DataType): UserDefinedFunction = {
    elementType match {
      case FloatType => createFloatDistanceUdf(metric)
      case DoubleType => createDoubleDistanceUdf(metric)
      case ByteType => createByteDistanceUdf(metric)
      case _ => throw new HoodieAnalysisException(
        s"Unsupported vector element type for distance computation: $elementType")
    }
  }

  private def createFloatDistanceUdf(metric: DistanceMetric.Value): UserDefinedFunction = metric match {
    case DistanceMetric.COSINE => udf((a: Seq[Float], b: Seq[Float]) => {
      requireSameLength(a.length, b.length)
      var dot = 0.0f; var normA = 0.0f; var normB = 0.0f; var i = 0
      while (i < a.length) { dot += a(i) * b(i); normA += a(i) * a(i); normB += b(i) * b(i); i += 1 }
      val denom = math.sqrt(normA.toDouble) * math.sqrt(normB.toDouble)
      if (denom == 0.0) 1.0 else 1.0 - (dot.toDouble / denom)
    })
    case DistanceMetric.L2 => udf((a: Seq[Float], b: Seq[Float]) => {
      requireSameLength(a.length, b.length)
      var sum = 0.0f; var i = 0
      while (i < a.length) { val d = a(i) - b(i); sum += d * d; i += 1 }
      math.sqrt(sum.toDouble)
    })
    case DistanceMetric.DOT_PRODUCT => udf((a: Seq[Float], b: Seq[Float]) => {
      requireSameLength(a.length, b.length)
      var dot = 0.0f; var i = 0
      while (i < a.length) { dot += a(i) * b(i); i += 1 }
      -dot.toDouble
    })
  }

  private def createDoubleDistanceUdf(metric: DistanceMetric.Value): UserDefinedFunction = metric match {
    case DistanceMetric.COSINE => udf((a: Seq[Double], b: Seq[Double]) => {
      requireSameLength(a.length, b.length)
      var dot = 0.0; var normA = 0.0; var normB = 0.0; var i = 0
      while (i < a.length) { dot += a(i) * b(i); normA += a(i) * a(i); normB += b(i) * b(i); i += 1 }
      val denom = math.sqrt(normA) * math.sqrt(normB)
      if (denom == 0.0) 1.0 else 1.0 - (dot / denom)
    })
    case DistanceMetric.L2 => udf((a: Seq[Double], b: Seq[Double]) => {
      requireSameLength(a.length, b.length)
      var sum = 0.0; var i = 0
      while (i < a.length) { val d = a(i) - b(i); sum += d * d; i += 1 }
      math.sqrt(sum)
    })
    case DistanceMetric.DOT_PRODUCT => udf((a: Seq[Double], b: Seq[Double]) => {
      requireSameLength(a.length, b.length)
      var dot = 0.0; var i = 0
      while (i < a.length) { dot += a(i) * b(i); i += 1 }
      -dot
    })
  }

  private def createByteDistanceUdf(metric: DistanceMetric.Value): UserDefinedFunction = metric match {
    case DistanceMetric.COSINE => udf((a: Seq[Byte], b: Seq[Byte]) => {
      requireSameLength(a.length, b.length)
      var dot = 0L; var normA = 0L; var normB = 0L; var i = 0
      while (i < a.length) { dot += a(i) * b(i); normA += a(i) * a(i); normB += b(i) * b(i); i += 1 }
      val denom = math.sqrt(normA.toDouble) * math.sqrt(normB.toDouble)
      if (denom == 0.0) 1.0 else 1.0 - (dot.toDouble / denom)
    })
    case DistanceMetric.L2 => udf((a: Seq[Byte], b: Seq[Byte]) => {
      requireSameLength(a.length, b.length)
      var sum = 0L; var i = 0
      while (i < a.length) { val d = a(i) - b(i); sum += d * d; i += 1 }
      math.sqrt(sum.toDouble)
    })
    case DistanceMetric.DOT_PRODUCT => udf((a: Seq[Byte], b: Seq[Byte]) => {
      requireSameLength(a.length, b.length)
      var dot = 0L; var i = 0
      while (i < a.length) { dot += a(i) * b(i); i += 1 }
      -dot.toDouble
    })
  }

  private def requireSameLength(aLen: Int, bLen: Int): Unit = {
    if (aLen != bLen) {
      throw new IllegalArgumentException(
        s"Vector dimension mismatch: $aLen vs $bLen")
    }
  }
}
