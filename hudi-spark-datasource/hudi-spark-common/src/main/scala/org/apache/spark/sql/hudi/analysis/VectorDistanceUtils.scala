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

import org.apache.spark.sql.catalyst.plans.logical.HoodieVectorSearchTableValuedFunction.DistanceMetric
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.hudi.command.exception.HoodieAnalysisException
import org.apache.spark.sql.types.{ByteType, DataType, DoubleType, FloatType}

/**
 * Vector distance utilities: raw distance functions and Spark UDF factories.
 *
 * Raw functions operate on Array[Double] and can be used outside of Spark
 * (e.g. for verification, index building, or non-Spark distance computation).
 *
 * UDF factories produce typed Spark UDFs for Float, Double, and Byte corpus columns.
 */
object VectorDistanceUtils {

  /**
   * Cosine distance: 1 - (a . b) / (||a|| * ||b||).
   * Returns 0.0 for identical vectors, 1.0 for orthogonal, 2.0 for opposite.
   * Returns 1.0 if either vector is zero (convention: maximal distance).
   */
  def cosineDistance(a: Array[Double], b: Array[Double]): Double = {
    require(a.length == b.length, s"Vector dimension mismatch: ${a.length} vs ${b.length}")
    var dot = 0.0; var normA = 0.0; var normB = 0.0; var i = 0
    while (i < a.length) {
      dot += a(i) * b(i); normA += a(i) * a(i); normB += b(i) * b(i); i += 1
    }
    val denom = math.sqrt(normA) * math.sqrt(normB)
    if (denom == 0.0) 1.0 else math.min(2.0, math.max(0.0, 1.0 - (dot / denom)))
  }

  /**
   * L2 (Euclidean) distance: sqrt(sum((a[i] - b[i])^2)).
   * Returns 0.0 for identical vectors.
   */
  def l2Distance(a: Array[Double], b: Array[Double]): Double = {
    require(a.length == b.length, s"Vector dimension mismatch: ${a.length} vs ${b.length}")
    var sum = 0.0; var i = 0
    while (i < a.length) { val d = a(i) - b(i); sum += d * d; i += 1 }
    math.sqrt(sum)
  }

  /**
   * Negated dot product distance: -(a . b).
   * Lower values indicate higher similarity (for ascending sort compatibility).
   */
  def dotProductDistance(a: Array[Double], b: Array[Double]): Double = {
    require(a.length == b.length, s"Vector dimension mismatch: ${a.length} vs ${b.length}")
    var dot = 0.0; var i = 0
    while (i < a.length) { dot += a(i) * b(i); i += 1 }
    -dot
  }

  // ======================== Spark UDF Factories ========================

  /**
   * Creates a Spark UDF for the given distance metric and corpus element type.
   * Supports Float, Double, and Byte element types.
   */
  def createDistanceUdf(metric: DistanceMetric.Value, elementType: DataType): UserDefinedFunction =
    elementType match {
      case FloatType  => createFloatDistanceUdf(metric)
      case DoubleType => createDoubleDistanceUdf(metric)
      case ByteType   => createByteDistanceUdf(metric)
      case _ => throw new HoodieAnalysisException(
        s"Unsupported vector element type for distance computation: $elementType")
    }

  private def createFloatDistanceUdf(metric: DistanceMetric.Value): UserDefinedFunction = metric match {
    case DistanceMetric.COSINE => udf((a: Seq[Float], b: Seq[Float]) => {
      requireSameLength(a.length, b.length)
      var dot = 0.0; var normA = 0.0; var normB = 0.0; var i = 0
      while (i < a.length) { dot += a(i) * b(i); normA += a(i) * a(i); normB += b(i) * b(i); i += 1 }
      val denom = math.sqrt(normA) * math.sqrt(normB)
      if (denom == 0.0) 1.0 else math.min(2.0, math.max(0.0, 1.0 - (dot / denom)))
    })
    case DistanceMetric.L2 => udf((a: Seq[Float], b: Seq[Float]) => {
      requireSameLength(a.length, b.length)
      var sum = 0.0; var i = 0
      while (i < a.length) { val d = a(i) - b(i); sum += d * d; i += 1 }
      math.sqrt(sum)
    })
    case DistanceMetric.DOT_PRODUCT => udf((a: Seq[Float], b: Seq[Float]) => {
      requireSameLength(a.length, b.length)
      var dot = 0.0; var i = 0
      while (i < a.length) { dot += a(i) * b(i); i += 1 }
      -dot
    })
  }

  private def createDoubleDistanceUdf(metric: DistanceMetric.Value): UserDefinedFunction = metric match {
    case DistanceMetric.COSINE => udf((a: Seq[Double], b: Seq[Double]) => {
      requireSameLength(a.length, b.length)
      var dot = 0.0; var normA = 0.0; var normB = 0.0; var i = 0
      while (i < a.length) { dot += a(i) * b(i); normA += a(i) * a(i); normB += b(i) * b(i); i += 1 }
      val denom = math.sqrt(normA) * math.sqrt(normB)
      if (denom == 0.0) 1.0 else math.min(2.0, math.max(0.0, 1.0 - (dot / denom)))
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
      if (denom == 0.0) 1.0 else math.min(2.0, math.max(0.0, 1.0 - (dot.toDouble / denom)))
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
      throw new IllegalArgumentException(s"Vector dimension mismatch: $aLen vs $bLen")
    }
  }
}
