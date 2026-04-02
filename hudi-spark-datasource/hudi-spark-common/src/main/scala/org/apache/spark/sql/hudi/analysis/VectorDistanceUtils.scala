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

import org.apache.spark.ml.linalg.{DenseVector, Vectors}
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
    val va = new DenseVector(a)
    val vb = new DenseVector(b)
    val denom = Vectors.norm(va, 2.0) * Vectors.norm(vb, 2.0)
    if (denom == 0.0) 1.0 else math.min(2.0, math.max(0.0, 1.0 - (va.dot(vb) / denom)))
  }

  /**
   * L2 (Euclidean) distance: sqrt(sum((a[i] - b[i])^2)).
   * Returns 0.0 for identical vectors.
   */
  def l2Distance(a: Array[Double], b: Array[Double]): Double = {
    require(a.length == b.length, s"Vector dimension mismatch: ${a.length} vs ${b.length}")
    math.sqrt(Vectors.sqdist(new DenseVector(a), new DenseVector(b)))
  }

  /**
   * Negated dot product distance: -(a . b).
   * Lower values indicate higher similarity (for ascending sort compatibility).
   */
  def dotProductDistance(a: Array[Double], b: Array[Double]): Double = {
    require(a.length == b.length, s"Vector dimension mismatch: ${a.length} vs ${b.length}")
    -(new DenseVector(a)).dot(new DenseVector(b))
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

  private def computeRawDistance(metric: DistanceMetric.Value, a: Array[Double], b: Array[Double]): Double =
    metric match {
      case DistanceMetric.COSINE      => cosineDistance(a, b)
      case DistanceMetric.L2          => l2Distance(a, b)
      case DistanceMetric.DOT_PRODUCT => dotProductDistance(a, b)
    }

  private def createFloatDistanceUdf(metric: DistanceMetric.Value): UserDefinedFunction =
    udf((a: Seq[Float], b: Seq[Float]) => {
      requireSameLength(a.length, b.length)
      computeRawDistance(metric, a.iterator.map(_.toDouble).toArray, b.iterator.map(_.toDouble).toArray)
    })

  private def createDoubleDistanceUdf(metric: DistanceMetric.Value): UserDefinedFunction =
    udf((a: Seq[Double], b: Seq[Double]) => {
      requireSameLength(a.length, b.length)
      computeRawDistance(metric, a.toArray, b.toArray)
    })

  private def createByteDistanceUdf(metric: DistanceMetric.Value): UserDefinedFunction =
    udf((a: Seq[Byte], b: Seq[Byte]) => {
      requireSameLength(a.length, b.length)
      computeRawDistance(metric, a.iterator.map(_.toDouble).toArray, b.iterator.map(_.toDouble).toArray)
    })

  private def requireSameLength(aLen: Int, bLen: Int): Unit = {
    if (aLen != bLen) {
      throw new IllegalArgumentException(s"Vector dimension mismatch: $aLen vs $bLen")
    }
  }
}
