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

/**
 * Raw vector distance functions that operate on Array[Double].
 * These are not UDF-wrapped and can be used by any vector search engine
 * for verification, index building, or non-Spark distance computation.
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
    if (denom == 0.0) 1.0 else 1.0 - (dot / denom)
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
}
