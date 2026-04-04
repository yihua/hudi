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


package org.apache.hudi.functional

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.types._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

/**
 * Temporary debug test — run to inspect execution plans via Spark UI at localhost:4040.
 * DELETE THIS FILE before merging.
 */
class DebugVectorSearchPlan {

  var spark: SparkSession = _

  @BeforeEach def setUp(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("DebugVectorSearchPlan")
      .config("spark.ui.enabled", "true")
      .config("spark.ui.port", "4040")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .withExtensions(new HoodieSparkSessionExtension)
      .getOrCreate()

    createCorpusView()
  }

  @AfterEach def tearDown(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  private def createCorpusView(): Unit = {
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false), nullable = false),
      StructField("label", StringType, nullable = true)
    ))
    val rows = Seq(
      Row("doc_1", Seq(1.0f, 0.0f, 0.0f), "x-axis"),
      Row("doc_2", Seq(0.0f, 1.0f, 0.0f), "y-axis"),
      Row("doc_3", Seq(0.0f, 0.0f, 1.0f), "z-axis"),
      Row("doc_4", Seq(0.70710678f, 0.70710678f, 0.0f), "xy-diagonal"),
      Row("doc_5", Seq(0.57735027f, 0.57735027f, 0.57735027f), "xyz-diagonal")
    )
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      .createOrReplaceTempView("corpus")
  }

  @Test
  def debugSingleQueryPlan(): Unit = {
    val df = spark.sql(
      """
        |SELECT id, label, _hudi_distance
        |FROM hudi_vector_search(
        |  'corpus', 'embedding', ARRAY(1.0, 0.0, 0.0), 3, 'cosine', 'brute_force'
        |)
        |ORDER BY _hudi_distance
        |""".stripMargin
    )

    println("\n=== SINGLE QUERY: EXTENDED PLAN ===")
    df.explain(true)
    println("\n=== SINGLE QUERY: RESULTS ===")
    df.show(truncate = false)

    println(">>> Spark UI at http://localhost:4040 — sleeping 5 minutes <<<")
    Thread.sleep(300000)
  }

  @Test
  def debugBatchQueryPlan(): Unit = {
    val querySchema = StructType(Seq(
      StructField("qid", StringType, nullable = false),
      StructField("qvec", ArrayType(FloatType, containsNull = false), nullable = false)
    ))
    val queryRows = Seq(
      Row("q_x", Seq(1.0f, 0.0f, 0.0f)),
      Row("q_z", Seq(0.0f, 0.0f, 1.0f))
    )
    spark.createDataFrame(spark.sparkContext.parallelize(queryRows), querySchema)
      .createOrReplaceTempView("queries")

    val df = spark.sql(
      """
        |SELECT id, label, _hudi_distance, _hudi_qid
        |FROM hudi_vector_search_batch(
        |  'corpus', 'embedding', 'queries', 'qvec', 3, 'cosine', 'brute_force'
        |)
        |ORDER BY _hudi_qid, _hudi_distance
        |""".stripMargin
    )

    println("\n=== BATCH QUERY: EXTENDED PLAN ===")
    df.explain(true)
    println("\n=== BATCH QUERY: RESULTS ===")
    df.show(truncate = false)

    println(">>> Spark UI at http://localhost:4040 — sleeping 5 minutes <<<")
    Thread.sleep(300000)
  }
}
