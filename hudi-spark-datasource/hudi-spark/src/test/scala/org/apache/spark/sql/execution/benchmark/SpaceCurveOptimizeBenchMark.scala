/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.benchmark

import org.apache.hadoop.fs.Path
import org.apache.spark.SpaceCurveOptimizeHelper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hudi.TestHoodieSqlBase

import scala.util.Random

object SpaceCurveOptimizeBenchMark extends TestHoodieSqlBase {

  def getSkippingPercent(tableName: String, co1: String, co2: String, value1: Int, value2: Int): Unit= {
    val minMax = SpaceCurveOptimizeHelper
      .getMinMaxValue(spark.sql(s"select * from ${tableName}"), s"${co1}, ${co2}")
      .collect().map(f => (f.getInt(1), f.getInt(2), f.getInt(4), f.getInt(5)))
    var c = 0
    for (elem <- minMax) {
      if ((elem._1 <= value1 && elem._2 >= value1) || (elem._3 <= value2 && elem._4 >= value2)) {
        c = c + 1
      }
    }

    val p = c / minMax.size.toDouble
    println(s"for table ${tableName} with query filter: ${co1} = ${value1} or ${co2} = ${value2} we can achieve skipping percent ${1.0 - p}")
  }

  /*
  for table table_z_sort_byMap with query filter: c1_int = 500000 or c2_int = 500000 we can achieve skipping percent 0.8
  for table table_z_sort_bySample with query filter: c1_int = 500000 or c2_int = 500000 we can achieve skipping percent 0.77
  for table table_hilbert_sort_byMap with query filter: c1_int = 500000 or c2_int = 500000 we can achieve skipping percent 0.855
  for table table_hilbert_sort_bySample with query filter: c1_int = 500000 or c2_int = 500000 we can achieve skipping percent 0.83
  */
  def runNormalTableSkippingBenchMark(): Unit = {
    withTempDir { f =>
      withTempTable("table_z_sort_byMap", "table_z_sort_bySample", "table_hilbert_sort_byMap", "table_hilbert_sort_bySample") {
        prepareNormalTable(new Path(f.getAbsolutePath), 1000000)
        // choose median value as filter condition.
        // the median value of c1_int is 500000
        // the median value of c2_int is 500000
        getSkippingPercent("table_z_sort_byMap", "c1_int", "c2_int", 500000, 500000)
        getSkippingPercent("table_z_sort_bySample", "c1_int", "c2_int", 500000, 500000)
        getSkippingPercent("table_hilbert_sort_byMap", "c1_int", "c2_int", 500000, 500000)
        getSkippingPercent("table_hilbert_sort_bySample", "c1_int", "c2_int", 500000, 500000)
      }
    }
  }

  /*
  for table table_z_sort_byMap_skew with query filter: c1_int = 5000 or c2_int = 500000 we can achieve skipping percent 0.0
  for table table_z_sort_bySample_skew with query filter: c1_int = 5000 or c2_int = 500000 we can achieve skipping percent 0.78
  for table table_hilbert_sort_byMap_skew with query filter: c1_int = 5000 or c2_int = 500000 we can achieve skipping percent 0.05500000000000005
  for table table_hilbert_sort_bySample_skew with query filter: c1_int = 5000 or c2_int = 500000 we can achieve skipping percent 0.84
  */
  def runSkewTableSkippingBenchMark(): Unit = {
    withTempDir { f =>
      withTempTable("table_z_sort_byMap_skew", "table_z_sort_bySample_skew", "table_hilbert_sort_byMap_skew", "table_hilbert_sort_bySample_skew") {
        prepareSkewTable(new Path(f.getAbsolutePath), 1000000)
        // choose median value as filter condition.
        // the median value of c1_int is 5000
        // the median value of c2_int is 500000
        getSkippingPercent("table_z_sort_byMap_skew", "c1_int", "c2_int", 5000, 500000)
        getSkippingPercent("table_z_sort_bySample_skew", "c1_int", "c2_int", 5000, 500000)
        getSkippingPercent("table_hilbert_sort_byMap_skew", "c1_int", "c2_int", 5000, 500000)
        getSkippingPercent("table_hilbert_sort_bySample_skew", "c1_int", "c2_int", 5000, 500000)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    runNormalTableSkippingBenchMark()
    runSkewTableSkippingBenchMark()
  }

  def withTempTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally tableNames.foreach(spark.catalog.dropTempView)
  }

  def prepareNormalTable(tablePath: Path, numRows: Int): Unit = {
    import spark.implicits._
    val df = spark.range(numRows).map(_ => (Random.nextInt(1000000), Random.nextInt(1000000))).toDF("c1_int", "c2_int")
    val dfOptimizeByMap = SpaceCurveOptimizeHelper.createOptimizedDataFrameByMapValue(df, "c1_int, c2_int", 200, "z-order")
    val dfOptimizeBySample = SpaceCurveOptimizeHelper.createOptimizeDataFrameBySample(df, "c1_int, c2_int", 200, "z-order")

    val dfHilbertOptimizeByMap = SpaceCurveOptimizeHelper.createOptimizedDataFrameByMapValue(df, "c1_int, c2_int", 200, "hilbert")
    val dfHilbertOptimizeBySample = SpaceCurveOptimizeHelper.createOptimizeDataFrameBySample(df, "c1_int, c2_int", 200, "hilbert")

    saveAsTable(dfOptimizeByMap, tablePath, "z_sort_byMap")
    saveAsTable(dfOptimizeBySample, tablePath, "z_sort_bySample")
    saveAsTable(dfHilbertOptimizeByMap, tablePath, "hilbert_sort_byMap")
    saveAsTable(dfHilbertOptimizeBySample, tablePath, "hilbert_sort_bySample")
  }

  def prepareSkewTable(tablePath: Path, numRows: Int): Unit = {
    import spark.implicits._
    val df = spark.range(numRows).map(_ => (Random.nextInt(10000), Random.nextInt(1000000))).toDF("c1_int", "c2_int")
    val dfOptimizeByMap = SpaceCurveOptimizeHelper.createOptimizedDataFrameByMapValue(df, "c1_int, c2_int", 200, "z-order")
    val dfOptimizeBySample = SpaceCurveOptimizeHelper.createOptimizeDataFrameBySample(df, "c1_int, c2_int", 200, "z-order")

    val dfHilbertOptimizeByMap = SpaceCurveOptimizeHelper.createOptimizedDataFrameByMapValue(df, "c1_int, c2_int", 200, "hilbert")
    val dfHilbertOptimizeBySample = SpaceCurveOptimizeHelper.createOptimizeDataFrameBySample(df, "c1_int, c2_int", 200, "hilbert")

    saveAsTable(dfOptimizeByMap, tablePath, "z_sort_byMap_skew")
    saveAsTable(dfOptimizeBySample, tablePath, "z_sort_bySample_skew")
    saveAsTable(dfHilbertOptimizeByMap, tablePath, "hilbert_sort_byMap_skew")
    saveAsTable(dfHilbertOptimizeBySample, tablePath, "hilbert_sort_bySample_skew")
  }

  def saveAsTable(df: DataFrame, savePath: Path, suffix: String): Unit = {

    df.write.mode("overwrite").save(new Path(savePath, suffix).toString)
    spark.read.parquet(new Path(savePath, suffix).toString).createOrReplaceTempView("table_" + suffix)
  }
}

