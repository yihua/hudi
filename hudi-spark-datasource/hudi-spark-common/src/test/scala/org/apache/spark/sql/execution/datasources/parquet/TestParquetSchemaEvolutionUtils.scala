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

import org.apache.hudi.common.util.{Option => HOption}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.{Assertions, Test}

import java.util.HashMap

/**
 * Unit tests for the per-file type change checks of [[ParquetSchemaEvolutionUtils]].
 */
class TestParquetSchemaEvolutionUtils {

  /**
   * Any type change is reported, so a reader returning rows reads the file row-based. Only a nested
   * change fails a caller that must read vectorized (it returns batches); an atomic change does not.
   */
  @Test
  def testTypeChangesAreReportedAndOnlyNestedOnesFailVectorizedReads(): Unit = {
    val footer = new FileMetaData(Types.buildMessage()
      .addField(Types.optional(PrimitiveTypeName.INT32).named("id"))
      .addField(Types.optionalGroup().addField(Types.optional(PrimitiveTypeName.INT32).named("a")).named("nested"))
      .named("test"), new HashMap[String, String](), "test")
    // The keys ParquetToSparkSchemaConverter reads, as SparkParquetReaderBase.read sets them.
    val conf = new Configuration(false)
    conf.setBoolean("spark.sql.caseSensitive", false)
    conf.setBoolean("spark.sql.parquet.binaryAsString", false)
    conf.setBoolean("spark.sql.parquet.int96AsTimestamp", true)
    conf.setBoolean("spark.sql.legacy.parquet.nanosAsLong", false)
    conf.setBoolean("spark.sql.parquet.inferTimestampNTZ.enabled", true)
    def utils(requiredSchema: String): ParquetSchemaEvolutionUtils = new ParquetSchemaEvolutionUtils(
      conf, new Path("file:///tmp/test.parquet"), StructType.fromDDL(requiredSchema), new StructType(), HOption.empty())

    val unchanged = utils("id int, nested struct<a: int>")
    unchanged.getHadoopAttemptConf(footer, true)
    Assertions.assertFalse(unchanged.hasTypeChange)

    val atomicChange = utils("id long, nested struct<a: int>")
    atomicChange.getHadoopAttemptConf(footer, true)
    Assertions.assertTrue(atomicChange.hasTypeChange)

    val nestedChange = utils("id int, nested struct<a: long>")
    nestedChange.getHadoopAttemptConf(footer, false)
    Assertions.assertTrue(nestedChange.hasTypeChange)
    val failure = Assertions.assertThrows(classOf[IllegalArgumentException], () =>
      utils("id int, nested struct<a: long>").getHadoopAttemptConf(footer, true))
    Assertions.assertTrue(failure.getMessage.contains("cannot be read in vectorized mode"), failure.getMessage)
  }
}
