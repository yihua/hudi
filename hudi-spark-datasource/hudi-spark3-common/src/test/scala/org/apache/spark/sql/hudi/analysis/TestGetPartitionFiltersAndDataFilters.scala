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

package org.apache.spark.sql.hudi.analysis

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

/**
 * Unit tests for [[Spark3HoodiePruneFileSourcePartitions.getPartitionFiltersAndDataFilters]].
 *
 * Tests that partition filter classification correctly handles nested partition columns
 * using the nested [[StructType]] partition schema from `partitionSchemaForSpark`.
 */
class TestGetPartitionFiltersAndDataFilters {

  private def attr(name: String, dataType: DataType = StringType): AttributeReference =
    AttributeReference(name, dataType)()

  // Nested partition schema for partition column "nested_record.level"
  // partitionSchemaForSpark produces: StructType(StructField("nested_record", StructType(StructField("level", StringType))))
  private val nestedPartitionSchema = StructType(Seq(
    StructField("nested_record", StructType(Seq(StructField("level", StringType))))
  ))

  // Flat partition schema for partition column "country"
  private val flatPartitionSchema = StructType(Seq(
    StructField("country", StringType)
  ))

  @Test
  def testFlatPartitionColumnRecognized(): Unit = {
    // country = 'US'
    val filter = EqualTo(attr("country"), Literal("US"))
    val (partFilters, dataFilters) =
      Spark3HoodiePruneFileSourcePartitions.getPartitionFiltersAndDataFilters(flatPartitionSchema, Seq(filter))
    assertEquals(1, partFilters.size, "Flat partition filter should be classified as partition filter")
    assertTrue(dataFilters.isEmpty, "No data filters expected")
  }

  @Test
  def testFlatDataColumnExcluded(): Unit = {
    // int_field = 5
    val filter = EqualTo(attr("int_field", IntegerType), Literal(5))
    val (partFilters, dataFilters) =
      Spark3HoodiePruneFileSourcePartitions.getPartitionFiltersAndDataFilters(flatPartitionSchema, Seq(filter))
    assertTrue(partFilters.isEmpty, "Data column filter should not be a partition filter")
    assertEquals(1, dataFilters.size)
  }

  @Test
  def testNestedPartitionColumnStructRootRecognized(): Unit = {
    // For nested partition column "nested_record.level", the nested schema has "nested_record"
    // as a top-level field. Spark represents filter nested_record.level = 'INFO' as
    // GetStructField(attr("nested_record"), ..., "level") = Literal("INFO").
    // The AttributeReference "nested_record" should be recognized as referencing a partition column.
    val nestedRecordType = StructType(Seq(StructField("level", StringType)))
    val gsf = GetStructField(attr("nested_record", nestedRecordType), 0, Some("level"))
    val filter = EqualTo(gsf, Literal("INFO"))
    val (partFilters, dataFilters) =
      Spark3HoodiePruneFileSourcePartitions.getPartitionFiltersAndDataFilters(nestedPartitionSchema, Seq(filter))
    assertEquals(1, partFilters.size, "Nested partition filter should be classified as partition filter")
  }

  @Test
  def testNonPartitionNestedFieldExcluded(): Unit = {
    // nested_record.nested_int = 10 — same struct root but different field
    // In the nested schema, "nested_record" is a top-level name, so the AttributeReference
    // "nested_record" still matches. This means the filter IS classified as a partition filter
    // by getPartitionFiltersAndDataFilters (which uses AttributeSet-based classification).
    // The precise filtering happens downstream in listMatchingPartitionPaths via
    // referencesOnlyPartitionColumns, which walks GetStructField chains.
    val nestedRecordType = StructType(Seq(StructField("nested_int", IntegerType), StructField("level", StringType)))
    val gsf = GetStructField(attr("nested_record", nestedRecordType), 0, Some("nested_int"))
    val filter = EqualTo(gsf, Literal(10))
    val (partFilters, _) =
      Spark3HoodiePruneFileSourcePartitions.getPartitionFiltersAndDataFilters(nestedPartitionSchema, Seq(filter))
    // Note: getPartitionFiltersAndDataFilters classifies by AttributeReference root ("nested_record"),
    // which IS in the nested partition schema. So this is classified as a partition filter here.
    // The downstream referencesOnlyPartitionColumns in listMatchingPartitionPaths will correctly
    // exclude it by checking the full GetStructField path "nested_record.nested_int".
    assertEquals(1, partFilters.size,
      "Filter on non-partition nested field is classified as partition filter by AttributeSet matching " +
        "(precise exclusion happens downstream in listMatchingPartitionPaths)")
  }

  @Test
  def testMixedPartitionAndDataColumns(): Unit = {
    // Two filters: nested_record.level = 'INFO' and int_field = 5
    val nestedRecordType = StructType(Seq(StructField("level", StringType)))
    val gsf = GetStructField(attr("nested_record", nestedRecordType), 0, Some("level"))
    val partFilter = EqualTo(gsf, Literal("INFO"))
    val dataFilter = EqualTo(attr("int_field", IntegerType), Literal(5))
    val (partFilters, dataFilters) =
      Spark3HoodiePruneFileSourcePartitions.getPartitionFiltersAndDataFilters(nestedPartitionSchema, Seq(partFilter, dataFilter))
    assertEquals(1, partFilters.size, "Partition filter should be extracted")
    assertEquals(1, dataFilters.size, "Data filter should remain")
  }

  @Test
  def testIsNotNullOnStructRootClassifiedAsPartition(): Unit = {
    // IsNotNull(nested_record) — Spark auto-adds this.
    // Since "nested_record" is in the nested partition schema names, it's classified as partition.
    val nestedRecordType = StructType(Seq(StructField("level", StringType)))
    val filter = IsNotNull(attr("nested_record", nestedRecordType))
    val (partFilters, _) =
      Spark3HoodiePruneFileSourcePartitions.getPartitionFiltersAndDataFilters(nestedPartitionSchema, Seq(filter))
    assertEquals(1, partFilters.size,
      "IsNotNull on struct root should be classified as partition filter (struct root is in nested schema)")
  }

  @Test
  def testUnrelatedStructNotRecognized(): Unit = {
    // other_struct.field = 'value' — struct root "other_struct" is NOT in partition schema
    val otherType = StructType(Seq(StructField("field", StringType)))
    val gsf = GetStructField(attr("other_struct", otherType), 0, Some("field"))
    val filter = EqualTo(gsf, Literal("value"))
    val (partFilters, dataFilters) =
      Spark3HoodiePruneFileSourcePartitions.getPartitionFiltersAndDataFilters(nestedPartitionSchema, Seq(filter))
    assertTrue(partFilters.isEmpty, "Filter on unrelated struct should not be a partition filter")
    assertEquals(1, dataFilters.size)
  }

  @Test
  def testEmptyFilters(): Unit = {
    val (partFilters, dataFilters) =
      Spark3HoodiePruneFileSourcePartitions.getPartitionFiltersAndDataFilters(nestedPartitionSchema, Seq.empty)
    assertTrue(partFilters.isEmpty)
    assertTrue(dataFilters.isEmpty)
  }
}
