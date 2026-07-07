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

package org.apache.spark.sql.hudi.execution;

import org.apache.hudi.common.util.BinaryUtil;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.sort.SpaceCurveSortingHelper;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.util.JavaScalaConverters;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class TestRangeSampleSort extends HoodieClientTestBase {

  @Test
  void sortDataFrameBySampleSupportAllTypes() {
    Dataset<Row> df = this.context.getSqlContext().sql("select 1 as id, array(2) as content");
    for (int i = 0; i < 2; i++) {
      final int limit = i;
      Assertions.assertDoesNotThrow(() ->
          RangeSampleSort$.MODULE$.sortDataFrameBySampleSupportAllTypes(df.limit(limit),
              JavaScalaConverters.convertJavaListToScalaSeq(Arrays.asList("id", "content")), 1), "range sort shall not fail when 0 or 1 record incoming");
    }
  }

  @Test
  void sortDataFrameBySample() {
    HoodieClusteringConfig.LayoutOptimizationStrategy layoutOptStrategy = HoodieClusteringConfig.LayoutOptimizationStrategy.HILBERT;
    Dataset<Row> df = this.context.getSqlContext().sql("select 1 as id, 2 as content");
    for (int i = 0; i < 2; i++) {
      final int limit = i;
      Assertions.assertDoesNotThrow(() ->
          RangeSampleSort$.MODULE$.sortDataFrameBySample(df.limit(limit), layoutOptStrategy,
              JavaScalaConverters.convertJavaListToScalaSeq(Arrays.asList("id", "content")), 1), "range sort shall not fail when 0 or 1 record incoming");
    }
  }

  /**
   * Builds a two-column integer frame with a single output partition so the
   * space-curve ordering is fully deterministic and can be asserted row by row.
   */
  private Dataset<Row> buildTwoIntColumnFrame(List<Row> rows) {
    StructType schema = new StructType(new StructField[] {
        new StructField("c1", DataTypes.IntegerType, true, org.apache.spark.sql.types.Metadata.empty()),
        new StructField("c2", DataTypes.IntegerType, true, org.apache.spark.sql.types.Metadata.empty())
    });
    return sparkSession.createDataFrame(rows, schema);
  }

  /**
   * Recomputes the Z-curve ordinal for a two-int row using the same public byte mapping
   * the helper relies on, so the expected ordering is derived independently.
   */
  private byte[] expectedZOrdinal(Integer c1, Integer c2) {
    byte[] b1 = BinaryUtil.intTo8Byte(c1 == null ? Integer.MAX_VALUE : c1);
    byte[] b2 = BinaryUtil.intTo8Byte(c2 == null ? Integer.MAX_VALUE : c2);
    return BinaryUtil.interleaving(new byte[][] {b1, b2}, 8);
  }

  private static int compareUnsigned(byte[] a, byte[] b) {
    for (int i = 0; i < a.length && i < b.length; i++) {
      int cmp = (a[i] & 0xFF) - (b[i] & 0xFF);
      if (cmp != 0) {
        return cmp;
      }
    }
    return a.length - b.length;
  }

  @Test
  void orderByMappingValuesZOrderPreservesSchemaAndSortsByCurve() {
    List<Row> rows = Arrays.asList(
        RowFactory.create(5, 5),
        RowFactory.create(1, 9),
        RowFactory.create(9, 1),
        RowFactory.create(1, 1));
    Dataset<Row> df = buildTwoIntColumnFrame(rows);

    Dataset<Row> ordered = SpaceCurveSortingHelper.orderDataFrameByMappingValues(
        df, HoodieClusteringConfig.LayoutOptimizationStrategy.ZORDER, Arrays.asList("c1", "c2"), 1);

    // The helper drops the internal Index column, so the output schema must match the input exactly.
    Assertions.assertArrayEquals(
        new String[] {"c1", "c2"}, ordered.schema().fieldNames(),
        "z-order output must retain only the original columns");

    List<Row> result = ordered.collectAsList();
    Assertions.assertEquals(rows.size(), result.size(), "no rows should be dropped");

    // Every emitted row must be non-decreasing under the independently recomputed Z-curve ordinal.
    for (int i = 1; i < result.size(); i++) {
      byte[] prev = expectedZOrdinal(result.get(i - 1).getInt(0), result.get(i - 1).getInt(1));
      byte[] curr = expectedZOrdinal(result.get(i).getInt(0), result.get(i).getInt(1));
      Assertions.assertTrue(compareUnsigned(prev, curr) <= 0,
          "row " + i + " violates ascending Z-curve order");
    }

    // (1,1) has the smallest interleaved ordinal, so it must sort first.
    Row first = result.get(0);
    Assertions.assertEquals(1, first.getInt(0));
    Assertions.assertEquals(1, first.getInt(1));
  }

  @Test
  void orderByMappingValuesZOrderPlacesNullLast() {
    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(2, 2));
    rows.add(RowFactory.create(null, null));
    rows.add(RowFactory.create(1, 1));
    Dataset<Row> df = buildTwoIntColumnFrame(rows);

    Dataset<Row> ordered = SpaceCurveSortingHelper.orderDataFrameByMappingValues(
        df, HoodieClusteringConfig.LayoutOptimizationStrategy.ZORDER, Arrays.asList("c1", "c2"), 1);

    List<Row> result = ordered.collectAsList();
    Assertions.assertEquals(3, result.size());
    // Nulls map to Integer.MAX_VALUE in the byte mapping, so the all-null row sorts last.
    Row last = result.get(result.size() - 1);
    Assertions.assertTrue(last.isNullAt(0) && last.isNullAt(1),
        "the all-null row must sort last under Z-curve mapping");
  }

  @Test
  void orderByMappingValuesHilbertPreservesRowsAndSchema() {
    List<Row> rows = Arrays.asList(
        RowFactory.create(7, 3),
        RowFactory.create(0, 0),
        RowFactory.create(4, 4));
    Dataset<Row> df = buildTwoIntColumnFrame(rows);

    Dataset<Row> ordered = SpaceCurveSortingHelper.orderDataFrameByMappingValues(
        df, HoodieClusteringConfig.LayoutOptimizationStrategy.HILBERT, Arrays.asList("c1", "c2"), 1);

    Assertions.assertArrayEquals(new String[] {"c1", "c2"}, ordered.schema().fieldNames(),
        "hilbert output must retain only the original columns");
    List<Row> result = ordered.collectAsList();
    Assertions.assertEquals(rows.size(), result.size(), "hilbert ordering must not drop rows");
    // Origin (0,0) maps to the smallest Hilbert index, so it must sort first.
    Assertions.assertEquals(0, result.get(0).getInt(0));
    Assertions.assertEquals(0, result.get(0).getInt(1));
  }

  @Test
  void orderByMappingValuesSingleColumnIsLinearOrder() {
    List<Row> rows = Arrays.asList(
        RowFactory.create(3, 30),
        RowFactory.create(1, 10),
        RowFactory.create(2, 20));
    Dataset<Row> df = buildTwoIntColumnFrame(rows);

    // A single ordering column short-circuits space-curve mapping into a plain range repartition.
    Dataset<Row> ordered = SpaceCurveSortingHelper.orderDataFrameByMappingValues(
        df, HoodieClusteringConfig.LayoutOptimizationStrategy.ZORDER, Arrays.asList("c1"), 1);

    Assertions.assertArrayEquals(new String[] {"c1", "c2"}, ordered.schema().fieldNames(),
        "single-column ordering must not add an Index column");
    List<Row> result = ordered.collectAsList();
    List<Integer> c1Values = new ArrayList<>();
    for (Row r : result) {
      c1Values.add(r.getInt(0));
    }
    Assertions.assertEquals(Arrays.asList(1, 2, 3), c1Values,
        "single-column ordering must produce ascending values");
  }

  @Test
  void orderByMappingValuesUnknownColumnReturnsInputUnchanged() {
    List<Row> rows = Arrays.asList(RowFactory.create(2, 2), RowFactory.create(1, 1));
    Dataset<Row> df = buildTwoIntColumnFrame(rows);

    // Ordering by a column absent from the schema must be a no-op that returns the same frame.
    Dataset<Row> ordered = SpaceCurveSortingHelper.orderDataFrameByMappingValues(
        df, HoodieClusteringConfig.LayoutOptimizationStrategy.ZORDER, Arrays.asList("missing"), 1);

    Assertions.assertSame(df, ordered, "missing order column must return the untouched input frame");
  }
}
