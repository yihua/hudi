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

package org.apache.hudi.io.storage.row;

import org.apache.hudi.testutils.HoodieClientTestBase;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Coverage for {@link HoodieRowParquetWriteSupport#resolveSessionLocalTimeZone()}:
 * the helper must return the SQLConf default (JVM default timezone) when no
 * override is set on the SparkSession or SparkConf, both on the driver and
 * inside Spark task closures.
 */
public class TestHoodieRowParquetWriteSupport extends HoodieClientTestBase {

  @Test
  public void testResolveSessionLocalTimeZoneWithoutOverride() {
    String expected = TimeZone.getDefault().getID();

    // Driver thread.
    assertEquals(expected, HoodieRowParquetWriteSupport.resolveSessionLocalTimeZone(),
        "driver-side helper did not return the JVM default timezone");

    // Executor task threads via vanilla parallelize().map() — outside any
    // Spark SQL execution context — exercise the SparkEnv-fallback branch.
    List<String> seen = jsc.parallelize(Arrays.asList(1, 2, 3, 4), 4)
        .map(i -> HoodieRowParquetWriteSupport.resolveSessionLocalTimeZone())
        .collect();
    for (int i = 0; i < seen.size(); i++) {
      assertEquals(expected, seen.get(i),
          "executor task #" + i + " resolved sessionLocalTimeZone to '" + seen.get(i)
              + "' with no overrides; expected the JVM default ('" + expected + "')");
    }
  }
}
