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

package org.apache.hudi.testutils;

import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility for injecting Gluten/Velox native execution into a test {@link SparkConf}.
 *
 * <p>Activated by passing {@code -Dgluten.bundle.jar=<path>} at test time.
 * If {@code ai.onehouse.quanton.QuantonPlugin} is on the classpath it is preferred;
 * otherwise {@code org.apache.gluten.GlutenPlugin} is used.
 */
public class GlutenTestUtils {

  private static final Logger LOG = LoggerFactory.getLogger(GlutenTestUtils.class);

  private GlutenTestUtils() {}

  /**
   * Applies Gluten/Velox native-execution settings to {@code sparkConf} when the
   * {@code gluten.bundle.jar} system property is set.  No-op otherwise.
   */
  public static void applyGlutenConf(SparkConf sparkConf) {
    String glutenBundleJar = System.getProperty("gluten.bundle.jar");
    if (glutenBundleJar == null || glutenBundleJar.isEmpty()) {
      return;
    }

    String pluginClass;
    try {
      Class.forName("ai.onehouse.quanton.QuantonPlugin");
      pluginClass = "ai.onehouse.quanton.QuantonPlugin";
    } catch (ClassNotFoundException e) {
      pluginClass = "org.apache.gluten.GlutenPlugin";
    }

    String confPrefix = pluginClass.contains("quanton") ? "spark.quanton" : "spark.gluten";
    String libName    = pluginClass.contains("quanton") ? "quanton"       : "gluten";

    sparkConf.set("spark.plugins",                pluginClass);
    sparkConf.set("spark.memory.offHeap.enabled", "true");
    sparkConf.set("spark.memory.offHeap.size",    System.getProperty("gluten.offheap.size", "8g"));
    sparkConf.set("spark.shuffle.manager",        "org.apache.spark.shuffle.sort.ColumnarShuffleManager");
    sparkConf.set(confPrefix + ".sql.columnar.libname",                          libName);
    sparkConf.set(confPrefix + ".sql.columnar.backend.velox.glogSeverityLevel", "0");

    LOG.warn("Using Gluten/Velox native execution with plugin={}, lib={}", pluginClass, libName);
  }
}
