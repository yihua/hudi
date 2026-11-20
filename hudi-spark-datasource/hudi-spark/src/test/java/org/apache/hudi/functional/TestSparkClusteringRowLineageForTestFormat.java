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

package org.apache.hudi.functional;

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.clustering.run.strategy.SingleSparkJobExecutionStrategy;
import org.apache.hudi.client.clustering.run.strategy.SparkBinaryCopyClusteringExecutionStrategy;
import org.apache.hudi.client.functional.TestSparkCopyOnWriteRowLineageForTestFormat;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.ClusteringGroupInfo;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.table.HoodieSparkTable;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Functional tests for the row lineage columns of an external table format, see
 * {@link org.apache.hudi.common.HoodieTableFormat#getRowLineageColumns()}, across clustering of a copy-on-write
 * table, which needs the Spark version adapter of the Spark datasource modules.
 */
@Tag("functional")
class TestSparkClusteringRowLineageForTestFormat extends TestSparkCopyOnWriteRowLineageForTestFormat {

  private static final String PARTITION = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;

  /**
   * Binary copy clustering copies the row groups of the base files, so it cannot add the row lineage columns.
   */
  @Test
  void testBinaryCopyClusteringIsRefused() throws IOException {
    HoodieTableMetaClient metaClient = initTable();
    HoodieWriteConfig config = buildConfig(metaClient, getRecordType(), 0, HoodieClusteringConfig.newBuilder()
        .withClusteringExecutionStrategyClass(SparkBinaryCopyClusteringExecutionStrategy.class.getName()).build());
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION});
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      write(client, toRecordType(dataGen.generateInsertsForPartition("001", 10, PARTITION), getRecordType()));
      write(client, toRecordType(dataGen.generateInsertsForPartition("002", 10, PARTITION), getRecordType()));
      Option<String> clusteringInstant = client.scheduleClustering(Option.empty());
      assertTrue(clusteringInstant.isPresent());
      Throwable failure = assertThrows(Exception.class, () -> client.cluster(clusteringInstant.get(), true));
      assertTrue(exceptionChainContains(failure, refusalMessage(SparkBinaryCopyClusteringExecutionStrategy.class)), () -> "Unexpected failure: " + failure);
    }
  }

  /**
   * The strategies that run a single Spark job write the records they read with the schema of the table.
   */
  @Test
  void testSingleSparkJobClusteringIsRefused() throws IOException {
    HoodieTableMetaClient metaClient = initTable();
    HoodieWriteConfig config = buildConfig(metaClient, getRecordType(), 0, HoodieClusteringConfig.newBuilder().build());
    TestSingleSparkJobExecutionStrategy strategy = new TestSingleSparkJobExecutionStrategy(HoodieSparkTable.create(config, context(), metaClient), context(), config);
    Throwable failure = assertThrows(HoodieNotSupportedException.class,
        () -> strategy.performClustering(new HoodieClusteringPlan(), HoodieSchema.parse(config.getSchema()), "001"));
    assertTrue(failure.getMessage().contains(refusalMessage(TestSingleSparkJobExecutionStrategy.class)), () -> "Unexpected failure: " + failure);
  }

  private static class TestSingleSparkJobExecutionStrategy extends SingleSparkJobExecutionStrategy<Object> {
    TestSingleSparkJobExecutionStrategy(HoodieSparkTable<?> table, HoodieEngineContext context, HoodieWriteConfig config) {
      super(table, context, config);
    }

    @Override
    protected List<WriteStatus> performClusteringForGroup(ReaderContextFactory<Object> readerContextFactory, ClusteringGroupInfo clusteringGroup,
                                                          Map<String, String> strategyParams, boolean preserveHoodieMetadata, HoodieSchema schema,
                                                          TaskContextSupplier taskContextSupplier, String instantTime) {
      throw new UnsupportedOperationException();
    }
  }

  private static String refusalMessage(Class<?> strategyClass) {
    return "Table format test-format declares row lineage columns, but " + strategyClass.getSimpleName()
        + " does not keep the row lineage of the records it rewrites";
  }

  private static boolean exceptionChainContains(Throwable throwable, String text) {
    for (Throwable current = throwable; current != null; current = current.getCause()) {
      if (current.getMessage() != null && current.getMessage().contains(text)) {
        return true;
      }
    }
    return false;
  }
}
