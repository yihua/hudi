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

package org.apache.hudi.io;

import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieMetadataWriteUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.HoodieSparkTable;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.table.action.compact.strategy.CompactionStrategy.TOTAL_IO_MB;
import static org.apache.hudi.table.action.compact.strategy.CompactionStrategy.TOTAL_LOG_FILES;

public class TestFileGroupReaderBasedMergeHandle {
  private static final Logger LOG = LoggerFactory.getLogger(TestFileGroupReaderBasedMergeHandle.class);

  @Test
  public void testRunCompaction() {
    String basePath = "/Users/ethan/Work/tmp/20251114-secondary-index/table/.hoodie/metadata";
    String instantTime = getStrictlyHigherTimestamp("20251014090234067");
    StorageConfiguration storageConf = HoodieTestUtils.getDefaultStorageConf();
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf)
        .setBasePath(basePath)
        .setLoadActiveTimelineOnLoad(true)
        .build();
    HoodieWriteConfig writeConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(
        HoodieWriteConfig.newBuilder()
            .withPath("/Users/ethan/Work/tmp/20251114-secondary-index/table")
            .withMemoryConfig(
                HoodieMemoryConfig.newBuilder()
                    .withMaxMemoryMaxSize(200000000, 200000000)
                    .build())
            .build(),
        HoodieFailedWritesCleaningPolicy.EAGER, HoodieTableVersion.NINE);
    CompactionOperation compactionOperation = buildCompactionOperation();
    HoodieReaderContext readerContext = new HoodieLocalEngineContext(storageConf).getReaderContextFactoryForWrite(
        metaClient, HoodieRecord.HoodieRecordType.AVRO, writeConfig.getProps()).getContext();

    HoodieSparkTable<?> table = HoodieSparkTable.create(
        writeConfig, new HoodieLocalEngineContext(storageConf), metaClient);

    long preMergeTimeMs = System.currentTimeMillis();
    FileGroupReaderBasedMergeHandle mergeHandle = new FileGroupReaderBasedMergeHandle(
        writeConfig, instantTime, table, compactionOperation, new LocalTaskContextSupplier(),
        readerContext, "20251014085650395", HoodieRecord.HoodieRecordType.SPARK);
    mergeHandle.doMerge();
    mergeHandle.close();
    LOG.warn("File group reader-based MERGE took: {} ms", System.currentTimeMillis() - preMergeTimeMs);
  }

  private static String getStrictlyHigherTimestamp(String timestamp) {
    long ts = Long.parseLong(timestamp);
    ValidationUtils.checkArgument(ts > 0, "Timestamp must be positive");
    long higher = ts + (long) (1000 * Math.random()) + 1;
    return "" + higher;
  }

  private static CompactionOperation buildCompactionOperation() {
    Map<String, Double> metrics = new HashMap<>();
    metrics.put(TOTAL_LOG_FILES, 1.0);
    metrics.put(TOTAL_IO_MB, 0.0);
    metrics.put("TOTAL_LOG_FILES_SIZE", 400000000.0);
    metrics.put("TOTAL_IO_READ_MB", 0.0);
    metrics.put("TOTAL_IO_WRITE_MB", 0.0);
    return new CompactionOperation(
        "secondary-index-key-0008-0", "secondary_index_key", "20251014084908397",
        Option.of("20251014084908397"),
        Collections.singletonList("/Users/ethan/Work/tmp/20251114-secondary-index/table/.hoodie/metadata/secondary_index_key/.secondary-index-key-0008-0_20251014085650395.log.1_49-176-40052"),
        Option.of("/Users/ethan/Work/tmp/20251114-secondary-index/table/.hoodie/metadata/secondary_index_key/secondary-index-key-0008-0_49-132-30288_20251014084908397.hfile"),
        Option.empty(),
        metrics);
  }
}
