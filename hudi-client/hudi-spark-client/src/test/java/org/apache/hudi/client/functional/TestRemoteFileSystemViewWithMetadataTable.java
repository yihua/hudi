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

package org.apache.hudi.client.functional;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieBackedTestDelayedTableMetadata;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.hudi.timeline.service.TimelineService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRemoteFileSystemViewWithMetadataTable extends HoodieClientTestHarness {
  private static final Logger LOG = LogManager.getLogger(TestRemoteFileSystemViewWithMetadataTable.class);

  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    initSparkContexts();
    initFileSystem();
    initMetaClient();
    initTimelineService();
    dataGen = new HoodieTestDataGenerator(0x1f86);
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupTimelineService();
    cleanupClients();
    cleanupSparkContexts();
    cleanupFileSystem();
    cleanupExecutorService();
    dataGen = null;
    System.gc();
  }

  @Override
  public void initTimelineService() {
    // Start a timeline server that are running across multiple commits
    HoodieLocalEngineContext localEngineContext = new HoodieLocalEngineContext(metaClient.getHadoopConf());

    try {
      HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
          .withPath(basePath)
          .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
              .withRemoteServerPort(incrementTimelineServicePortToUse()).build())
          .build();
      timelineService = new TimelineService(localEngineContext, new Configuration(),
          TimelineService.Config.builder().enableMarkerRequests(true)
              .serverPort(config.getViewStorageConfig().getRemoteViewServerPort()).build(),
          FileSystem.get(new Configuration()),
          FileSystemViewManager.createViewManager(
              context, config.getMetadataConfig(), config.getViewStorageConfig(),
              config.getCommonConfig(),
              () -> new HoodieBackedTestDelayedTableMetadata(
                  context, config.getMetadataConfig(), basePath,
                  config.getViewStorageConfig().getSpillableDir(), true)));
      timelineService.startService();
      timelineServicePort = timelineService.getServerPort();
      LOG.info("Started timeline server on port: " + timelineServicePort);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Test
  public void testGetLatestFileSliceWithMetadataTable() throws IOException {
    // This test utilizes the `HoodieBackedTestDelayedTableMetadata` to make sure the
    // synced file system view is always served.
    for (int i = 0; i < 3; i++) {
      writeToTable(i, timelineService);
    }

    HoodieTableMetaClient newMetaClient = HoodieTableMetaClient.builder()
        .setConf(metaClient.getHadoopConf())
        .setBasePath(basePath)
        .build();
    HoodieActiveTimeline timeline = newMetaClient.getActiveTimeline();
    HoodieInstant compactionCommit = timeline.lastInstant().get();
    assertTrue(timeline.lastInstant().get().getAction().equals(COMMIT_ACTION));
    HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
        timeline.getInstantDetails(compactionCommit).get(), HoodieCommitMetadata.class);
    List<Pair<String, String>> partitionFileIdPairList =
        commitMetadata.getPartitionToWriteStats().entrySet().stream().flatMap(
            entry -> {
              String partitionPath = entry.getKey();
              return entry.getValue().stream().map(
                  writeStat -> Pair.of(partitionPath, writeStat.getFileId()));
            }
        ).collect(Collectors.toList());
    List<Pair<String, String>> lookupList = new ArrayList<>();
    while (lookupList.size() < 128) {
      lookupList.addAll(partitionFileIdPairList);
    }

    LOG.info("Connecting to Timeline Server: " + timelineService.getServerPort());
    RemoteHoodieTableFileSystemView view = new RemoteHoodieTableFileSystemView(
        "localhost", timelineService.getServerPort(), metaClient);

    List<TestViewLookUpCallable> callableList = lookupList.stream()
        .map(pair -> new TestViewLookUpCallable(view, pair, compactionCommit.getTimestamp()))
        .collect(Collectors.toList());
    List<Future<Boolean>> resultList = new ArrayList<>();

    ExecutorService pool = Executors.newCachedThreadPool();
    callableList.forEach(callable -> {
      resultList.add(pool.submit(callable));
    });

    assertTrue(resultList.stream().map(future -> {
      try {
        return future.get();
      } catch (Exception e) {
        LOG.error(e);
        return false;
      }
    }).reduce((a, b) -> a && b).get());
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

  private void writeToTable(int round, TimelineService timelineService) throws IOException {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .withFinalizeWriteParallelism(2)
        .withDeleteParallelism(2)
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .withMergeSmallFileGroupCandidatesLimit(0)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(3)
            .build())
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.REMOTE_ONLY)
            .withRemoteServerPort(timelineService.getServerPort())
            .build())
        .withAutoCommit(false)
        .forTable("test_mor_table")
        .build();
    // do one upsert with synchronous metadata update
    SparkRDDWriteClient writeClient =
        new SparkRDDWriteClient(context, writeConfig, Option.of(timelineService));
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    writeClient.startCommitWithTime(instantTime);
    List<HoodieRecord> records = round == 0
        ? dataGen.generateInserts(instantTime, 100)
        : dataGen.generateUpdates(instantTime, 100);

    JavaRDD<WriteStatus> writeStatusRDD = writeClient.upsert(jsc.parallelize(records, 1), instantTime);
    writeClient.commit(instantTime, writeStatusRDD, Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());
    writeClient.scheduleCompaction(Option.empty());
    writeClient.runAnyPendingCompactions();
  }

  class TestViewLookUpCallable implements Callable<Boolean> {
    private final RemoteHoodieTableFileSystemView view;
    private final Pair<String, String> partitionFileIdPair;
    private final String expectedCommitTime;

    public TestViewLookUpCallable(
        RemoteHoodieTableFileSystemView view,
        Pair<String, String> partitionFileIdPair,
        String expectedCommitTime) {
      this.view = view;
      this.partitionFileIdPair = partitionFileIdPair;
      this.expectedCommitTime = expectedCommitTime;
    }

    @Override
    public Boolean call() throws Exception {
      Option<FileSlice> latestFileSlice = view.getLatestFileSlice(
          partitionFileIdPair.getLeft(), partitionFileIdPair.getRight());
      return latestFileSlice.isPresent() && expectedCommitTime.equals(
          FSUtils.getCommitTime(new Path(latestFileSlice.get().getBaseFile().get().getPath()).getName()));
    }
  }
}