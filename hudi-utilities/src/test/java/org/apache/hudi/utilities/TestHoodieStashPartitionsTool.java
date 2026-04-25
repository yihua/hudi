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

package org.apache.hudi.utilities;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.validator.DefaultStashPartitionRenameHelper;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HoodieStashPartitionsTool}.
 */
public class TestHoodieStashPartitionsTool extends SparkClientFunctionalTestHarness {

  private HoodieTableMetaClient metaClient;
  private HoodieTestDataGenerator dataGen;
  private String stashPath;

  @BeforeEach
  public void init() throws IOException {
    metaClient = getHoodieMetaClient(HoodieTableType.COPY_ON_WRITE);
    dataGen = new HoodieTestDataGenerator();
    Path stashDir = tempDir.resolve("stash");
    Files.createDirectories(stashDir);
    stashPath = stashDir.toAbsolutePath().toUri().toString();
  }

  /**
   * Given: A COW table with data in multiple partitions.
   * When: Stash tool runs in stash mode for one partition.
   * Then: Files are moved to stash location, partition is deleted from table,
   *       and a replace commit exists on the timeline.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testStashSinglePartition(boolean enableMetadata) throws IOException {
    insertRecords("001", 20, enableMetadata);

    HoodieStorage storage = hoodieStorage();
    StoragePath sourcePartition = new StoragePath(basePath(), DEFAULT_FIRST_PARTITION_PATH);
    StoragePath stashPartition = new StoragePath(stashPath, DEFAULT_FIRST_PARTITION_PATH);

    assertTrue(storage.exists(sourcePartition));
    assertFalse(storage.listDirectEntries(sourcePartition).isEmpty());

    HoodieStashPartitionsTool.Config cfg = buildConfig(DEFAULT_FIRST_PARTITION_PATH, "stash", enableMetadata);
    new HoodieStashPartitionsTool(jsc(), cfg).run();

    assertTrue(storage.exists(stashPartition));
    assertFalse(storage.listDirectEntries(stashPartition).isEmpty());
    assertSourcePartitionEmpty(storage, sourcePartition);

    HoodieTableMetaClient reloadedMetaClient = HoodieTableMetaClient.reload(metaClient);
    assertFalse(reloadedMetaClient.getActiveTimeline().getCompletedReplaceTimeline().empty());

    // Other partitions should be untouched
    StoragePath secondPartition = new StoragePath(basePath(), DEFAULT_SECOND_PARTITION_PATH);
    assertTrue(storage.exists(secondPartition));
    assertFalse(storage.listDirectEntries(secondPartition).isEmpty());
  }

  /**
   * Given: A COW table with data, and one partition already stashed.
   * When: Stash tool runs in stash mode for the same partition again (retry/MDT case).
   * Then: The validator skips the already-stashed partition gracefully.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testStashAlreadyStashedPartitionIsIdempotent(boolean enableMetadata) throws IOException {
    insertRecords("001", 20, enableMetadata);

    HoodieStashPartitionsTool.Config cfg = buildConfig(DEFAULT_FIRST_PARTITION_PATH, "stash", enableMetadata);
    new HoodieStashPartitionsTool(jsc(), cfg).run();

    HoodieStorage storage = hoodieStorage();
    StoragePath stashPartition = new StoragePath(stashPath, DEFAULT_FIRST_PARTITION_PATH);
    List<StoragePathInfo> stashFilesAfterFirst = storage.listDirectEntries(stashPartition);

    // Run again
    new HoodieStashPartitionsTool(jsc(), cfg).run();

    List<StoragePathInfo> stashFilesAfterSecond = storage.listDirectEntries(stashPartition);
    assertEquals(stashFilesAfterFirst.size(), stashFilesAfterSecond.size());
  }

  /**
   * Given: A COW table with data in multiple partitions.
   * When: Stash tool runs in stash mode for multiple partitions.
   * Then: All specified partitions are moved to stash location.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testStashMultiplePartitions(boolean enableMetadata) throws IOException {
    insertRecords("001", 30, enableMetadata);

    String partitions = DEFAULT_FIRST_PARTITION_PATH + "," + DEFAULT_SECOND_PARTITION_PATH;
    HoodieStashPartitionsTool.Config cfg = buildConfig(partitions, "stash", enableMetadata);
    new HoodieStashPartitionsTool(jsc(), cfg).run();

    HoodieStorage storage = hoodieStorage();
    for (String partition : new String[]{DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH}) {
      StoragePath stashPartition = new StoragePath(stashPath, partition);
      assertTrue(storage.exists(stashPartition));
      assertFalse(storage.listDirectEntries(stashPartition).isEmpty());
      assertSourcePartitionEmpty(storage, new StoragePath(basePath(), partition));
    }

    // Third partition untouched
    StoragePath thirdPartition = new StoragePath(basePath(), DEFAULT_THIRD_PARTITION_PATH);
    assertTrue(storage.exists(thirdPartition));
    assertFalse(storage.listDirectEntries(thirdPartition).isEmpty());
  }

  /**
   * Given: A partition has been stashed.
   * When: Rollback stash is run.
   * Then: Files are copied back from stash to the original source location.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRollbackStash(boolean enableMetadata) throws IOException {
    insertRecords("001", 20, enableMetadata);

    HoodieStorage storage = hoodieStorage();
    StoragePath sourcePartition = new StoragePath(basePath(), DEFAULT_FIRST_PARTITION_PATH);
    List<String> originalFileNames = storage.listDirectEntries(sourcePartition).stream()
        .map(info -> info.getPath().getName()).sorted().collect(Collectors.toList());

    new HoodieStashPartitionsTool(jsc(), buildConfig(DEFAULT_FIRST_PARTITION_PATH, "stash", enableMetadata)).run();
    assertSourcePartitionEmpty(storage, sourcePartition);

    new HoodieStashPartitionsTool(jsc(), buildConfig(DEFAULT_FIRST_PARTITION_PATH, "rollback_stash", enableMetadata)).run();

    assertTrue(storage.exists(sourcePartition));
    List<String> restoredFileNames = storage.listDirectEntries(sourcePartition).stream()
        .map(info -> info.getPath().getName()).sorted().collect(Collectors.toList());
    assertEquals(originalFileNames, restoredFileNames);

    StoragePath stashPartition = new StoragePath(stashPath, DEFAULT_FIRST_PARTITION_PATH);
    assertTrue(!storage.exists(stashPartition) || storage.listDirectEntries(stashPartition).isEmpty());
  }

  /**
   * Given: No partition has been stashed.
   * When: Rollback stash is run.
   * Then: Tool completes without error (no-op).
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRollbackStashWithNothingToRollback(boolean enableMetadata) throws IOException {
    insertRecords("001", 10, enableMetadata);

    HoodieStashPartitionsTool.Config cfg = buildConfig(DEFAULT_FIRST_PARTITION_PATH, "rollback_stash", enableMetadata);
    new HoodieStashPartitionsTool(jsc(), cfg).run();
  }

  /**
   * Given: A COW table with data.
   * When: Dry run mode is executed.
   * Then: No files are moved, no commits created.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testDryRunDoesNotModifyTable(boolean enableMetadata) throws IOException {
    insertRecords("001", 20, enableMetadata);

    HoodieStorage storage = hoodieStorage();
    StoragePath sourcePartition = new StoragePath(basePath(), DEFAULT_FIRST_PARTITION_PATH);
    List<StoragePathInfo> filesBefore = storage.listDirectEntries(sourcePartition);
    int commitCountBefore = metaClient.getActiveTimeline().countInstants();

    new HoodieStashPartitionsTool(jsc(), buildConfig(DEFAULT_FIRST_PARTITION_PATH, "dry_run", enableMetadata)).run();

    List<StoragePathInfo> filesAfter = storage.listDirectEntries(sourcePartition);
    assertEquals(filesBefore.size(), filesAfter.size());

    StoragePath stashPartition = new StoragePath(stashPath, DEFAULT_FIRST_PARTITION_PATH);
    assertFalse(storage.exists(stashPartition) && !storage.listDirectEntries(stashPartition).isEmpty());

    HoodieTableMetaClient reloaded = HoodieTableMetaClient.reload(metaClient);
    assertEquals(commitCountBefore, reloaded.getActiveTimeline().countInstants());
  }

  /**
   * Scenario (a): Stash for multiple partitions was started. Operation failed before reaching
   * the pre-commit validator (no files moved, no commit). Retry should succeed without issues.
   *
   * We simulate this by manually creating a requested replace commit instant (as if startCommit
   * ran but deletePartitions failed), then retrying with the tool.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRetryAfterFailureBeforePreCommitValidator(boolean enableMetadata) throws IOException {
    insertRecords("001", 30, enableMetadata);

    HoodieStorage storage = hoodieStorage();
    String partitions = DEFAULT_FIRST_PARTITION_PATH + "," + DEFAULT_SECOND_PARTITION_PATH;

    // Verify all partitions have data and nothing is in stash
    for (String partition : new String[]{DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH}) {
      StoragePath src = new StoragePath(basePath(), partition);
      assertTrue(storage.exists(src));
      assertFalse(storage.listDirectEntries(src).isEmpty());
    }

    // The first attempt "failed" before reaching the validator — no files moved, no commit.
    // This is the initial state. Just retry the tool — it should work from scratch.
    HoodieStashPartitionsTool.Config cfg = buildConfig(partitions, "stash", enableMetadata);
    new HoodieStashPartitionsTool(jsc(), cfg).run();

    // Verify success
    for (String partition : new String[]{DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH}) {
      StoragePath stashPartition = new StoragePath(stashPath, partition);
      assertTrue(storage.exists(stashPartition));
      assertFalse(storage.listDirectEntries(stashPartition).isEmpty());
      assertSourcePartitionEmpty(storage, new StoragePath(basePath(), partition));
    }
  }

  /**
   * Scenario (b): Stash for multiple partitions was started. The pre-commit validator
   * stashed some partitions (moved files) but crashed before completing. On retry, the
   * tool's pre-check (non-MDT) should recover partial state, and the validator (MDT)
   * should handle it. The stash should eventually succeed.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRetryAfterValidatorPartiallyStashedAndCrashed(boolean enableMetadata) throws IOException {
    insertRecords("001", 30, enableMetadata);

    HoodieStorage storage = hoodieStorage();
    String[] partitions = {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH};

    // Simulate partial stash: move first partition's files to stash (as if validator did it),
    // but no commit landed.
    DefaultStashPartitionRenameHelper renameHelper = new DefaultStashPartitionRenameHelper();
    StoragePath src = new StoragePath(basePath(), DEFAULT_FIRST_PARTITION_PATH);
    StoragePath dest = new StoragePath(stashPath, DEFAULT_FIRST_PARTITION_PATH);
    renameHelper.movePartitionFiles(storage, src, dest);

    // Verify partial state: first partition in stash, others untouched
    assertTrue(storage.exists(dest));
    assertFalse(storage.listDirectEntries(dest).isEmpty());
    assertSourcePartitionEmpty(storage, src);

    // Retry with the tool — should handle partial state and complete all partitions
    String allPartitions = String.join(",", partitions);
    HoodieStashPartitionsTool.Config cfg = buildConfig(allPartitions, "stash", enableMetadata);
    new HoodieStashPartitionsTool(jsc(), cfg).run();

    // Verify: all partitions stashed
    for (String partition : partitions) {
      StoragePath stashPartition = new StoragePath(stashPath, partition);
      assertTrue(storage.exists(stashPartition), "Stash should exist for: " + partition);
      assertFalse(storage.listDirectEntries(stashPartition).isEmpty(),
          "Stash should have files for: " + partition);
      assertSourcePartitionEmpty(storage, new StoragePath(basePath(), partition));
    }

    HoodieTableMetaClient reloaded = HoodieTableMetaClient.reload(metaClient);
    assertFalse(reloaded.getActiveTimeline().getCompletedReplaceTimeline().empty());
  }

  /**
   * Scenario (d): Pre-commit validator succeeded for all partitions (files moved), but
   * crashed just after that (before the commit landed). On retry, the tool should handle
   * the state where all files are in stash but no commit exists, and succeed.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRetryAfterValidatorSucceededButCrashedBeforeCommit(boolean enableMetadata) throws IOException {
    insertRecords("001", 30, enableMetadata);

    HoodieStorage storage = hoodieStorage();
    String[] partitions = {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH};

    // Simulate: validator moved all files to stash, but commit never landed
    DefaultStashPartitionRenameHelper renameHelper = new DefaultStashPartitionRenameHelper();
    for (String partition : partitions) {
      StoragePath src = new StoragePath(basePath(), partition);
      StoragePath dest = new StoragePath(stashPath, partition);
      if (storage.exists(src) && !storage.listDirectEntries(src).isEmpty()) {
        renameHelper.movePartitionFiles(storage, src, dest);
      }
    }

    // Verify: all files in stash, source empty, NO commit on timeline
    for (String partition : partitions) {
      StoragePath stashPartition = new StoragePath(stashPath, partition);
      assertTrue(storage.exists(stashPartition));
      assertFalse(storage.listDirectEntries(stashPartition).isEmpty());
      assertSourcePartitionEmpty(storage, new StoragePath(basePath(), partition));
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertTrue(metaClient.getActiveTimeline().getCompletedReplaceTimeline().empty(),
        "No replace commit should exist yet");

    // Retry with the tool
    String allPartitions = String.join(",", partitions);
    HoodieStashPartitionsTool.Config cfg = buildConfig(allPartitions, "stash", enableMetadata);
    new HoodieStashPartitionsTool(jsc(), cfg).run();

    // Verify: commit exists now
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertFalse(metaClient.getActiveTimeline().getCompletedReplaceTimeline().empty());

    // Stash files should still be there
    for (String partition : partitions) {
      StoragePath stashPartition = new StoragePath(stashPath, partition);
      assertTrue(storage.exists(stashPartition));
      assertFalse(storage.listDirectEntries(stashPartition).isEmpty());
    }
  }

  // ---- Helper methods ----

  private void insertRecords(String commitTime, int numRecords, boolean enableMetadata) throws IOException {
    HoodieWriteConfig writeConfig = getConfigBuilder(false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(enableMetadata).build())
        .build();
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig)) {
      List<HoodieRecord> records = dataGen.generateInserts(commitTime, numRecords);
      JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 2);
      WriteClientTestUtils.startCommitWithTime(writeClient, commitTime);
      JavaRDD<WriteStatus> statusesRdd = writeClient.insert(writeRecords, commitTime);
      List<WriteStatus> statuses = statusesRdd.collect();
      assertNoWriteErrors(statuses);
      writeClient.commit(commitTime, jsc().parallelize(statuses));
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
  }

  private void assertSourcePartitionEmpty(HoodieStorage storage, StoragePath sourcePartition) throws IOException {
    if (storage.exists(sourcePartition)) {
      List<StoragePathInfo> remainingFiles = storage.listDirectEntries(sourcePartition).stream()
          .filter(f -> !f.getPath().getName().startsWith("."))
          .collect(Collectors.toList());
      assertTrue(remainingFiles.isEmpty(),
          "Source partition should have no data files after stash, but found: " + remainingFiles);
    }
  }

  private HoodieStashPartitionsTool.Config buildConfig(String partitions, String mode, boolean enableMetadata) {
    HoodieStashPartitionsTool.Config cfg = new HoodieStashPartitionsTool.Config();
    cfg.basePath = basePath();
    cfg.tableName = "test-trip-table";
    cfg.stashPath = stashPath;
    cfg.partitions = partitions;
    cfg.runningMode = mode;
    cfg.parallelism = 2;
    if (enableMetadata) {
      cfg.configs.add(HoodieMetadataConfig.ENABLE.key() + "=true");
    } else {
      cfg.configs.add(HoodieMetadataConfig.ENABLE.key() + "=false");
    }
    return cfg;
  }
}
