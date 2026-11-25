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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getSimpleSchema;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Reads a multi-block log file through {@link PrefetchedLogFileStorage} and checks the blocks match a read
 * through the file system.
 */
public class TestPrefetchedLogFileStorage {

  @ParameterizedTest
  @EnumSource(names = {"AVRO_DATA_BLOCK", "PARQUET_DATA_BLOCK"})
  public void testReadMultipleBlocks(HoodieLogBlockType dataBlockType, @TempDir Path tempDir)
      throws IOException, InterruptedException, URISyntaxException {
    StoragePath dir = new StoragePath(tempDir.toUri().toString());
    HoodieStorage storage = HoodieStorageUtils.getStorage(dir.toString(), HoodieTestUtils.getDefaultStorageConf());
    HoodieLogFormat.Writer writer = HoodieLogFormatWriter.builder()
        .withParentPath(dir)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
        .withLogFileId("prefetch")
        .withInstantTime("100")
        .withStorage(storage)
        .build();
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
    writer.appendBlock(HoodieCommonTestHarness.getDataBlock(dataBlockType, SchemaTestUtil.generateTestRecords(0, 100), header));
    List<Pair<DeleteRecord, Long>> deletes = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      deletes.add(Pair.of(DeleteRecord.create("key" + i, "partition"), (long) i));
    }
    writer.appendBlock(new HoodieDeleteBlock(deletes, header));
    writer.appendBlock(HoodieCommonTestHarness.getDataBlock(dataBlockType, SchemaTestUtil.generateTestRecords(100, 50), header));
    writer.close();

    StoragePath logPath = writer.getLogFile().getPath();
    HoodieLogFile logFile = new HoodieLogFile(logPath, storage.getPathInfo(logPath).getLength());
    List<String> expected = readBlocks(storage, logFile);
    assertEquals(3, expected.size());

    // The whole file is fetched into memory; the reader still has to find the end of the file after the last block
    try (PrefetchedLogFileStorage prefetched = PrefetchedLogFileStorage.prefetch(
        storage, Collections.singletonList(logFile), Long.MAX_VALUE)) {
      assertEquals(expected, readBlocks(prefetched, logFile));
    }
    // A file over the budget is read through the wrapped storage
    try (PrefetchedLogFileStorage prefetched = PrefetchedLogFileStorage.prefetch(
        storage, Collections.singletonList(logFile), 1)) {
      assertEquals(expected, readBlocks(prefetched, logFile));
    }
  }

  private static List<String> readBlocks(HoodieStorage storage, HoodieLogFile logFile) throws IOException {
    List<String> blocks = new ArrayList<>();
    try (HoodieLogFileReader reader = new HoodieLogFileReader(storage, logFile, getSimpleSchema(), 4096)) {
      while (reader.hasNext()) {
        HoodieLogBlock block = reader.next();
        int count = 0;
        if (block instanceof HoodieDeleteBlock) {
          count = ((HoodieDeleteBlock) block).getRecordsToDelete().length;
        } else {
          try (ClosableIterator<HoodieRecord<Object>> records =
                   ((HoodieDataBlock) block).getRecordIterator(HoodieRecord.HoodieRecordType.AVRO)) {
            while (records.hasNext()) {
              records.next();
              count++;
            }
          }
        }
        blocks.add(block.getBlockType() + ":" + count);
      }
    }
    return blocks;
  }
}
