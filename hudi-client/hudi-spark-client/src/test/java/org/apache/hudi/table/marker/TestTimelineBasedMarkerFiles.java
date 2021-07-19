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

package org.apache.hudi.table.marker;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView;
import org.apache.hudi.common.testutils.FileSystemTestUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.timeline.service.TimelineService;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

public class TestTimelineBasedMarkerFiles extends TestMarkerFiles {
  TimelineService timelineService;

  @BeforeEach
  public void setup() throws IOException {
    initPath();
    initMetaClient();
    this.jsc = new JavaSparkContext(
        HoodieClientTestUtils.getSparkConfForTest(TestTimelineBasedMarkerFiles.class.getName()));
    this.context = new HoodieSparkEngineContext(jsc);
    this.fs = FSUtils.getFs(metaClient.getBasePath(), metaClient.getHadoopConf());
    this.markerFolderPath =  new Path(metaClient.getMarkerFolderPath("000"));

    FileSystemViewStorageConfig storageConf =
        FileSystemViewStorageConfig.newBuilder().withStorageType(FileSystemViewStorageType.SPILLABLE_DISK).build();
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().build();
    HoodieLocalEngineContext localEngineContext = new HoodieLocalEngineContext(metaClient.getHadoopConf());

    try {
      timelineService = new TimelineService(localEngineContext, 0,
          FileSystemViewManager.createViewManager(localEngineContext, metadataConfig, storageConf));
      timelineService.startService();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    RemoteHoodieTableFileSystemView view =
        new RemoteHoodieTableFileSystemView("localhost", timelineService.getServerPort(), metaClient);
    this.markerFiles = new TimelineBasedMarkerFiles(
        metaClient.getBasePath(), markerFolderPath.toString(), "000", view);
  }

  @AfterEach
  public void cleanup() {
    if (timelineService != null) {
      timelineService.close();
    }
    jsc.stop();
    context = null;
  }

  @Override
  void verifyMarkersInFileSystem() throws IOException {
    List<String> markerFiles = FileSystemTestUtils.listRecursive(fs, markerFolderPath)
        .stream().filter(status -> status.getPath().getName().contains("MARKER"))
        .flatMap(status -> {
          // Read all markers stored in each marker file maintained by the timeline service
          FSDataInputStream fsDataInputStream = null;
          BufferedReader bufferedReader = null;
          List<String> markers = null;
          try {
            fsDataInputStream = fs.open(status.getPath());
            bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream, StandardCharsets.UTF_8));
            markers = bufferedReader.lines().collect(Collectors.toList());
          } catch (IOException e) {
            e.printStackTrace();
          } finally {
            closeQuietly(bufferedReader);
            closeQuietly(fsDataInputStream);
          }
          return markers.stream();
        })
        .sorted()
        .collect(Collectors.toList());
    assertEquals(3, markerFiles.size());
    assertIterableEquals(CollectionUtils.createImmutableList(
        "2020/06/01/file1.marker.MERGE",
        "2020/06/02/file2.marker.APPEND",
        "2020/06/03/file3.marker.CREATE"),
        markerFiles);
  }

  /**
   * Closes {@code Closeable} quietly.
   *
   * @param closeable {@code Closeable} to close
   */
  private void closeQuietly(Closeable closeable) {
    if (closeable == null) {
      return;
    }
    try {
      closeable.close();
    } catch (IOException e) {
      // Ignore
    }
  }
}
