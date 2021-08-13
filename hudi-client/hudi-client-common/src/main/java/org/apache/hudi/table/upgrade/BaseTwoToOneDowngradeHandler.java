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

package org.apache.hudi.table.upgrade;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.MarkerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.DirectWriteMarkers;

import com.esotericsoftware.minlog.Log;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.MarkerUtils.MARKERS_FILENAME_PREFIX;

public abstract class BaseTwoToOneDowngradeHandler implements DowngradeHandler {

  @Override
  public Map<ConfigProperty, String> downgrade(HoodieWriteConfig config, HoodieEngineContext context, String instantTime) {
    HoodieTable table = getTable(config, context);
    HoodieTableMetaClient metaClient = table.getMetaClient();

    // re-create marker files if any partial timeline server based markers are found
    HoodieTimeline inflightTimeline = metaClient.getCommitsTimeline().filterPendingExcludingCompaction();
    List<HoodieInstant> commits = inflightTimeline.getReverseOrderedInstants().collect(Collectors.toList());
    for (HoodieInstant commitInstant : commits) {
      // Converts the markers in new format to old format of direct markers
      try {
        convertToDirectMarkers(
            commitInstant.getTimestamp(), table, context, config.getMarkersDeleteParallelism());
      } catch (IOException e) {
        throw new HoodieException("Converting marker files to DIRECT style failed during downgrade", e);
      }
    }
    return Collections.EMPTY_MAP;
  }

  abstract HoodieTable getTable(HoodieWriteConfig config, HoodieEngineContext context);

  /**
   * Converts the markers in new format(timeline server based) to old format of direct markers,
   * i.e., one marker file per data file, without MARKERS.type file.
   * This needs to be idempotent.
   * 1. read all markers from timeline server based marker files
   * 2. create direct style markers
   * 3. delete marker type file
   * 4. delete timeline server based marker files
   *
   * @param commitInstantTime instant of interest for marker conversion.
   * @param table             instance of {@link HoodieTable} to use
   * @param context           instance of {@link HoodieEngineContext} to use
   * @param parallelism       parallelism to use
   */
  private void convertToDirectMarkers(final String commitInstantTime,
                                      HoodieTable table,
                                      HoodieEngineContext context,
                                      int parallelism) throws IOException {
    String markerDir = table.getMetaClient().getMarkerFolderPath(commitInstantTime);
    FileSystem fileSystem = FSUtils.getFs(markerDir, context.getHadoopConf().newCopy());
    Option<MarkerType> markerTypeOption = MarkerUtils.readMarkerType(fileSystem, markerDir);
    if (markerTypeOption.isPresent()) {
      switch (markerTypeOption.get()) {
        case TIMELINE_SERVER_BASED:
          // Reads all markers written by the timeline server
          Map<String, Set<String>> markersMap =
              MarkerUtils.readTimelineServerBasedMarkersFromFileSystem(
                  markerDir, fileSystem, context, parallelism);
          DirectWriteMarkers directWriteMarkers = new DirectWriteMarkers(table, commitInstantTime);
          // Recreates the markers in the direct format
          for (Set<String> markers : markersMap.values()) {
            markers.forEach(directWriteMarkers::create);
          }
          // Delete marker type file
          MarkerUtils.deleteMarkerTypeFile(fileSystem, markerDir);
          // delete timeline server based markers
          deleteTimelineBasedMarkerFiles(markerDir, fileSystem);
          break;
        default:
          throw new HoodieException("The marker type \"" + markerTypeOption.get().name() + "\" is not supported.");
      }
    } else {
      // incase of partial failures during downgrade, there is a chance that marker type file was deleted, but timeline server based marker files are left.
      // so delete them if any
      deleteTimelineBasedMarkerFiles(markerDir, fileSystem);
    }
  }

  private void deleteTimelineBasedMarkerFiles(String markerDir, FileSystem fileSystem) throws IOException {
    // Delete timeline based marker files if any.
    Path dirPath = new Path(markerDir);
    FileStatus[] fileStatuses = fileSystem.listStatus(dirPath);
    Predicate<FileStatus> prefixFilter = pathStr -> pathStr.getPath().getName().contains(MARKERS_FILENAME_PREFIX);
    List<String> markerDirSubPaths = Arrays.stream(fileStatuses)
        .filter(prefixFilter)
        .map(fileStatus -> fileStatus.getPath().toString())
        .collect(Collectors.toList());
    markerDirSubPaths.forEach(fileToDelete -> {
      try {
        fileSystem.delete(new Path(fileToDelete), false);
      } catch (IOException e) {
        Log.warn("Deleting Timeline based marker files failed ", e);
      }
    });
  }
}
