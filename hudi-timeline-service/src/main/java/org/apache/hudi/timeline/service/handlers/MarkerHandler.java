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

package org.apache.hudi.timeline.service.handlers;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.timeline.service.TimelineService;
import org.apache.hudi.timeline.service.handlers.marker.MarkerCreationBatchingRunnable;
import org.apache.hudi.timeline.service.handlers.marker.MarkerCreationCompletableFuture;
import org.apache.hudi.timeline.service.handlers.marker.MarkerDirState;

import io.javalin.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * REST Handler servicing marker requests.
 *
 * The marker creation requests are handled asynchronous, while other types of requests
 * are handled synchronous.
 *
 * Marker creation requests are batch processed periodically by a thread.  Each batch
 * processing thread adds new markers to a marker file.  Given that marker file operation
 * can take time, multiple concurrent threads can run at the same, while they operate
 * on different marker files storing mutually exclusive marker entries.  At any given
 * time, a marker file is touched by at most one thread to guarantee consistency.
 * Below is an example of running batch processing threads.
 *
 *           |-----| batch interval
 * Thread 1  |-------------------------->| writing to MARKERS1
 * Thread 2        |-------------------------->| writing to MARKERS2
 * Thread 3               |-------------------------->| writing to MARKERS3
 */
public class MarkerHandler extends Handler {
  private static final Logger LOG = LogManager.getLogger(MarkerHandler.class);
  // Margin time for scheduling the processing of the next batch of marker creation requests
  private static final long SCHEDULING_MARGIN_TIME_MS = 5L;

  private final Registry metricsRegistry;
  private final ScheduledExecutorService executorService;

  // Batch process interval in milliseconds
  private final long batchIntervalMs;
  // Parallelism for reading and deleting marker files
  private final int parallelism;
  // Marker directory states, {markerDirPath -> MarkerDirState instance}
  private final Map<String, MarkerDirState> markerDirStateMap = new HashMap<>();
  private transient HoodieEngineContext hoodieEngineContext;
  // Lock for synchronous processing of marker creating requests
  private final Object createMarkerRequestLockObject = new Object();
  // Next batch process timestamp in milliseconds
  private long nextBatchProcessTimeMs = 0L;

  public MarkerHandler(Configuration conf, TimelineService.Config timelineServiceConfig,
                       HoodieEngineContext hoodieEngineContext, FileSystem fileSystem,
                       FileSystemViewManager viewManager, Registry metricsRegistry) throws IOException {
    super(conf, timelineServiceConfig, fileSystem, viewManager);
    LOG.debug("MarkerHandler FileSystem: " + this.fileSystem.getScheme());
    LOG.debug("MarkerHandler batching params: batchNumThreads=" + timelineServiceConfig.markerBatchNumThreads
        + " batchIntervalMs=" + timelineServiceConfig.markerBatchIntervalMs + "ms");
    this.hoodieEngineContext = hoodieEngineContext;
    this.metricsRegistry = metricsRegistry;
    this.batchIntervalMs = timelineServiceConfig.markerBatchIntervalMs;
    this.parallelism = timelineServiceConfig.markerParallelism;
    this.executorService = Executors.newScheduledThreadPool(timelineServiceConfig.markerBatchNumThreads);
  }

  /**
   * @param markerDir marker directory path
   * @return all marker paths in the marker directory
   */
  public Set<String> getAllMarkers(String markerDir) {
    MarkerDirState markerDirState = markerDirStateMap.computeIfAbsent(markerDir,
        k -> new MarkerDirState(markerDir, timelineServiceConfig.markerBatchNumThreads, fileSystem,
            hoodieEngineContext, parallelism));
    return markerDirState.getAllMarkers();
  }

  /**
   * @param markerDir marker directory path
   * @return all marker paths of write IO type "CREATE" and "MERGE"
   */
  public Set<String> getCreateAndMergeMarkers(String markerDir) {
    return getAllMarkers(markerDir).stream()
        .filter(markerName -> !markerName.endsWith(IOType.APPEND.name()))
        .collect(Collectors.toSet());
  }

  /**
   * @param markerDir  marker directory path
   * @return {@code true} if the marker directory exists; {@code false} otherwise.
   */
  public boolean doesMarkerDirExist(String markerDir) {
    MarkerDirState markerDirState = markerDirStateMap.computeIfAbsent(markerDir,
        k -> new MarkerDirState(markerDir, timelineServiceConfig.markerBatchNumThreads, fileSystem,
            hoodieEngineContext, parallelism));
    return markerDirState.exists();
  }

  /**
   * Generates a future for an async marker creation request
   *
   * The future is added to the marker creation future list and waits for the next batch processing
   * of marker creation requests.
   *
   * @param context Javalin app context
   * @param markerDir marker directory path
   * @param markerName marker name
   * @return the {@code CompletableFuture} instance for the request
   */
  public CompletableFuture<String> createMarker(Context context, String markerDir, String markerName) {
    LOG.info("Request: create marker " + markerDir + " " + markerName);
    MarkerCreationCompletableFuture future = new MarkerCreationCompletableFuture(context, markerDir, markerName);
    // Add the future to the list
    MarkerDirState markerDirState = markerDirStateMap.computeIfAbsent(
        markerDir, k -> new MarkerDirState(markerDir, timelineServiceConfig.markerBatchNumThreads,
            fileSystem, hoodieEngineContext, parallelism));
    markerDirState.addMarkerCreationFuture(future);
    synchronized (createMarkerRequestLockObject) {
      // Update the next batch processing time and schedule the batch processing if necessary
      long currTimeMs = System.currentTimeMillis();
      // If the current request may miss the next batch processing, schedule a new batch processing thread
      // A margin time is always considered for checking the tiemstamp to make sure no request is missed
      if (currTimeMs >= nextBatchProcessTimeMs - SCHEDULING_MARGIN_TIME_MS) {
        if (currTimeMs < nextBatchProcessTimeMs + batchIntervalMs - SCHEDULING_MARGIN_TIME_MS) {
          // within the batch interval from the latest batch processing thread
          // increment nextBatchProcessTimeMs by batchIntervalMs
          nextBatchProcessTimeMs += batchIntervalMs;
        } else {
          // Otherwise, wait for batchIntervalMs based on the current timestamp
          nextBatchProcessTimeMs = currTimeMs + batchIntervalMs;
        }

        long waitMs = nextBatchProcessTimeMs - currTimeMs;
        executorService.schedule(new MarkerCreationBatchingRunnable(markerDirStateMap, metricsRegistry),
            Math.max(0L, waitMs), TimeUnit.MILLISECONDS);
        LOG.debug("Wait for " + waitMs + " ms, next batch time: " + nextBatchProcessTimeMs);
      }
    }
    return future;
  }

  /**
   * Deletes markers in the directory.
   *
   * @param markerDir marker directory path
   * @return {@code true} if successful; {@code false} otherwise.
   */
  public Boolean deleteMarkers(String markerDir) {
    boolean result = getMarkerDirState(markerDir).deleteAllMarkers();
    markerDirStateMap.remove(markerDir);
    return result;
  }

  private MarkerDirState getMarkerDirState(String markerDir) {
    return markerDirStateMap.computeIfAbsent(markerDir,
        k -> new MarkerDirState(markerDir, timelineServiceConfig.markerBatchNumThreads, fileSystem,
            hoodieEngineContext, parallelism));
  }
}
