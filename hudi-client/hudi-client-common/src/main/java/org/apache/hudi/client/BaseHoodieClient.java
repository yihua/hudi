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

package org.apache.hudi.client;

import org.apache.hudi.callback.HoodieClientInitCallback;
import org.apache.hudi.client.embedded.EmbeddedTimelineServerHelper;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.client.heartbeat.HoodieHeartbeatClient;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.client.utils.TransactionUtils;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.table.HoodieTable;

import com.codahale.metrics.Timer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Abstract class taking care of holding common member variables (FileSystem, SparkContext, HoodieConfigs) Also, manages
 * embedded timeline-server if enabled.
 */
public abstract class BaseHoodieClient implements Serializable, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(BaseHoodieClient.class);

  protected final transient FileSystem fs;
  protected final transient HoodieEngineContext context;
  protected final transient Configuration hadoopConf;
  protected final transient HoodieMetrics metrics;
  protected final HoodieWriteConfig config;
  protected final String basePath;
  protected final HoodieHeartbeatClient heartbeatClient;
  protected final TransactionManager txnManager;

  /**
   * Timeline Server has the same lifetime as that of Client. Any operations done on the same timeline service will be
   * able to take advantage of the cached file-system view. New completed actions will be synced automatically in an
   * incremental fashion.
   */
  private transient Option<EmbeddedTimelineService> timelineServer;
  private final boolean shouldStopTimelineServer;

  protected BaseHoodieClient(HoodieEngineContext context, HoodieWriteConfig clientConfig) {
    this(context, clientConfig, Option.empty());
  }

  protected BaseHoodieClient(HoodieEngineContext context, HoodieWriteConfig clientConfig,
      Option<EmbeddedTimelineService> timelineServer) {
    this.hadoopConf = context.getHadoopConf().get();
    this.fs = FSUtils.getFs(clientConfig.getBasePath(), hadoopConf);
    this.context = context;
    this.basePath = clientConfig.getBasePath();
    this.config = clientConfig;
    this.timelineServer = timelineServer;
    shouldStopTimelineServer = !timelineServer.isPresent();
    this.heartbeatClient = new HoodieHeartbeatClient(this.fs, this.basePath,
        clientConfig.getHoodieClientHeartbeatIntervalInMs(), clientConfig.getHoodieClientHeartbeatTolerableMisses());
    this.metrics = new HoodieMetrics(config);
    this.txnManager = new TransactionManager(config, fs);
    startEmbeddedServerView();
    initWrapperFSMetrics();
    runClientInitCallbacks();
  }

  /**
   * Releases any resources used by the client.
   */
  @Override
  public void close() {
    stopEmbeddedServerView(true);
    this.context.setJobStatus("", "");
    this.heartbeatClient.stop();
    this.txnManager.close();
  }

  private synchronized void stopEmbeddedServerView(boolean resetViewStorageConfig) {
    if (timelineServer.isPresent() && shouldStopTimelineServer) {
      // Stop only if owner
      LOG.info("Stopping Timeline service !!");
      timelineServer.get().stop();
    }

    timelineServer = Option.empty();
    // Reset Storage Config to Client specified config
    if (resetViewStorageConfig) {
      config.resetViewStorageConfig();
    }
  }

  private synchronized void startEmbeddedServerView() {
    if (config.isEmbeddedTimelineServerEnabled()) {
      if (!timelineServer.isPresent()) {
        // Run Embedded Timeline Server
        try {
          timelineServer = EmbeddedTimelineServerHelper.createEmbeddedTimelineService(context, config);
        } catch (IOException e) {
          LOG.warn("Unable to start timeline service. Proceeding as if embedded server is disabled", e);
          stopEmbeddedServerView(false);
        }
      } else {
        LOG.info("Timeline Server already running. Not restarting the service");
      }
    } else {
      LOG.info("Embedded Timeline Server is disabled. Not starting timeline service");
    }
  }

  private void runClientInitCallbacks() {
    String callbackClassNames = config.getClientInitCallbackClassNames();
    if (StringUtils.isNullOrEmpty(callbackClassNames)) {
      return;
    }
    Arrays.stream(callbackClassNames.split(",")).forEach(callbackClass -> {
      Object callback = ReflectionUtils.loadClass(callbackClass);
      if (!(callback instanceof HoodieClientInitCallback)) {
        throw new HoodieException(callbackClass + " is not a subclass of "
            + HoodieClientInitCallback.class.getSimpleName());
      }
      ((HoodieClientInitCallback) callback).call(this);
    });
  }

  public HoodieWriteConfig getConfig() {
    return config;
  }

  public HoodieEngineContext getEngineContext() {
    return context;
  }

  protected void initWrapperFSMetrics() {
    // no-op.
  }

  protected HoodieTableMetaClient createMetaClient(boolean loadActiveTimelineOnLoad) {
    return HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(config.getBasePath())
        .setLoadActiveTimelineOnLoad(loadActiveTimelineOnLoad).setConsistencyGuardConfig(config.getConsistencyGuardConfig())
        .setLayoutVersion(Option.of(new TimelineLayoutVersion(config.getTimelineLayoutVersion())))
        .setFileSystemRetryConfig(config.getFileSystemRetryConfig())
        .setMetaserverConfig(config.getProps()).build();
  }

  public Option<EmbeddedTimelineService> getTimelineServer() {
    return timelineServer;
  }

  public HoodieHeartbeatClient getHeartbeatClient() {
    return heartbeatClient;
  }

  /**
   * Resolve write conflicts before commit.
   *
   * @param table A hoodie table instance created after transaction starts so that the latest commits and files are captured.
   * @param metadata Current committing instant's metadata
   * @param pendingInflightAndRequestedInstants Pending instants on the timeline
   *
   * @see {@link BaseHoodieWriteClient#preCommit}
   * @see {@link BaseHoodieTableServiceClient#preCommit}
   */
  protected void resolveWriteConflict(HoodieTable table, HoodieCommitMetadata metadata, Set<String> pendingInflightAndRequestedInstants) {
    Timer.Context conflictResolutionTimer = metrics.getConflictResolutionCtx();
    try {
      TransactionUtils.resolveWriteConflictIfAny(table, this.txnManager.getCurrentTransactionOwner(),
          Option.of(metadata), config, txnManager.getLastCompletedTransactionOwner(), false, pendingInflightAndRequestedInstants);
      metrics.emitConflictResolutionSuccessful();
    } catch (HoodieWriteConflictException e) {
      metrics.emitConflictResolutionFailed();
      throw e;
    } finally {
      if (conflictResolutionTimer != null) {
        conflictResolutionTimer.stop();
      }
    }
  }

  /**
   * Finalize Write operation.
   *
   * @param table HoodieTable
   * @param instantTime Instant Time
   * @param stats Hoodie Write Stat
   */
  protected void finalizeWrite(HoodieTable table, String instantTime, List<HoodieWriteStat> stats) {
    try {
      final Timer.Context finalizeCtx = metrics.getFinalizeCtx();
      table.finalizeWrite(context, instantTime, stats);
      if (finalizeCtx != null) {
        Option<Long> durationInMs = Option.of(metrics.getDurationInMs(finalizeCtx.stop()));
        durationInMs.ifPresent(duration -> {
          LOG.info("Finalize write elapsed time (milliseconds): " + duration);
          metrics.updateFinalizeWriteMetrics(duration, stats.size());
        });
      }
    } catch (HoodieIOException ioe) {
      throw new HoodieCommitException("Failed to complete commit " + instantTime + " due to finalize errors.", ioe);
    }
  }
}
