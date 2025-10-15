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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.buffer.HoodieFileGroupRecordBuffer;
import org.apache.hudi.common.table.read.buffer.StreamingSortedFileGroupRecordBuffer;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.expression.Expression;
import org.apache.hudi.expression.Predicate;
import org.apache.hudi.expression.Predicates;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.fs.FSUtils.getRelativePartitionPath;

/**
 * A streaming log record reader that assumes records in log files are sorted by record key.
 *
 * <p>This reader provides significant memory savings compared to {@link HoodieMergedLogRecordReader}
 * by not storing all log records in memory. Instead, it performs streaming sorted merge on-the-fly.
 *
 * <p><b>Key Requirements:</b>
 * <ul>
 *   <li>Log records MUST be sorted by record key within each data block</li>
 *   <li>Base file records (if present) MUST be sorted by record key</li>
 * </ul>
 *
 * <p><b>Benefits:</b>
 * <ul>
 *   <li>Reduced memory usage - only keeps minimal state per log block</li>
 *   <li>Efficient sorted merge using priority queue</li>
 *   <li>Suitable for large log files where memory is constrained</li>
 * </ul>
 *
 * <p><b>Usage Example:</b>
 * <pre>{@code
 * StreamingSortedLogRecordReader<InternalRow> reader =
 *     StreamingSortedLogRecordReader.<InternalRow>newBuilder()
 *         .withHoodieReaderContext(readerContext)
 *         .withStorage(storage)
 *         .withLogFiles(logFiles)
 *         .withMetaClient(metaClient)
 *         .withRecordBuffer(buffer)
 *         .build();
 *
 * // Iterate through merged records
 * for (BufferedRecord<InternalRow> record : reader) {
 *     // Process record
 * }
 * }</pre>
 *
 * @param <T> type of engine-specific record representation
 */
public class StreamingSortedLogRecordReader<T> extends BaseHoodieLogRecordReader<T>
    implements Iterable<BufferedRecord<T>>, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingSortedLogRecordReader.class);

  // Timer for performance tracking
  public final HoodieTimer timer = HoodieTimer.create();

  // Count of merged records in log
  private long numMergedRecordsInLog;

  // Total time taken to read and merge blocks
  private long totalTimeTakenToReadAndMergeBlocks;

  private StreamingSortedLogRecordReader(HoodieReaderContext<T> readerContext,
                                         HoodieTableMetaClient metaClient,
                                         HoodieStorage storage,
                                         List<String> logFilePaths,
                                         boolean reverseReader,
                                         int bufferSize,
                                         Option<InstantRange> instantRange,
                                         boolean withOperationField,
                                         boolean forceFullScan,
                                         Option<String> partitionName,
                                         Option<String> keyFieldOverride,
                                         boolean enableOptimizedLogBlocksScan,
                                         HoodieFileGroupRecordBuffer<T> recordBuffer,
                                         boolean allowInflightInstants) {
    super(readerContext, metaClient, storage, logFilePaths, reverseReader, bufferSize, instantRange,
          withOperationField, forceFullScan, partitionName, keyFieldOverride, enableOptimizedLogBlocksScan,
          recordBuffer, allowInflightInstants);

    ValidationUtils.checkArgument(recordBuffer instanceof StreamingSortedFileGroupRecordBuffer,
        "StreamingSortedLogRecordReader requires StreamingSortedFileGroupRecordBuffer");

    if (forceFullScan) {
      performScan();
    }
  }

  /**
   * Scans delta-log files processing blocks in streaming fashion.
   */
  public final void scan() {
    scan(false);
  }

  /**
   * Scans delta-log files.
   *
   * @param skipProcessingBlocks if true, only scans block metadata without processing records
   */
  public final void scan(boolean skipProcessingBlocks) {
    if (forceFullScan) {
      // NOTE: When full-scan is enforced, scanning is invoked upfront (during initialization)
      return;
    }

    scanInternal(Option.empty(), skipProcessingBlocks);
  }

  /**
   * Performs the actual scan of log files.
   */
  private void performScan() {
    // Do the scan and merge
    timer.startTimer();

    Option<KeySpec> keySpecOpt = createKeySpec(readerContext.getKeyFilterOpt());
    scanInternal(keySpecOpt, false);

    this.totalTimeTakenToReadAndMergeBlocks = timer.endTimer();
    this.numMergedRecordsInLog = recordBuffer.size();

    LOG.info("Streaming sorted scan completed - Log files: {}, Merged records: {}, Time taken: {}ms",
             logFilePaths.size(), numMergedRecordsInLog, totalTimeTakenToReadAndMergeBlocks);
  }

  /**
   * Creates a key specification for filtering records based on predicates.
   *
   * @param filter optional predicate for filtering
   * @return key specification if applicable
   */
  static Option<KeySpec> createKeySpec(Option<Predicate> filter) {
    if (filter.isEmpty()) {
      return Option.empty();
    }
    if (filter.get().getOperator() == Expression.Operator.IN) {
      List<Expression> rightChildren = ((Predicates.In) filter.get()).getRightChildren();
      List<String> keyOrPrefixes = rightChildren.stream()
          .map(e -> (String) e.eval(null)).collect(Collectors.toList());
      return Option.of(new FullKeySpec(keyOrPrefixes));
    } else if (filter.get().getOperator() == Expression.Operator.STARTS_WITH) {
      List<Expression> rightChildren = ((Predicates.StringStartsWithAny) filter.get()).getRightChildren();
      List<String> keyOrPrefixes = rightChildren.stream()
          .map(e -> (String) e.eval(null)).collect(Collectors.toList());
      return Option.of(new PrefixKeySpec(keyOrPrefixes));
    } else {
      return Option.empty();
    }
  }

  @Override
  public Iterator<BufferedRecord<T>> iterator() {
    return recordBuffer.getLogRecordIterator();
  }

  /**
   * Returns the map of records. Note: In streaming mode, this may not contain all records
   * if they have already been consumed by the iterator.
   *
   * @return map of record key to buffered record
   */
  public Map<Serializable, BufferedRecord<T>> getRecords() {
    return recordBuffer.getLogRecords();
  }

  /**
   * @return the number of merged records in log
   */
  public long getNumMergedRecordsInLog() {
    return numMergedRecordsInLog;
  }

  /**
   * @return total time taken to read and merge blocks in milliseconds
   */
  public long getTotalTimeTakenToReadAndMergeBlocks() {
    return totalTimeTakenToReadAndMergeBlocks;
  }

  @Override
  public void close() {
    // Close the record buffer which will close all iterators
    if (recordBuffer != null) {
      recordBuffer.close();
    }
  }

  /**
   * Returns a new builder for {@code StreamingSortedLogRecordReader}.
   *
   * @param <T> type of engine-specific record representation
   * @return builder instance
   */
  public static <T> Builder<T> newBuilder() {
    return new Builder<>();
  }

  /**
   * Builder for {@code StreamingSortedLogRecordReader}.
   *
   * @param <T> type of engine-specific record representation
   */
  public static class Builder<T> extends BaseHoodieLogRecordReader.Builder<T> {
    private HoodieReaderContext<T> readerContext;
    private HoodieStorage storage;
    private List<String> logFilePaths;
    private boolean reverseReader;
    private int bufferSize;
    private Option<InstantRange> instantRange = Option.empty();
    private String partitionName;
    private boolean withOperationField = false;
    private String keyFieldOverride;
    private boolean forceFullScan = true;
    private boolean enableOptimizedLogBlocksScan = false;
    private HoodieFileGroupRecordBuffer<T> recordBuffer;
    private boolean allowInflightInstants = false;
    private HoodieTableMetaClient metaClient;

    @Override
    public Builder<T> withHoodieReaderContext(HoodieReaderContext<T> readerContext) {
      this.readerContext = readerContext;
      return this;
    }

    @Override
    public Builder<T> withStorage(HoodieStorage storage) {
      this.storage = storage;
      return this;
    }

    @Override
    public Builder<T> withLogFiles(List<HoodieLogFile> hoodieLogFiles) {
      this.logFilePaths = hoodieLogFiles.stream()
          .filter(l -> !l.isCDC())
          .map(l -> l.getPath().toString())
          .collect(Collectors.toList());
      return this;
    }

    @Override
    public Builder<T> withReverseReader(boolean reverseReader) {
      this.reverseReader = reverseReader;
      return this;
    }

    @Override
    public Builder<T> withBufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }

    @Override
    public Builder<T> withInstantRange(Option<InstantRange> instantRange) {
      this.instantRange = instantRange;
      return this;
    }

    @Override
    public Builder<T> withOperationField(boolean withOperationField) {
      this.withOperationField = withOperationField;
      return this;
    }

    @Override
    public Builder<T> withPartition(String partitionName) {
      this.partitionName = partitionName;
      return this;
    }

    @Override
    public Builder<T> withOptimizedLogBlocksScan(boolean enableOptimizedLogBlocksScan) {
      this.enableOptimizedLogBlocksScan = enableOptimizedLogBlocksScan;
      return this;
    }

    /**
     * Sets the key field override for custom record key extraction.
     *
     * @param keyFieldOverride custom key field name
     * @return builder instance
     */
    public Builder<T> withKeyFieldOverride(String keyFieldOverride) {
      this.keyFieldOverride = keyFieldOverride;
      return this;
    }

    /**
     * Sets whether to force full scan upfront.
     *
     * @param forceFullScan if true, scans all log blocks during initialization
     * @return builder instance
     */
    public Builder<T> withForceFullScan(boolean forceFullScan) {
      this.forceFullScan = forceFullScan;
      return this;
    }

    /**
     * Sets the record buffer to use. Must be a {@link StreamingSortedFileGroupRecordBuffer}.
     *
     * @param recordBuffer the streaming sorted record buffer
     * @return builder instance
     */
    public Builder<T> withRecordBuffer(HoodieFileGroupRecordBuffer<T> recordBuffer) {
      this.recordBuffer = recordBuffer;
      return this;
    }

    /**
     * Sets whether to allow inflight instants.
     *
     * @param allowInflightInstants if true, includes records from inflight instants
     * @return builder instance
     */
    public Builder<T> withAllowInflightInstants(boolean allowInflightInstants) {
      this.allowInflightInstants = allowInflightInstants;
      return this;
    }

    /**
     * Sets the table meta client.
     *
     * @param metaClient the table meta client
     * @return builder instance
     */
    public Builder<T> withMetaClient(HoodieTableMetaClient metaClient) {
      this.metaClient = metaClient;
      return this;
    }

    @Override
    public StreamingSortedLogRecordReader<T> build() {
      ValidationUtils.checkArgument(recordBuffer != null,
          "Record Buffer is null in StreamingSortedLogRecordReader");
      ValidationUtils.checkArgument(recordBuffer instanceof StreamingSortedFileGroupRecordBuffer,
          "Record Buffer must be StreamingSortedFileGroupRecordBuffer");
      ValidationUtils.checkArgument(readerContext != null,
          "Reader Context is null in StreamingSortedLogRecordReader");
      ValidationUtils.checkArgument(metaClient != null,
          "Meta Client is null in StreamingSortedLogRecordReader");

      if (this.partitionName == null && CollectionUtils.nonEmpty(this.logFilePaths)) {
        this.partitionName = getRelativePartitionPath(
            new StoragePath(readerContext.getTablePath()),
            new StoragePath(this.logFilePaths.get(0)).getParent());
      }

      return new StreamingSortedLogRecordReader<>(
          readerContext, metaClient, storage, logFilePaths,
          reverseReader, bufferSize, instantRange,
          withOperationField, forceFullScan,
          Option.ofNullable(partitionName),
          Option.ofNullable(keyFieldOverride),
          enableOptimizedLogBlocksScan, recordBuffer,
          allowInflightInstants);
    }
  }
}
