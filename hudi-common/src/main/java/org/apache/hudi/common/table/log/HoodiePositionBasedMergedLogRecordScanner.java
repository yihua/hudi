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

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodiePreCombineAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.InternalSchema;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.fs.FSUtils.getRelativePartitionPath;

public class HoodiePositionBasedMergedLogRecordScanner extends AbstractHoodieLogRecordReader
    implements Iterable<HoodieRecord>, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(HoodiePositionBasedMergedLogRecordScanner.class);
  // A timer for calculating elapsed time in millis
  public final HoodieTimer timer = HoodieTimer.create();
  // Map of compacted/merged records
  private final ExternalSpillableMap<Integer, HoodieRecord> records;
  private final Set<Integer> deletePositions = new HashSet<>();
  // Set of already scanned prefixes allowing us to avoid scanning same prefixes again
  private final Set<String> scannedPrefixes;
  // count of merged records in log
  private long numMergedRecordsInLog;
  private final long maxMemorySizeInBytes;
  // Stores the total time taken to perform reading and merging of log blocks
  private long totalTimeTakenToReadAndMergeBlocks;

  protected HoodiePositionBasedMergedLogRecordScanner(FileSystem fs, String basePath, List<String> logFilePaths, Schema readerSchema,
                                                      String latestInstantTime, Long maxMemorySizeInBytes, boolean readBlocksLazily,
                                                      boolean reverseReader, int bufferSize, String spillableMapBasePath,
                                                      Option<InstantRange> instantRange,
                                                      ExternalSpillableMap.DiskMapType diskMapType,
                                                      boolean isBitCaskDiskMapCompressionEnabled,
                                                      boolean withOperationField, boolean forceFullScan,
                                                      Option<String> partitionName,
                                                      InternalSchema internalSchema,
                                                      Option<String> keyFieldOverride,
                                                      boolean enableOptimizedLogBlocksScan,
                                                      boolean useRecordPositions,
                                                      HoodieRecordMerger recordMerger) {
    super(fs, basePath, logFilePaths, readerSchema, latestInstantTime, readBlocksLazily, reverseReader, bufferSize,
        instantRange, withOperationField, forceFullScan, partitionName, internalSchema, keyFieldOverride, enableOptimizedLogBlocksScan, useRecordPositions, recordMerger);
    try {
      this.maxMemorySizeInBytes = maxMemorySizeInBytes;
      // Store merged records for all versions for this log file, set the in-memory footprint to maxInMemoryMapSize
      this.records = new ExternalSpillableMap<>(maxMemorySizeInBytes, spillableMapBasePath, new DefaultSizeEstimator(),
          new HoodieRecordSizeEstimator(readerSchema), diskMapType, isBitCaskDiskMapCompressionEnabled);
      this.scannedPrefixes = new HashSet<>();
    } catch (IOException e) {
      throw new HoodieIOException("IOException when creating ExternalSpillableMap at " + spillableMapBasePath, e);
    }

    if (forceFullScan) {
      performScan();
    }
  }

  /**
   * Scans delta-log files processing blocks
   */
  public final void scan() {
    scan(false);
  }

  public final void scan(boolean skipProcessingBlocks) {
    if (forceFullScan) {
      // NOTE: When full-scan is enforced, scanning is invoked upfront (during initialization)
      return;
    }

    scanInternal(Option.empty(), skipProcessingBlocks);
  }

  private void performScan() {
    // Do the scan and merge
    timer.startTimer();

    scanInternal(Option.empty(), false);

    this.totalTimeTakenToReadAndMergeBlocks = timer.endTimer();
    this.numMergedRecordsInLog = records.size();

    LOG.info("Number of log files scanned => " + logFilePaths.size());
    LOG.info("MaxMemoryInBytes allowed for compaction => " + maxMemorySizeInBytes);
    LOG.info("Number of entries in MemoryBasedMap in ExternalSpillableMap => " + records.getInMemoryMapNumEntries());
    LOG.info("Total size in bytes of MemoryBasedMap in ExternalSpillableMap => " + records.getCurrentInMemoryMapSize());
    LOG.info("Number of entries in BitCaskDiskMap in ExternalSpillableMap => " + records.getDiskBasedMapNumEntries());
    LOG.info("Size of file spilled to disk => " + records.getSizeOfFileOnDiskInBytes());
  }

  @Override
  public Iterator<HoodieRecord> iterator() {
    return records.iterator();
  }

  public Map<Integer, HoodieRecord> getRecords() {
    return records;
  }

  public HoodieRecord.HoodieRecordType getRecordType() {
    return recordMerger.getRecordType();
  }

  public long getNumMergedRecordsInLog() {
    return numMergedRecordsInLog;
  }

  public static HoodieMergedLogRecordScanner.Builder newBuilder() {
    return new HoodieMergedLogRecordScanner.Builder();
  }

  @Override
  public <T> void processNextRecord(HoodieRecord<T> newRecord, Option<Integer> position) throws IOException {
    // Assume position is present
    int pos = position.get();
    if (pos >= 0) {
      records.put(pos, newRecord.copy());
    }
  }

  @Override
  protected void processNextDeletedRecord(DeleteRecord deleteRecord) {
    /* skip now
    String key = deleteRecord.getRecordKey();
    HoodieRecord oldRecord = records.get(key);
    if (oldRecord != null) {
      // Merge and store the merged record. The ordering val is taken to decide whether the same key record
      // should be deleted or be kept. The old record is kept only if the DELETE record has smaller ordering val.
      // For same ordering values, uses the natural order(arrival time semantics).

      Comparable curOrderingVal = oldRecord.getOrderingValue(this.readerSchema, this.hoodieTableMetaClient.getTableConfig().getProps());
      Comparable deleteOrderingVal = deleteRecord.getOrderingValue();
      // Checks the ordering value does not equal to 0
      // because we use 0 as the default value which means natural order
      boolean choosePrev = !deleteOrderingVal.equals(0)
          && ReflectionUtils.isSameClass(curOrderingVal, deleteOrderingVal)
          && curOrderingVal.compareTo(deleteOrderingVal) > 0;
      if (choosePrev) {
        // The DELETE message is obsolete if the old message has greater orderingVal.
        return;
      }
    }
    // Put the DELETE record
    if (recordType == HoodieRecord.HoodieRecordType.AVRO) {
      records.put(key, SpillableMapUtils.generateEmptyPayload(key,
          deleteRecord.getPartitionPath(), deleteRecord.getOrderingValue(), getPayloadClassFQN()));
    } else {
      HoodieEmptyRecord record = new HoodieEmptyRecord<>(new HoodieKey(key, deleteRecord.getPartitionPath()), null, deleteRecord.getOrderingValue(), recordType);
      records.put(key, record);
    }*/
  }

  @Override
  protected void processNextDeletePosition(int position) {
    deletePositions.add(position);
  }

  public long getTotalTimeTakenToReadAndMergeBlocks() {
    return totalTimeTakenToReadAndMergeBlocks;
  }

  @Override
  public void close() {
    if (records != null) {
      records.close();
    }
  }

  /**
   * Builder used to build {@code HoodieUnMergedLogRecordScanner}.
   */
  public static class Builder extends AbstractHoodieLogRecordReader.Builder {
    private FileSystem fs;
    private String basePath;
    private List<String> logFilePaths;
    private Schema readerSchema;
    private InternalSchema internalSchema = InternalSchema.getEmptyInternalSchema();
    private String latestInstantTime;
    private boolean readBlocksLazily;
    private boolean reverseReader;
    private int bufferSize;
    // specific configurations
    private Long maxMemorySizeInBytes;
    private String spillableMapBasePath;
    private ExternalSpillableMap.DiskMapType diskMapType = HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.defaultValue();
    private boolean isBitCaskDiskMapCompressionEnabled = HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue();
    // incremental filtering
    private Option<InstantRange> instantRange = Option.empty();
    private String partitionName;
    // operation field default false
    private boolean withOperationField = false;
    private String keyFieldOverride;
    // By default, we're doing a full-scan
    private boolean forceFullScan = true;
    private boolean enableOptimizedLogBlocksScan = false;
    private boolean useRecordPositions = false;
    private HoodieRecordMerger recordMerger = HoodiePreCombineAvroRecordMerger.INSTANCE;

    @Override
    public Builder withFileSystem(FileSystem fs) {
      this.fs = fs;
      return this;
    }

    @Override
    public Builder withBasePath(String basePath) {
      this.basePath = basePath;
      return this;
    }

    @Override
    public Builder withLogFilePaths(List<String> logFilePaths) {
      this.logFilePaths = logFilePaths.stream()
          .filter(p -> !p.endsWith(HoodieCDCUtils.CDC_LOGFILE_SUFFIX))
          .collect(Collectors.toList());
      return this;
    }

    @Override
    public Builder withReaderSchema(Schema schema) {
      this.readerSchema = schema;
      return this;
    }

    @Override
    public Builder withLatestInstantTime(String latestInstantTime) {
      this.latestInstantTime = latestInstantTime;
      return this;
    }

    @Override
    public Builder withReadBlocksLazily(boolean readBlocksLazily) {
      this.readBlocksLazily = readBlocksLazily;
      return this;
    }

    @Override
    public Builder withReverseReader(boolean reverseReader) {
      this.reverseReader = reverseReader;
      return this;
    }

    @Override
    public Builder withBufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }

    @Override
    public Builder withInstantRange(Option<InstantRange> instantRange) {
      this.instantRange = instantRange;
      return this;
    }

    public Builder withMaxMemorySizeInBytes(Long maxMemorySizeInBytes) {
      this.maxMemorySizeInBytes = maxMemorySizeInBytes;
      return this;
    }

    public Builder withSpillableMapBasePath(String spillableMapBasePath) {
      this.spillableMapBasePath = spillableMapBasePath;
      return this;
    }

    public Builder withDiskMapType(ExternalSpillableMap.DiskMapType diskMapType) {
      this.diskMapType = diskMapType;
      return this;
    }

    public Builder withBitCaskDiskMapCompressionEnabled(boolean isBitCaskDiskMapCompressionEnabled) {
      this.isBitCaskDiskMapCompressionEnabled = isBitCaskDiskMapCompressionEnabled;
      return this;
    }

    @Override
    public Builder withInternalSchema(InternalSchema internalSchema) {
      this.internalSchema = internalSchema;
      return this;
    }

    public Builder withOperationField(boolean withOperationField) {
      this.withOperationField = withOperationField;
      return this;
    }

    @Override
    public Builder withPartition(String partitionName) {
      this.partitionName = partitionName;
      return this;
    }

    @Override
    public Builder withOptimizedLogBlocksScan(boolean enableOptimizedLogBlocksScan) {
      this.enableOptimizedLogBlocksScan = enableOptimizedLogBlocksScan;
      return this;
    }

    public Builder useRecordPositions() {
      this.useRecordPositions = true;
      return this;
    }

    @Override
    public Builder withRecordMerger(HoodieRecordMerger recordMerger) {
      this.recordMerger = HoodieRecordUtils.mergerToPreCombineMode(recordMerger);
      return this;
    }

    public Builder withKeyFiledOverride(String keyFieldOverride) {
      this.keyFieldOverride = Objects.requireNonNull(keyFieldOverride);
      return this;
    }

    public Builder withForceFullScan(boolean forceFullScan) {
      this.forceFullScan = forceFullScan;
      return this;
    }

    @Override
    public HoodiePositionBasedMergedLogRecordScanner build() {
      if (this.partitionName == null && CollectionUtils.nonEmpty(this.logFilePaths)) {
        this.partitionName = getRelativePartitionPath(new Path(basePath), new Path(this.logFilePaths.get(0)).getParent());
      }
      ValidationUtils.checkArgument(recordMerger != null);

      return new HoodiePositionBasedMergedLogRecordScanner(fs, basePath, logFilePaths, readerSchema,
          latestInstantTime, maxMemorySizeInBytes, readBlocksLazily, reverseReader,
          bufferSize, spillableMapBasePath, instantRange,
          diskMapType, isBitCaskDiskMapCompressionEnabled, withOperationField, forceFullScan,
          Option.ofNullable(partitionName), internalSchema, Option.ofNullable(keyFieldOverride),
          enableOptimizedLogBlocksScan, useRecordPositions, recordMerger);
    }
  }
}
