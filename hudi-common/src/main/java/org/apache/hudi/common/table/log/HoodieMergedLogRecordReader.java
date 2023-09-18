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
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodiePreCombineAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.internal.schema.InternalSchema;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.fs.FSUtils.getRelativePartitionPath;

public class HoodieMergedLogRecordReader<T> extends BaseHoodieLogRecordReader<T>
    implements Iterable<Pair<Option<T>, Map<String, Object>>>, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieMergedLogRecordReader.class);
  // A timer for calculating elapsed time in millis
  public final HoodieTimer timer = HoodieTimer.create();
  // Map of compacted/merged records
  // TODO(yihua): re-evaluate if we really want to use spillable map here since we see performance
  //              regression if the memory is not properly configured for the log reader and merging
  private final Map<String, Pair<Option<T>, Map<String, Object>>> records;
  // Set of already scanned prefixes allowing us to avoid scanning same prefixes again
  private final Set<String> scannedPrefixes;
  // count of merged records in log
  private long numMergedRecordsInLog;
  // Stores the total time taken to perform reading and merging of log blocks
  private long totalTimeTakenToReadAndMergeBlocks;

  @SuppressWarnings("unchecked")
  private HoodieMergedLogRecordReader(HoodieReaderContext<T> readerContext,
                                      FileSystem fs, String basePath, List<String> logFilePaths, Schema readerSchema,
                                      String latestInstantTime, boolean readBlocksLazily,
                                      boolean reverseReader, int bufferSize, Option<InstantRange> instantRange,
                                      boolean withOperationField, boolean forceFullScan,
                                      Option<String> partitionName,
                                      InternalSchema internalSchema,
                                      Option<String> keyFieldOverride,
                                      boolean enableOptimizedLogBlocksScan, HoodieRecordMerger recordMerger) {
    super(readerContext, fs, basePath, logFilePaths, readerSchema, latestInstantTime, readBlocksLazily, reverseReader, bufferSize,
        instantRange, withOperationField, forceFullScan, partitionName, internalSchema, keyFieldOverride, enableOptimizedLogBlocksScan, recordMerger);
    this.records = new HashMap<>();
    // Store merged records for all versions for this log file, set the in-memory footprint to maxInMemoryMapSize
    //this.records = new ExternalSpillableMap<>(maxMemorySizeInBytes, spillableMapBasePath, new DefaultSizeEstimator(),
    //    new HoodieRecordSizeEstimator(readerSchema), diskMapType, isBitCaskDiskMapCompressionEnabled);
    this.scannedPrefixes = new HashSet<>();

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

  /**
   * Provides incremental scanning capability where only provided keys will be looked
   * up in the delta-log files, scanned and subsequently materialized into the internal
   * cache
   *
   * @param keys to be looked up
   */
  public void scanByFullKeys(List<String> keys) {
    // We can skip scanning in case reader is in full-scan mode, in which case all blocks
    // are processed upfront (no additional scanning is necessary)
    if (forceFullScan) {
      return; // no-op
    }

    List<String> missingKeys = keys.stream()
        .filter(key -> !records.containsKey(key))
        .collect(Collectors.toList());

    if (missingKeys.isEmpty()) {
      // All the required records are already fetched, no-op
      return;
    }

    scanInternal(Option.of(KeySpec.fullKeySpec(missingKeys)), false);
  }

  /**
   * Provides incremental scanning capability where only keys matching provided key-prefixes
   * will be looked up in the delta-log files, scanned and subsequently materialized into
   * the internal cache
   *
   * @param keyPrefixes to be looked up
   */
  public void scanByKeyPrefixes(List<String> keyPrefixes) {
    // We can skip scanning in case reader is in full-scan mode, in which case all blocks
    // are processed upfront (no additional scanning is necessary)
    if (forceFullScan) {
      return;
    }

    List<String> missingKeyPrefixes = keyPrefixes.stream()
        .filter(keyPrefix ->
            // NOTE: We can skip scanning the prefixes that have already
            //       been covered by the previous scans
            scannedPrefixes.stream().noneMatch(keyPrefix::startsWith))
        .collect(Collectors.toList());

    if (missingKeyPrefixes.isEmpty()) {
      // All the required records are already fetched, no-op
      return;
    }

    // NOTE: When looking up by key-prefixes unfortunately we can't short-circuit
    //       and will have to scan every time as we can't know (based on just
    //       the records cached) whether particular prefix was scanned or just records
    //       matching the prefix looked up (by [[scanByFullKeys]] API)
    scanInternal(Option.of(KeySpec.prefixKeySpec(missingKeyPrefixes)), false);
    scannedPrefixes.addAll(missingKeyPrefixes);
  }

  private void performScan() {
    // Do the scan and merge
    timer.startTimer();

    scanInternal(Option.empty(), false);

    this.totalTimeTakenToReadAndMergeBlocks = timer.endTimer();
    this.numMergedRecordsInLog = records.size();

    LOG.info("Number of log files scanned => " + logFilePaths.size());
    LOG.info("Number of entries in Map => " + records.size());
  }

  @Override
  public Iterator<Pair<Option<T>, Map<String, Object>>> iterator() {
    return records.values().iterator();
  }

  public Map<String, Pair<Option<T>, Map<String, Object>>> getRecords() {
    return records;
  }

  public HoodieRecord.HoodieRecordType getRecordType() {
    return recordMerger.getRecordType();
  }

  public long getNumMergedRecordsInLog() {
    return numMergedRecordsInLog;
  }

  /**
   * Returns the builder for {@code HoodieMergedLogRecordReader}.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public void processNextRecord(T newRecord, Map<String, Object> newRecordInfo) throws IOException {
    String key = readerContext.getRecordKey(newRecord);
    Pair<Option<T>, Map<String, Object>> prevRecordInfo = records.get(key);
    // TODO(yihua): need to revisit this logic around deletes, i.e., record option can be empty,
    //              and how record operation should be properly assigned
    if (prevRecordInfo != null) {
      // Merge and store the combined record
      // TODO(yihua): Need to rewrite this based on new API
      HoodieRecord<T> combinedRecord = (HoodieRecord<T>) recordMerger.merge(
          readerContext.constructHoodieRecord(prevRecordInfo), readerSchema,
          readerContext.constructHoodieRecord(Pair.of(Option.of(newRecord), newRecordInfo)),
          readerSchema, this.getPayloadProps()).get().getLeft();
      // If pre-combine returns existing record, no need to update it
      if (combinedRecord.getData() != prevRecordInfo.getLeft().get()) {
        //HoodieRecord latestHoodieRecord =
        //    combinedRecord.newInstance(new HoodieKey(key, newRecord.getPartitionPath()), newRecord.getOperation());


        // TODO(yihua): add instant time to meta
        //latestHoodieRecord.unseal();
        //latestHoodieRecord.setCurrentLocation(newRecord.getCurrentLocation());
        //latestHoodieRecord.seal();

        // NOTE: Record have to be cloned here to make sure if it holds low-level engine-specific
        //       payload pointing into a shared, mutable (underlying) buffer we get a clean copy of
        //       it since these records will be put into records(Map).
        // TODO(yihua): provide a mode to not copy for engine-specific optimization
        //   add current location
        //   reconstruct record info map
        records.put(key, Pair.of(Option.ofNullable(combinedRecord.getData()), newRecordInfo));
      }
    } else {
      // Put the record as is
      // NOTE: Record have to be cloned here to make sure if it holds low-level engine-specific
      //       payload pointing into a shared, mutable (underlying) buffer we get a clean copy of
      //       it since these records will be put into records(Map).
      records.put(key, Pair.of(Option.of(readerContext.copy(newRecord)), newRecordInfo));
    }
  }

  @Override
  protected void processNextDeletedRecord(DeleteRecord deleteRecord) {
    String key = deleteRecord.getRecordKey();
    Pair<Option<T>, Map<String, Object>> oldRecordInfo = records.get(key);
    if (oldRecordInfo != null) {
      // Merge and store the merged record. The ordering val is taken to decide whether the same key record
      // should be deleted or be kept. The old record is kept only if the DELETE record has smaller ordering val.
      // For same ordering values, uses the natural order(arrival time semantics).

      // Comparable curOrderingVal = oldRecord.getOrderingValue(this.readerSchema, this.hoodieTableMetaClient.getTableConfig().getProps());
      Comparable curOrderingVal = readerContext.getOrderingValue(oldRecordInfo, this.hoodieTableMetaClient.getTableConfig().getProps());
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
    records.put(key, Pair.of(Option.empty(),
        readerContext.generateMetaForDelete(key, deleteRecord.getPartitionPath(), deleteRecord.getOrderingValue())));

    /*
    if (recordType == HoodieRecord.HoodieRecordType.AVRO) {
      records.put(key, SpillableMapUtils.generateEmptyPayload(key,
          deleteRecord.getPartitionPath(), deleteRecord.getOrderingValue(), getPayloadClassFQN()));
    } else {
      HoodieEmptyRecord record = new HoodieEmptyRecord<>(new HoodieKey(key, deleteRecord.getPartitionPath()), null, deleteRecord.getOrderingValue(), recordType);
      records.put(key, record);
    }*/
  }

  public long getTotalTimeTakenToReadAndMergeBlocks() {
    return totalTimeTakenToReadAndMergeBlocks;
  }

  @Override
  public void close() {
    if (records != null) {
      records.clear();
    }
  }

  /**
   * Builder used to build {@code HoodieUnMergedLogRecordScanner}.
   */
  public static class Builder<T> extends BaseHoodieLogRecordReader.Builder<T> {
    private HoodieReaderContext<T> readerContext;
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
    // incremental filtering
    private Option<InstantRange> instantRange = Option.empty();
    private String partitionName;
    // operation field default false
    private boolean withOperationField = false;
    private String keyFieldOverride;
    // By default, we're doing a full-scan
    private boolean forceFullScan = true;
    private boolean enableOptimizedLogBlocksScan = false;
    private HoodieRecordMerger recordMerger = HoodiePreCombineAvroRecordMerger.INSTANCE;

    @Override
    public Builder<T> withHoodieReaderContext(HoodieReaderContext<T> readerContext) {
      this.readerContext = readerContext;
      return this;
    }

    @Override
    public Builder<T> withFileSystem(FileSystem fs) {
      this.fs = fs;
      return this;
    }

    @Override
    public Builder<T> withBasePath(String basePath) {
      this.basePath = basePath;
      return this;
    }

    @Override
    public Builder<T> withLogFilePaths(List<String> logFilePaths) {
      this.logFilePaths = logFilePaths.stream()
          .filter(p -> !p.endsWith(HoodieCDCUtils.CDC_LOGFILE_SUFFIX))
          .collect(Collectors.toList());
      return this;
    }

    @Override
    public Builder<T> withReaderSchema(Schema schema) {
      this.readerSchema = schema;
      return this;
    }

    @Override
    public Builder<T> withLatestInstantTime(String latestInstantTime) {
      this.latestInstantTime = latestInstantTime;
      return this;
    }

    @Override
    public Builder<T> withReadBlocksLazily(boolean readBlocksLazily) {
      this.readBlocksLazily = readBlocksLazily;
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
    public Builder<T> withInternalSchema(InternalSchema internalSchema) {
      this.internalSchema = internalSchema;
      return this;
    }

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

    @Override
    public Builder<T> withRecordMerger(HoodieRecordMerger recordMerger) {
      this.recordMerger = HoodieRecordUtils.mergerToPreCombineMode(recordMerger);
      return this;
    }

    public Builder<T> withKeyFiledOverride(String keyFieldOverride) {
      this.keyFieldOverride = Objects.requireNonNull(keyFieldOverride);
      return this;
    }

    public Builder<T> withForceFullScan(boolean forceFullScan) {
      this.forceFullScan = forceFullScan;
      return this;
    }

    @Override
    public HoodieMergedLogRecordReader<T> build() {
      if (this.partitionName == null && CollectionUtils.nonEmpty(this.logFilePaths)) {
        this.partitionName = getRelativePartitionPath(new Path(basePath), new Path(this.logFilePaths.get(0)).getParent());
      }
      ValidationUtils.checkArgument(recordMerger != null);

      return new HoodieMergedLogRecordReader<>(readerContext, fs, basePath, logFilePaths, readerSchema,
          latestInstantTime, readBlocksLazily, reverseReader,
          bufferSize, instantRange,
          withOperationField, forceFullScan,
          Option.ofNullable(partitionName), internalSchema, Option.ofNullable(keyFieldOverride), enableOptimizedLogBlocksScan, recordMerger);
    }
  }
}
