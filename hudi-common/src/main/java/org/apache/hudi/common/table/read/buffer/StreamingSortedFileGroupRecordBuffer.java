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

package org.apache.hudi.common.table.read.buffer;

import org.apache.hudi.avro.AvroSchemaCache;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.log.KeySpec;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.table.read.UpdateProcessor;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * A streaming buffer that assumes log records are sorted by record key.
 * This buffer performs sorted merge between base file and log records without
 * storing all log records in memory, significantly reducing memory footprint.
 *
 * <p>Key assumptions:
 * <ul>
 *   <li>Records in log files are sorted by record key within each data block</li>
 *   <li>Base file records (if provided) are sorted by record key</li>
 * </ul>
 *
 * <p>This implementation uses a multi-way merge approach:
 * <ul>
 *   <li>Maintains an iterator per log data block</li>
 *   <li>Uses a priority queue to efficiently select the next record by key</li>
 *   <li>Merges records with the same key on-the-fly</li>
 *   <li>Does sorted merge with base file records</li>
 * </ul>
 */
public class StreamingSortedFileGroupRecordBuffer<T> extends FileGroupRecordBuffer<T> {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingSortedFileGroupRecordBuffer.class);

  // List of iterators for each log data block, ordered by instant time
  private final List<LogBlockIterator> logBlockIterators = new ArrayList<>();

  // Priority queue for multi-way merge of log records
  private PriorityQueue<LogRecordEntry<T>> logRecordQueue;

  // The next base file record that has been read but not yet processed
  private Option<T> queuedBaseFileRecord = Option.empty();

  // Flag to track if we've started processing
  private boolean initialized = false;

  // Counter for tracking records
  private int recordCount = 0;

  public StreamingSortedFileGroupRecordBuffer(HoodieReaderContext<T> readerContext,
                                              HoodieTableMetaClient hoodieTableMetaClient,
                                              RecordMergeMode recordMergeMode,
                                              Option<PartialUpdateMode> partialUpdateModeOpt,
                                              TypedProperties props,
                                              List<String> orderingFieldNames,
                                              UpdateProcessor<T> updateProcessor) {
    super(readerContext, hoodieTableMetaClient, recordMergeMode, partialUpdateModeOpt, props, orderingFieldNames, updateProcessor);
  }

  @Override
  public BufferType getBufferType() {
    return BufferType.KEY_BASED_MERGE;
  }

  /**
   * Process a data block by creating an iterator for it.
   * The iterator will be lazily consumed during the merge phase.
   */
  @Override
  public void processDataBlock(HoodieDataBlock dataBlock, Option<KeySpec> keySpecOpt) throws IOException {
    Pair<ClosableIterator<T>, Schema> recordsIteratorSchemaPair = getRecordsIterator(dataBlock, keySpecOpt);

    if (dataBlock.containsPartialUpdates() && !enablePartialMerging) {
      enablePartialMerging = true;
      // Note: bufferedRecordMerger update would be needed if partial merging is enabled
    }

    Schema schema = AvroSchemaCache.intern(recordsIteratorSchemaPair.getRight());
    ClosableIterator<T> recordIterator = recordsIteratorSchemaPair.getLeft();

    // Create a log block iterator and add it to our list
    LogBlockIterator blockIterator = new LogBlockIterator(recordIterator, schema);
    logBlockIterators.add(blockIterator);

    LOG.debug("Added log block iterator for schema: {}", schema.getName());
  }

  /**
   * Process a single data record by buffering it temporarily.
   * In streaming mode, this is not the primary path - we prefer processDataBlock.
   */
  @Override
  public void processNextDataRecord(BufferedRecord<T> record, Serializable recordKey) throws IOException {
    // For streaming mode, we handle records through the iterator
    // This method is kept for compatibility but increments the counter
    totalLogRecords++;
    recordCount++;

    // Store in the parent's map for fallback compatibility
    BufferedRecord<T> existingRecord = records.get(recordKey);
    bufferedRecordMerger.deltaMerge(record, existingRecord).ifPresent(bufferedRecord ->
        records.put(recordKey, bufferedRecord.toBinary(readerContext.getRecordContext())));
  }

  /**
   * Process a delete block. For streaming mode, we create delete record entries.
   */
  @Override
  public void processDeleteBlock(HoodieDeleteBlock deleteBlock) throws IOException {
    Iterator<DeleteRecord> it = Arrays.stream(deleteBlock.getRecordsToDelete()).iterator();

    // Create a pseudo-iterator for delete records
    List<DeleteRecord> deleteRecords = new ArrayList<>();
    while (it.hasNext()) {
      deleteRecords.add(it.next());
    }

    if (!deleteRecords.isEmpty()) {
      DeleteBlockIterator deleteIterator = new DeleteBlockIterator(deleteRecords.iterator());
      logBlockIterators.add(deleteIterator);
      LOG.debug("Added delete block iterator with {} records", deleteRecords.size());
    }
  }

  @Override
  public void processNextDeletedRecord(DeleteRecord deleteRecord, Serializable recordIdentifier) {
    totalLogRecords++;
    recordCount++;

    // Store in parent's map for fallback compatibility
    BufferedRecord<T> existingRecord = records.get(recordIdentifier);
    Option<DeleteRecord> recordOpt = bufferedRecordMerger.deltaMerge(deleteRecord, existingRecord);
    recordOpt.ifPresent(deleteRec ->
        records.put(recordIdentifier, BufferedRecords.fromDeleteRecord(deleteRec, readerContext.getRecordContext())));
  }

  @Override
  public boolean containsLogRecord(String recordKey) {
    // In streaming mode, we don't maintain a full index
    // Fall back to the parent's records map
    return records.containsKey(recordKey);
  }

  @Override
  public int size() {
    return recordCount;
  }

  /**
   * Initialize the priority queue with the first record from each log block iterator.
   */
  private void initialize() throws IOException {
    if (initialized) {
      return;
    }

    initialized = true;
    logRecordQueue = new PriorityQueue<>((a, b) -> a.recordKey.compareTo(b.recordKey));

    // Prime the queue with the first record from each iterator
    for (int i = 0; i < logBlockIterators.size(); i++) {
      LogBlockIterator blockIterator = logBlockIterators.get(i);
      if (blockIterator.hasNext()) {
        LogRecordEntry<T> entry = blockIterator.next();
        entry.iteratorIndex = i;
        logRecordQueue.offer(entry);
      }
    }

    LOG.info("Initialized streaming merge with {} active log block iterators", logBlockIterators.size());
  }

  /**
   * Get the next log record by record key order.
   * Handles merging of records with the same key from multiple log blocks.
   */
  private Option<BufferedRecord<T>> getNextLogRecord() throws IOException {
    if (!initialized) {
      initialize();
    }

    if (logRecordQueue.isEmpty()) {
      return Option.empty();
    }

    // Get the record with the smallest key
    LogRecordEntry<T> currentEntry = logRecordQueue.poll();
    String currentKey = currentEntry.recordKey;
    BufferedRecord<T> mergedRecord = currentEntry.record;

    // Refill the queue from the iterator we just consumed
    LogBlockIterator sourceIterator = logBlockIterators.get(currentEntry.iteratorIndex);
    if (sourceIterator.hasNext()) {
      LogRecordEntry<T> nextEntry = sourceIterator.next();
      nextEntry.iteratorIndex = currentEntry.iteratorIndex;
      logRecordQueue.offer(nextEntry);
    }

    // Merge all records with the same key
    while (!logRecordQueue.isEmpty() && logRecordQueue.peek().recordKey.equals(currentKey)) {
      LogRecordEntry<T> sameKeyEntry = logRecordQueue.poll();

      // Merge this record with our current merged result
      Option<BufferedRecord<T>> mergeResult = bufferedRecordMerger.deltaMerge(sameKeyEntry.record, mergedRecord);
      if (mergeResult.isPresent()) {
        mergedRecord = mergeResult.get();
      }

      // Refill the queue
      LogBlockIterator sameKeyIterator = logBlockIterators.get(sameKeyEntry.iteratorIndex);
      if (sameKeyIterator.hasNext()) {
        LogRecordEntry<T> nextEntry = sameKeyIterator.next();
        nextEntry.iteratorIndex = sameKeyEntry.iteratorIndex;
        logRecordQueue.offer(nextEntry);
      }
    }

    return Option.of(mergedRecord);
  }

  /**
   * Perform sorted merge between base file record and log records.
   */
  @Override
  protected boolean doHasNext() throws IOException {
    ValidationUtils.checkState(baseFileIterator != null, "Base file iterator has not been set yet");

    if (!initialized) {
      initialize();
    }

    // First, check if we have a queued base file record from the previous iteration
    if (queuedBaseFileRecord.isPresent()) {
      T baseRecord = queuedBaseFileRecord.get();
      queuedBaseFileRecord = Option.empty();
      if (processBaseRecord(baseRecord)) {
        return true;
      }
    }

    // Main merge loop between base file and log records
    while (baseFileIterator.hasNext() || !logRecordQueue.isEmpty()) {
      // Peek at the next log record without consuming it
      Option<String> nextLogRecordKey = Option.empty();
      if (!logRecordQueue.isEmpty()) {
        nextLogRecordKey = Option.of(logRecordQueue.peek().recordKey);
      }

      // If we have base file records left
      if (baseFileIterator.hasNext()) {
        T baseRecord = baseFileIterator.next();
        String baseRecordKey = readerContext.getRecordContext().getRecordKey(baseRecord, readerSchema);

        // Compare base record key with next log record key
        if (nextLogRecordKey.isEmpty()) {
          // No more log records, just process base record
          nextRecord = bufferedRecordConverter.convert(readerContext.getRecordContext().seal(baseRecord));
          return true;
        }

        int comparison = baseRecordKey.compareTo(nextLogRecordKey.get());

        if (comparison < 0) {
          // Base record comes first, emit it as-is
          nextRecord = bufferedRecordConverter.convert(readerContext.getRecordContext().seal(baseRecord));
          return true;
        } else if (comparison == 0) {
          // Keys match, merge them
          Option<BufferedRecord<T>> logRecord = getNextLogRecord();
          if (logRecord.isPresent()) {
            BufferedRecord<T> baseRecordInfo = BufferedRecords.fromEngineRecord(
                baseRecord, readerSchema, readerContext.getRecordContext(), orderingFieldNames, false);
            BufferedRecord<T> mergeResult = bufferedRecordMerger.finalMerge(baseRecordInfo, logRecord.get());
            nextRecord = updateProcessor.processUpdate(logRecord.get().getRecordKey(), baseRecordInfo, mergeResult, mergeResult.isDelete());
            if (nextRecord != null) {
              return true;
            }
          }
        } else {
          // Log record comes first (comparison > 0)
          // Queue the base record for next iteration and process log record
          queuedBaseFileRecord = Option.of(baseRecord);
          Option<BufferedRecord<T>> logRecord = getNextLogRecord();
          if (logRecord.isPresent()) {
            nextRecord = updateProcessor.processUpdate(logRecord.get().getRecordKey(), null, logRecord.get(), logRecord.get().isDelete());
            if (nextRecord != null) {
              return true;
            }
          }
        }
      } else {
        // No more base records, just process remaining log records
        Option<BufferedRecord<T>> logRecord = getNextLogRecord();
        if (logRecord.isPresent()) {
          nextRecord = updateProcessor.processUpdate(logRecord.get().getRecordKey(), null, logRecord.get(), logRecord.get().isDelete());
          if (nextRecord != null) {
            return true;
          }
        } else {
          break;
        }
      }
    }

    return false;
  }

  /**
   * Process a base record directly without merging.
   */
  private boolean processBaseRecord(T baseRecord) throws IOException {
    nextRecord = bufferedRecordConverter.convert(readerContext.getRecordContext().seal(baseRecord));
    return true;
  }

  @Override
  public void close() {
    // Close all log block iterators
    for (LogBlockIterator iterator : logBlockIterators) {
      try {
        iterator.close();
      } catch (IOException e) {
        LOG.warn("Error closing log block iterator", e);
      }
    }
    logBlockIterators.clear();

    if (logRecordQueue != null) {
      logRecordQueue.clear();
    }

    super.close();
  }

  /**
   * Entry in the priority queue representing a record from a log block.
   */
  private static class LogRecordEntry<T> {
    String recordKey;
    BufferedRecord<T> record;
    int iteratorIndex; // Index of the iterator this record came from

    LogRecordEntry(String recordKey, BufferedRecord<T> record) {
      this.recordKey = recordKey;
      this.record = record;
    }
  }

  /**
   * Iterator wrapper for a log data block.
   */
  private class LogBlockIterator {
    private final ClosableIterator<T> recordIterator;
    private final Schema schema;
    private final RecordContext<T> recordContext;

    LogBlockIterator(ClosableIterator<T> recordIterator, Schema schema) {
      this.recordIterator = recordIterator;
      this.schema = schema;
      this.recordContext = readerContext.getRecordContext();
    }

    boolean hasNext() throws IOException {
      return recordIterator.hasNext();
    }

    LogRecordEntry<T> next() throws IOException {
      T nextRecord = recordIterator.next();
      boolean isDelete = recordContext.isDeleteRecord(nextRecord, deleteContext);
      BufferedRecord<T> bufferedRecord = BufferedRecords.fromEngineRecord(
          nextRecord, schema, recordContext, orderingFieldNames, isDelete);
      totalLogRecords++;
      return new LogRecordEntry<>(bufferedRecord.getRecordKey(), bufferedRecord);
    }

    void close() throws IOException {
      recordIterator.close();
    }
  }

  /**
   * Iterator wrapper for a delete block.
   */
  private class DeleteBlockIterator extends LogBlockIterator {
    private final Iterator<DeleteRecord> deleteIterator;

    DeleteBlockIterator(Iterator<DeleteRecord> deleteIterator) {
      super(null, null);
      this.deleteIterator = deleteIterator;
    }

    @Override
    boolean hasNext() {
      return deleteIterator.hasNext();
    }

    @Override
    LogRecordEntry<T> next() {
      DeleteRecord deleteRecord = deleteIterator.next();
      BufferedRecord<T> bufferedRecord = BufferedRecords.fromDeleteRecord(
          deleteRecord, readerContext.getRecordContext());
      totalLogRecords++;
      return new LogRecordEntry<>(deleteRecord.getRecordKey(), bufferedRecord);
    }

    @Override
    void close() {
      // No-op for delete iterator
    }
  }
}
