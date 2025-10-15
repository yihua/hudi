# Streaming Sorted Log Record Reader

## Overview

The `StreamingSortedLogRecordReader` and `StreamingSortedFileGroupRecordBuffer` provide a memory-efficient way to read and merge log records in Apache Hudi. Unlike the traditional `HoodieMergedLogRecordReader` which loads all log records into memory before merging, the streaming approach performs sorted merge on-the-fly, significantly reducing memory usage.

## Key Features

- **Reduced Memory Usage**: Only maintains minimal state per log block instead of storing all records in memory
- **Streaming Merge**: Uses priority queue-based multi-way merge algorithm
- **Sorted Output**: Maintains sorted order by record key throughout the merge process
- **Compatible with Existing Infrastructure**: Works with existing Hudi log file formats and merge strategies

## Requirements

**Critical**: The streaming reader assumes:
1. Records within each log data block are sorted by record key
2. Base file records (if present) are sorted by record key

If these requirements are not met, the merge results will be incorrect.

## Architecture

### StreamingSortedFileGroupRecordBuffer

The buffer maintains:
- A list of iterators, one per log data block
- A priority queue for efficient multi-way merge
- Minimal memory footprint - only the current record from each block is kept in memory

### Multi-Way Merge Algorithm

```
1. Initialize: Create an iterator for each log block
2. Prime: Add the first record from each iterator to a priority queue (min-heap by record key)
3. Merge Loop:
   a. Extract the record with the smallest key from the queue
   b. If there are more records with the same key, merge them together
   c. Refill the queue with the next record from the consumed iterator(s)
   d. Continue until queue is empty
4. Base File Merge: Perform sorted merge between log records and base file records
```

### Memory Comparison

**Traditional Approach (KeyBasedFileGroupRecordBuffer)**:
```
Memory Usage = O(total_log_records)
```

**Streaming Approach (StreamingSortedFileGroupRecordBuffer)**:
```
Memory Usage = O(num_log_blocks) + O(priority_queue_size)
             ≈ O(num_log_blocks)  [typically << total_log_records]
```

For example, if you have 1 million log records across 10 log blocks:
- Traditional: ~1,000,000 records in memory
- Streaming: ~10 records in memory (one per iterator) + priority queue overhead

## Usage

### Basic Usage

```java
import org.apache.hudi.common.table.log.StreamingSortedLogRecordReader;
import org.apache.hudi.common.table.read.buffer.StreamingSortedFileGroupRecordBuffer;

// Create the streaming buffer
StreamingSortedFileGroupRecordBuffer<InternalRow> buffer =
    new StreamingSortedFileGroupRecordBuffer<>(
        readerContext,
        metaClient,
        RecordMergeMode.OVERWRITE_WITH_LATEST,
        Option.empty(),
        props,
        orderingFieldNames,
        updateProcessor);

// Create the streaming reader
StreamingSortedLogRecordReader<InternalRow> reader =
    StreamingSortedLogRecordReader.<InternalRow>newBuilder()
        .withHoodieReaderContext(readerContext)
        .withStorage(storage)
        .withLogFiles(logFiles)
        .withMetaClient(metaClient)
        .withRecordBuffer(buffer)
        .withForceFullScan(true)
        .build();

// Iterate through merged records
for (BufferedRecord<InternalRow> record : reader) {
    // Process record
    processRecord(record);
}

// Clean up
reader.close();
```

### With Base File Merging

```java
// Set the base file iterator for sorted merge
ClosableIterator<InternalRow> baseFileIterator = createBaseFileIterator(baseFilePath);
buffer.setBaseFileIterator(baseFileIterator);

// Use hasNext/next pattern for manual iteration
while (buffer.hasNext()) {
    BufferedRecord<InternalRow> mergedRecord = buffer.next();
    // Process merged record (combines base + log)
    processRecord(mergedRecord);
}
```

### With Instant Range Filtering

```java
// Only process log records within a specific instant range
InstantRange range = InstantRange.builder()
    .startInstant("20240101000000")
    .endInstant("20240131235959")
    .build();

StreamingSortedLogRecordReader<InternalRow> reader =
    StreamingSortedLogRecordReader.<InternalRow>newBuilder()
        .withHoodieReaderContext(readerContext)
        .withStorage(storage)
        .withLogFiles(logFiles)
        .withMetaClient(metaClient)
        .withRecordBuffer(buffer)
        .withInstantRange(Option.of(range))
        .build();
```

## When to Use

### Use Streaming Sorted Reader When:
- ✅ Log records are sorted by record key
- ✅ Memory is constrained
- ✅ Processing large log files (hundreds of MB to GBs)
- ✅ You need sorted output
- ✅ You can ensure base file records are sorted

### Use Traditional Reader When:
- ❌ Log records are not sorted
- ❌ Memory is abundant
- ❌ Processing small log files (< 100MB)
- ❌ Random access to records is needed
- ❌ You need the complete in-memory map for other operations

## Performance Considerations

### Memory Usage
The streaming reader uses approximately:
```
Memory ≈ (num_log_blocks × avg_record_size) + priority_queue_overhead
```

For 100 log blocks with 1KB average record size:
```
Memory ≈ 100 × 1KB + ~10KB = ~110KB
```

Compare this to traditional approach:
```
Memory ≈ total_records × avg_record_size
      = 1,000,000 × 1KB = ~1GB
```

### CPU Usage
- **Streaming**: Slightly higher CPU due to priority queue operations (O(log n) per record)
- **Traditional**: Lower CPU for reading, but higher for initial merge

### I/O Pattern
Both readers have similar I/O patterns as they scan log files sequentially.

## Implementation Details

### How Records are Sorted in Log Files

When writing log files with sorted records, ensure:

```java
// Sort records by key before writing
List<HoodieRecord> sortedRecords = records.stream()
    .sorted(Comparator.comparing(HoodieRecord::getRecordKey))
    .collect(Collectors.toList());

// Write sorted records to log block
HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(
    sortedRecords,
    header,
    keyFieldName,
    instantTime);
```

### Priority Queue Key Comparison

Records are compared by their record key (string comparison):
```java
logRecordQueue = new PriorityQueue<>((a, b) ->
    a.recordKey.compareTo(b.recordKey));
```

### Merging Records with Same Key

When multiple records have the same key:
1. Records are merged using the configured `BufferedRecordMerger`
2. Merge strategy respects the ordering field (e.g., timestamp)
3. Final merged result is emitted

## Limitations

1. **Requires Sorted Input**: Will produce incorrect results if records are not sorted
2. **No Random Access**: Cannot efficiently look up individual records by key
3. **Single Pass**: Records can only be iterated once in streaming mode
4. **Memory Fallback**: Still uses parent's map for compatibility methods like `containsLogRecord()`

## Future Enhancements

Potential improvements:
- [ ] Add validation to detect unsorted records and fail fast
- [ ] Support for partially sorted blocks (sort each block on-the-fly if needed)
- [ ] Parallel processing of independent log blocks
- [ ] Metrics and monitoring for memory savings
- [ ] Adaptive strategy selection based on log file characteristics

## Testing

Run the test suite:
```bash
mvn test -Dtest=TestStreamingSortedLogRecordReader
```

## References

- [Multi-way merge algorithm](https://en.wikipedia.org/wiki/K-way_merge_algorithm)
- [HoodieMergedLogRecordReader](../../main/java/org/apache/hudi/common/table/log/HoodieMergedLogRecordReader.java)
- [SortedKeyBasedFileGroupRecordBuffer](../../main/java/org/apache/hudi/common/table/read/buffer/SortedKeyBasedFileGroupRecordBuffer.java)
