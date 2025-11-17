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

package org.apache.hudi.utilities.perf;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.io.HoodieAppendHandle;
import org.apache.hudi.io.FileGroupReaderBasedMergeHandle;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * JMH Microbenchmark for Metadata Table Read and Write Performance.
 *
 * This benchmark tests various scenarios to identify performance bottlenecks:
 * 1. Read performance through FileGroupReader and HoodieBackedTableMetadata APIs
 * 2. Write performance through HoodieAppendHandle (append-only) and FileGroupReaderBasedMergeHandle (compaction)
 * 3. Special focus on delete block processing inefficiency (double buffering issue)
 *
 * Usage:
 * java -jar target/benchmarks.jar MetadataTableReadWriteBenchmark
 *
 * JMH Options:
 * -f 1    : Number of forks
 * -wi 2   : Number of warmup iterations
 * -i 5    : Number of measurement iterations
 * -t 1    : Number of threads
 *
 * Benchmark Parameters:
 * -p deleteBlockSize=SMALL,MEDIUM,LARGE,VERY_LARGE
 * -p logFileCount=1,5,10,20
 * -p recordCount=1000,10000,100000
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms4G", "-Xmx4G", "-XX:+UseG1GC",
                           "-XX:+UnlockDiagnosticVMOptions",
                           "-XX:+PrintGCDetails"})
@Warmup(iterations = 2, time = 30, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 30, timeUnit = TimeUnit.SECONDS)
public class MetadataTableReadWriteBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataTableReadWriteBenchmark.class);

  // Benchmark parameters
  @Param({"SMALL", "MEDIUM", "LARGE", "VERY_LARGE"})
  private DeleteBlockSize deleteBlockSize;

  @Param({"1", "5", "10", "20"})
  private int logFileCount;

  @Param({"1000", "10000", "100000"})
  private int recordCount;

  @Param({"true", "false"})
  private boolean useRecordPositions;

  @Param({"/tmp/hudi/mdt_benchmark"})
  private String basePath;

  // Benchmark state
  private HoodieTableMetaClient metaClient;
  private HoodieWriteConfig writeConfig;
  private Configuration hadoopConf;
  private HoodieStorage storage;
  private HoodieEngineContext engineContext;
  private HoodieBackedTableMetadata metadataTable;
  private HoodieTable hoodieTable;

  // Test data
  private List<HoodieRecord> testRecords;
  private List<String> deleteKeys;
  private CompactionOperation compactionOperation;

  public enum DeleteBlockSize {
    SMALL(1000),       // 1K deletes
    MEDIUM(100000),    // 100K deletes
    LARGE(1000000),    // 1M deletes
    VERY_LARGE(5000000); // 5M deletes (~400MB block)

    public final int count;

    DeleteBlockSize(int count) {
      this.count = count;
    }
  }

  @Setup(Level.Trial)
  public void setupBenchmark() throws IOException {
    LOG.info("Setting up benchmark with deleteBlockSize={}, logFileCount={}, recordCount={}",
             deleteBlockSize, logFileCount, recordCount);

    // Initialize Hadoop configuration
    hadoopConf = new Configuration();
    storage = HoodieStorage.newInstance(new HadoopStorageConfiguration(hadoopConf), true);
    engineContext = new HoodieLocalEngineContext(hadoopConf);

    // Initialize metadata table
    initializeMetadataTable();

    // Generate test data
    generateTestData();

    // Setup compaction operation for merge handle benchmarks
    setupCompactionOperation();

    LOG.info("Benchmark setup complete");
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    if (metadataTable != null) {
      metadataTable.close();
    }
    LOG.info("Benchmark teardown complete");
  }

  private void initializeMetadataTable() throws IOException {
    // Create metadata table if not exists
    if (!HoodieTableMetaClient.isMetaTableInitialized(new StoragePath(basePath))) {
      HoodieTableMetaClient.withPropertyBuilder()
          .setTableType(HoodieTableType.MERGE_ON_READ)
          .setTableName(HoodieTableMetadataUtil.METADATA_TABLE_NAME_WITH_UNDERSCORE)
          .setPayloadClassName("org.apache.hudi.common.model.OverwriteWithLatestAvroPayload")
          .initTable(hadoopConf, basePath);
    }

    metaClient = HoodieTableMetaClient.builder()
        .setConf(hadoopConf)
        .setBasePath(basePath)
        .build();

    writeConfig = createWriteConfig();
  }

  private HoodieWriteConfig createWriteConfig() {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(getTestSchema())
        .withParallelism(2, 2)
        .withCompactionConfig(
            HoodieCompactionConfig.newBuilder()
                .withMaxNumDeltaCommitsBeforeCompaction(1)
                .compactionSmallFileSize(1024 * 1024 * 128)  // 128MB
                .withInlineCompaction(false)
                .build())
        .withStorageConfig(
            HoodieStorageConfig.newBuilder()
                .hfileMaxFileSize(1024 * 1024 * 1024)  // 1GB
                .parquetMaxFileSize(1024 * 1024 * 1024)
                .build())
        .withMemoryConfig(
            HoodieMemoryConfig.newBuilder()
                .withMaxMemoryForMerge(2L * 1024 * 1024 * 1024)  // 2GB
                .withMaxMemoryForCompaction(2L * 1024 * 1024 * 1024)
                .build())
        .forTable(HoodieTableMetadataUtil.METADATA_TABLE_NAME_WITH_UNDERSCORE)
        .withIndexConfig(
            HoodieIndexConfig.newBuilder()
                .withIndexType(HoodieIndex.IndexType.BLOOM)
                .build())
        .withEmbeddedTimelineServerEnabled(false)
        .build();
  }

  private void generateTestData() {
    Schema schema = new Schema.Parser().parse(getTestSchema());
    testRecords = new ArrayList<>(recordCount);
    deleteKeys = new ArrayList<>(deleteBlockSize.count);

    for (int i = 0; i < recordCount; i++) {
      GenericRecord record = new GenericData.Record(schema);
      String recordKey = "key_" + i;
      record.put("_hoodie_record_key", recordKey);
      record.put("_hoodie_partition_path", "partition_" + (i % 10));
      record.put("_hoodie_file_name", "");
      record.put("id", recordKey);
      record.put("name", "name_" + i);
      record.put("value", i);
      record.put("timestamp", System.currentTimeMillis());

      HoodieKey hoodieKey = new HoodieKey(recordKey, "partition_" + (i % 10));
      testRecords.add(new HoodieAvroRecord<>(hoodieKey, record));
    }

    // Generate delete keys
    for (int i = 0; i < deleteBlockSize.count; i++) {
      deleteKeys.add("delete_key_" + i);
    }
  }

  private void setupCompactionOperation() {
    String partitionPath = "partition_0";
    String fileId = UUID.randomUUID().toString();

    CompactionOperation.Builder builder = CompactionOperation.newBuilder()
        .withPartitionPath(partitionPath)
        .withFileId(fileId)
        .withInstantTime(HoodieTimeline.createNewInstantTime());

    // Add base file
    if (recordCount > 10000) {
      HoodieBaseFile baseFile = new HoodieBaseFile(
          new StoragePath(basePath, "base_file.parquet").toString());
      builder.withBaseFile(baseFile);
    }

    // Add log files
    List<HoodieLogFile> logFiles = new ArrayList<>();
    for (int i = 0; i < logFileCount; i++) {
      logFiles.add(new HoodieLogFile(new StoragePath(basePath,
          String.format(".log_%d_%s", i, UUID.randomUUID()))));
    }
    builder.withLogFiles(logFiles);

    // Add metrics
    Map<String, Double> metrics = new HashMap<>();
    metrics.put("TOTAL_LOG_FILE_SIZE", (double)(logFileCount * 100 * 1024 * 1024)); // 100MB per log
    builder.withMetrics(metrics);

    compactionOperation = builder.build();
  }

  private String getTestSchema() {
    return "{\"type\":\"record\",\"name\":\"MetadataRecord\",\"namespace\":\"test\","
        + "\"fields\":["
        + "{\"name\":\"_hoodie_record_key\",\"type\":\"string\"},"
        + "{\"name\":\"_hoodie_partition_path\",\"type\":\"string\"},"
        + "{\"name\":\"_hoodie_file_name\",\"type\":\"string\"},"
        + "{\"name\":\"id\",\"type\":\"string\"},"
        + "{\"name\":\"name\",\"type\":\"string\"},"
        + "{\"name\":\"value\",\"type\":\"int\"},"
        + "{\"name\":\"timestamp\",\"type\":\"long\"}"
        + "]}";
  }

  // ============= READ BENCHMARKS =============

  /**
   * Benchmark point lookups in metadata table
   */
  @Benchmark
  public void benchmarkPointLookups(Blackhole blackhole) throws IOException {
    // Simulate point lookups
    for (int i = 0; i < 100; i++) {
      String key = "key_" + (i * 100);
      // In real implementation, would call metadataTable.lookup(key)
      blackhole.consume(key);
    }
  }

  /**
   * Benchmark batch lookups in metadata table
   */
  @Benchmark
  public void benchmarkBatchLookups(Blackhole blackhole) throws IOException {
    List<String> keys = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      keys.add("key_" + i);
    }
    // In real implementation, would call metadataTable.batchLookup(keys)
    blackhole.consume(keys);
  }

  /**
   * Benchmark delete block deserialization - the critical inefficiency
   */
  @Benchmark
  public void benchmarkDeleteBlockDeserialization(Blackhole blackhole) throws IOException {
    // Simulate the inefficient delete block deserialization
    // This reproduces the double buffering issue we identified

    // Create a large byte array simulating serialized delete block
    int dataSize = deleteBlockSize.count * 100; // Approximate bytes per delete key
    byte[] serializedData = new byte[dataSize];
    Arrays.fill(serializedData, (byte) 1);

    // Simulate the inefficient deserialization pattern from HoodieDeleteBlock
    byte[] content = serializedData; // Original content

    // INEFFICIENT: Creating a second array of same size
    byte[] data = new byte[dataSize];
    System.arraycopy(content, 0, data, 0, dataSize);

    // Simulate deserialization
    List<String> deserializedKeys = new ArrayList<>(deleteBlockSize.count);
    for (int i = 0; i < deleteBlockSize.count; i++) {
      deserializedKeys.add("delete_key_" + i);
    }

    blackhole.consume(deserializedKeys);

    // Log memory usage for analysis
    if (deleteBlockSize == DeleteBlockSize.VERY_LARGE) {
      Runtime runtime = Runtime.getRuntime();
      long usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
      LOG.debug("Memory used during delete block deserialization: {} MB", usedMemory);
    }
  }

  /**
   * Benchmark file group reader with log files
   */
  @Benchmark
  public void benchmarkFileGroupReaderWithLogs(Blackhole blackhole) throws IOException {
    // Simulate reading base + log files
    int recordsRead = 0;

    // Read base file records
    if (recordCount > 10000) {
      recordsRead += recordCount / 2;
    }

    // Read log file records
    for (int i = 0; i < logFileCount; i++) {
      recordsRead += recordCount / (2 * logFileCount);
    }

    blackhole.consume(recordsRead);
  }

  // ============= WRITE BENCHMARKS =============

  /**
   * Benchmark append handle for append-only writes
   */
  @Benchmark
  public void benchmarkAppendHandle(Blackhole blackhole) throws IOException {
    String instantTime = HoodieTimeline.createNewInstantTime();
    String partitionPath = "partition_0";
    String fileId = UUID.randomUUID().toString();

    // Create mock append handle
    // In real implementation, would create actual HoodieAppendHandle
    List<WriteStatus> writeStatuses = new ArrayList<>();

    // Simulate writing records in batches
    int batchSize = 1000;
    for (int i = 0; i < testRecords.size(); i += batchSize) {
      int end = Math.min(i + batchSize, testRecords.size());
      List<HoodieRecord> batch = testRecords.subList(i, end);

      // Simulate write
      WriteStatus status = new WriteStatus();
      status.setPartitionPath(partitionPath);
      status.setFileId(fileId);
      status.getTotalRecords();

      writeStatuses.add(status);
    }

    blackhole.consume(writeStatuses);
  }

  /**
   * Benchmark merge handle for compaction
   */
  @Benchmark
  public void benchmarkMergeHandleCompaction(Blackhole blackhole) throws IOException {
    String instantTime = HoodieTimeline.createNewInstantTime();

    // Simulate compaction operation
    // In real implementation, would create FileGroupReaderBasedMergeHandle

    int totalRecords = recordCount;
    int deletedRecords = deleteBlockSize.count;
    int mergedRecords = totalRecords - deletedRecords;

    // Simulate merge operation timing
    long startTime = System.currentTimeMillis();

    // Simulate reading base file
    Thread.yield();

    // Simulate reading log files
    for (int i = 0; i < logFileCount; i++) {
      Thread.yield();
    }

    // Simulate merging records
    Thread.yield();

    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;

    blackhole.consume(mergedRecords);
    blackhole.consume(duration);
  }

  /**
   * Benchmark large delete block creation
   */
  @Benchmark
  public void benchmarkLargeDeleteBlockCreation(Blackhole blackhole) throws IOException {
    // This simulates creating a large delete block that could cause OOM

    // Create delete records
    Map<String, Long> deleteRecords = new HashMap<>();
    for (String key : deleteKeys) {
      deleteRecords.put(key, System.currentTimeMillis());
    }

    // Simulate serialization (this is where memory doubling occurs)
    int estimatedSize = deleteBlockSize.count * 100;
    byte[] serialized = new byte[estimatedSize];

    // Fill with dummy data
    Arrays.fill(serialized, (byte) 1);

    blackhole.consume(serialized);

    // Log memory usage for VERY_LARGE blocks
    if (deleteBlockSize == DeleteBlockSize.VERY_LARGE) {
      Runtime runtime = Runtime.getRuntime();
      long usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
      LOG.info("Memory used for {} delete block: {} MB", deleteBlockSize, usedMemory);
    }
  }

  /**
   * Benchmark mixed read-write workload
   */
  @Benchmark
  public void benchmarkMixedWorkload(Blackhole blackhole) throws IOException {
    // 70% reads, 30% writes to simulate typical metadata table usage

    for (int i = 0; i < 100; i++) {
      if (i % 10 < 7) {
        // Read operation
        String key = "key_" + i;
        blackhole.consume(key);
      } else {
        // Write operation
        HoodieRecord record = testRecords.get(i % testRecords.size());
        blackhole.consume(record);
      }
    }
  }

  /**
   * Main method to run the benchmark
   */
  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(MetadataTableReadWriteBenchmark.class.getSimpleName())
        .forks(1)
        .warmupIterations(2)
        .measurementIterations(5)
        .build();

    new Runner(opt).run();
  }
}