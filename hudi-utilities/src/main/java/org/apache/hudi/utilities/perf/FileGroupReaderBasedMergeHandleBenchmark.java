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

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.io.FileGroupReaderBasedMergeHandle;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.UniformReservoir;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.internal.SQLConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Microbenchmark for FileGroupReaderBasedMergeHandle to measure compaction performance
 * for MDT (Metadata Table) file groups.
 *
 * This benchmark supports both local and S3 cloud storage paths for testing
 * compaction performance across different storage backends.
 *
 * Example usage:
 * - Local: --base-path /tmp/hudi/mdt --log-files file1.log,file2.log --base-file base.parquet
 * - S3: --base-path s3://bucket/path/mdt --log-files file1.log,file2.log --base-file base.parquet
 */
public class FileGroupReaderBasedMergeHandleBenchmark implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(FileGroupReaderBasedMergeHandleBenchmark.class);

  private final Config cfg;
  private transient JavaSparkContext jsc;
  private transient HoodieSparkEngineContext engineContext;
  private transient HoodieTableMetaClient metaClient;
  private transient HoodieTable hoodieTable;

  public static class Config implements Serializable {

    @Parameter(names = {"--base-path", "-bp"}, description = "Base path for MDT table (local or S3)",
        required = true)
    public String basePath;

    @Parameter(names = {"--base-file", "-bf"}, description = "Path to base file (relative to base path)")
    public String baseFile;

    @Parameter(names = {"--log-files", "-lf"}, description = "Comma-separated list of log file paths (relative to base path)")
    public String logFiles;

    @Parameter(names = {"--partition-path", "-pp"}, description = "Partition path within the table")
    public String partitionPath = "";

    @Parameter(names = {"--file-id", "-fid"}, description = "File ID for the file group")
    public String fileId = "test-file-id";

    @Parameter(names = {"--num-iterations", "-n"}, description = "Number of benchmark iterations")
    public int numIterations = 10;

    @Parameter(names = {"--warmup-iterations", "-w"}, description = "Number of warmup iterations")
    public int warmupIterations = 2;

    @Parameter(names = {"--output-path", "-o"}, description = "Output path for compacted file")
    public String outputPath;

    @Parameter(names = {"--spark-master", "-sm"}, description = "Spark master URL")
    public String sparkMaster = "local[4]";

    @Parameter(names = {"--spark-memory", "-mem"}, description = "Spark executor memory")
    public String sparkMemory = "4g";

    @Parameter(names = {"--max-memory-for-compaction", "-mmc"}, description = "Max memory for compaction in MB")
    public long maxMemoryForCompaction = 1024;

    @Parameter(names = {"--use-record-positions", "-urp"}, description = "Use record positions for merging")
    public boolean useRecordPositions = false;

    @Parameter(names = {"--sort-output", "-so"}, description = "Sort output records")
    public boolean sortOutput = false;

    @Parameter(names = {"--help", "-h"}, help = true)
    public boolean help = false;

    // S3 specific configurations
    @Parameter(names = {"--aws-access-key", "-ak"}, description = "AWS access key for S3")
    public String awsAccessKey;

    @Parameter(names = {"--aws-secret-key", "-sk"}, description = "AWS secret key for S3")
    public String awsSecretKey;

    @Parameter(names = {"--aws-session-token", "-st"}, description = "AWS session token for S3")
    public String awsSessionToken;

    @Parameter(names = {"--aws-region", "-ar"}, description = "AWS region for S3")
    public String awsRegion = "us-east-1";

    @Parameter(names = {"--s3-endpoint", "-se"}, description = "Custom S3 endpoint (for S3-compatible storage)")
    public String s3Endpoint;
  }

  public FileGroupReaderBasedMergeHandleBenchmark(Config cfg) {
    this.cfg = cfg;
  }

  private void initializeSpark() {
    SparkConf sparkConf = new SparkConf()
        .setAppName("FileGroupReaderBasedMergeHandleBenchmark")
        .setMaster(cfg.sparkMaster)
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.sql.shuffle.partitions", "4")
        .set("spark.executor.memory", cfg.sparkMemory)
        .set("spark.executor.memoryOverhead", "512m");

    // Set S3 configurations if provided
    if (cfg.awsAccessKey != null && cfg.awsSecretKey != null) {
      sparkConf.set("spark.hadoop.fs.s3a.access.key", cfg.awsAccessKey)
               .set("spark.hadoop.fs.s3a.secret.key", cfg.awsSecretKey);

      if (cfg.awsSessionToken != null) {
        sparkConf.set("spark.hadoop.fs.s3a.session.token", cfg.awsSessionToken);
      }

      if (cfg.s3Endpoint != null) {
        sparkConf.set("spark.hadoop.fs.s3a.endpoint", cfg.s3Endpoint);
      }

      sparkConf.set("spark.hadoop.fs.s3a.endpoint.region", cfg.awsRegion)
               .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
               .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
               .set("spark.hadoop.fs.s3a.fast.upload", "true")
               .set("spark.hadoop.fs.s3a.block.size", "128M")
               .set("spark.hadoop.fs.s3a.multipart.size", "128M");
    }

    this.jsc = new JavaSparkContext(sparkConf);
    this.engineContext = new HoodieSparkEngineContext(jsc);
  }

  private Configuration getHadoopConfiguration() {
    Configuration hadoopConf = jsc.hadoopConfiguration();

    // Add S3 configurations if provided
    if (cfg.awsAccessKey != null && cfg.awsSecretKey != null) {
      hadoopConf.set("fs.s3a.access.key", cfg.awsAccessKey);
      hadoopConf.set("fs.s3a.secret.key", cfg.awsSecretKey);

      if (cfg.awsSessionToken != null) {
        hadoopConf.set("fs.s3a.session.token", cfg.awsSessionToken);
      }

      if (cfg.s3Endpoint != null) {
        hadoopConf.set("fs.s3a.endpoint", cfg.s3Endpoint);
      }

      hadoopConf.set("fs.s3a.endpoint.region", cfg.awsRegion);
      hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    }

    return hadoopConf;
  }

  private HoodieWriteConfig getWriteConfig() {
    return HoodieWriteConfig.newBuilder()
        .withPath(cfg.basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
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
                .parquetMaxFileSize(1024 * 1024 * 1024)  // 1GB
                .build())
        .withMemoryConfig(
            HoodieMemoryConfig.newBuilder()
                .withMaxMemoryForMerge(cfg.maxMemoryForCompaction * 1024 * 1024)  // Convert MB to bytes
                .withMaxMemoryForCompaction(cfg.maxMemoryForCompaction * 1024 * 1024)
                .build())
        .forTable(HoodieTableMetadataUtil.METADATA_TABLE_NAME_WITH_UNDERSCORE)
        .withIndexConfig(
            HoodieIndexConfig.newBuilder()
                .withIndexType(HoodieIndex.IndexType.BLOOM)
                .build())
        .withEmbeddedTimelineServerEnabled(false)
        .build();
  }

  private void initializeTable() throws IOException {
    Configuration hadoopConf = getHadoopConfiguration();
    HoodieStorage storage = HoodieStorage.newInstance(new HadoopStorageConfiguration(hadoopConf), true);

    // Check if table exists, if not create a minimal metadata table structure
    if (!HoodieTableMetaClient.isMetaTableInitialized(new StoragePath(cfg.basePath))) {
      // Initialize as metadata table
      HoodieTableMetaClient.withPropertyBuilder()
          .setTableType(HoodieTableType.MERGE_ON_READ)
          .setTableName(HoodieTableMetadataUtil.METADATA_TABLE_NAME_WITH_UNDERSCORE)
          .setPayloadClassName("org.apache.hudi.common.model.OverwriteWithLatestAvroPayload")
          .initTable(hadoopConf, cfg.basePath);
    }

    this.metaClient = HoodieTableMetaClient.builder()
        .setConf(hadoopConf)
        .setBasePath(cfg.basePath)
        .build();

    HoodieWriteConfig writeConfig = getWriteConfig();
    this.hoodieTable = HoodieSparkTable.create(writeConfig, engineContext, metaClient);
  }

  private CompactionOperation buildCompactionOperation() {
    CompactionOperation.Builder builder = CompactionOperation.newBuilder()
        .withPartitionPath(cfg.partitionPath)
        .withFileId(cfg.fileId)
        .withInstantTime(HoodieTimeline.createNewInstantTime());

    // Add base file if provided
    if (cfg.baseFile != null && !cfg.baseFile.isEmpty()) {
      HoodieBaseFile baseFile = new HoodieBaseFile(new StoragePath(cfg.basePath, cfg.baseFile).toString());
      builder.withBaseFile(baseFile);
    }

    // Add log files if provided
    if (cfg.logFiles != null && !cfg.logFiles.isEmpty()) {
      List<String> logFilesList = Arrays.stream(cfg.logFiles.split(","))
          .map(String::trim)
          .filter(s -> !s.isEmpty())
          .collect(Collectors.toList());

      builder.withLogFiles(logFilesList.stream()
          .map(logFile -> new HoodieLogFile(new StoragePath(cfg.basePath, logFile)))
          .collect(Collectors.toList()));
    }

    // Add metrics
    Map<String, Double> metrics = new HashMap<>();
    metrics.put("TOTAL_LOG_FILE_SIZE", 100000.0);  // Placeholder metric
    builder.withMetrics(metrics);

    return builder.build();
  }

  private long runSingleIteration(CompactionOperation compactionOperation, boolean warmup) throws IOException {
    String instantTime = HoodieTimeline.createNewInstantTime();
    HoodieWriteConfig writeConfig = getWriteConfig();

    // Create a task context supplier
    TaskContextSupplier taskContextSupplier = new TaskContextSupplier() {
      @Override
      public int getPartitionId() { return 0; }
      @Override
      public int getStageId() { return 0; }
      @Override
      public long getAttemptId() { return 0; }
    };

    // Create reader context
    HoodieReaderContext<Object> readerContext = engineContext.getHoodieReaderContext("test-reader",
        HoodieRecord.HoodieRecordType.SPARK);

    long startTime = System.nanoTime();

    try (FileGroupReaderBasedMergeHandle mergeHandle = new FileGroupReaderBasedMergeHandle(
        writeConfig,
        instantTime,
        hoodieTable,
        compactionOperation,
        taskContextSupplier,
        readerContext,
        instantTime,
        HoodieRecord.HoodieRecordType.SPARK)) {

      // Perform the merge
      mergeHandle.doMerge();

      // Get write status
      mergeHandle.close();
    }

    long endTime = System.nanoTime();
    long duration = endTime - startTime;

    if (!warmup) {
      LOG.info("Iteration completed in {} ms", TimeUnit.NANOSECONDS.toMillis(duration));
    } else {
      LOG.info("Warmup iteration completed in {} ms", TimeUnit.NANOSECONDS.toMillis(duration));
    }

    return duration;
  }

  public void run() throws IOException {
    LOG.info("Starting FileGroupReaderBasedMergeHandle Benchmark");
    LOG.info("Configuration: {}", cfg);

    // Initialize Spark and table
    initializeSpark();
    initializeTable();

    // Build compaction operation
    CompactionOperation compactionOperation = buildCompactionOperation();
    LOG.info("Compaction operation: partitionPath={}, fileId={}, baseFile={}, logFiles={}",
        cfg.partitionPath, cfg.fileId, cfg.baseFile, cfg.logFiles);

    // Run warmup iterations
    LOG.info("Running {} warmup iterations...", cfg.warmupIterations);
    for (int i = 0; i < cfg.warmupIterations; i++) {
      runSingleIteration(compactionOperation, true);
    }

    // Run benchmark iterations
    LOG.info("Running {} benchmark iterations...", cfg.numIterations);
    Histogram histogram = new Histogram(new UniformReservoir());
    List<Long> durations = new ArrayList<>();

    for (int i = 0; i < cfg.numIterations; i++) {
      LOG.info("Starting iteration {}/{}", i + 1, cfg.numIterations);
      long duration = runSingleIteration(compactionOperation, false);
      durations.add(duration);
      histogram.update(duration);
    }

    // Print statistics
    printStatistics(durations, histogram);

    // Cleanup
    jsc.stop();
  }

  private void printStatistics(List<Long> durations, Histogram histogram) {
    LOG.info("========================================");
    LOG.info("Benchmark Results");
    LOG.info("========================================");
    LOG.info("Number of iterations: {}", cfg.numIterations);
    LOG.info("Base path: {}", cfg.basePath);
    LOG.info("Partition path: {}", cfg.partitionPath);
    LOG.info("File ID: {}", cfg.fileId);
    LOG.info("Base file: {}", cfg.baseFile);
    LOG.info("Log files: {}", cfg.logFiles);
    LOG.info("----------------------------------------");

    // Convert to milliseconds for better readability
    List<Long> durationsMs = durations.stream()
        .map(d -> TimeUnit.NANOSECONDS.toMillis(d))
        .collect(Collectors.toList());

    long min = durationsMs.stream().min(Long::compare).orElse(0L);
    long max = durationsMs.stream().max(Long::compare).orElse(0L);
    double avg = durationsMs.stream().mapToLong(Long::longValue).average().orElse(0);

    LOG.info("Min duration: {} ms", min);
    LOG.info("Max duration: {} ms", max);
    LOG.info("Avg duration: {:.2f} ms", avg);

    // Calculate percentiles
    durationsMs.sort(Long::compare);
    int p50Index = (int) (durationsMs.size() * 0.50);
    int p75Index = (int) (durationsMs.size() * 0.75);
    int p90Index = (int) (durationsMs.size() * 0.90);
    int p95Index = (int) (durationsMs.size() * 0.95);
    int p99Index = (int) (durationsMs.size() * 0.99);

    LOG.info("P50 (median): {} ms", durationsMs.get(p50Index));
    LOG.info("P75: {} ms", durationsMs.get(p75Index));
    LOG.info("P90: {} ms", durationsMs.get(p90Index));
    LOG.info("P95: {} ms", durationsMs.get(p95Index));
    LOG.info("P99: {} ms", durationsMs.get(p99Index));
    LOG.info("========================================");
  }

  public static void main(String[] args) throws IOException {
    Config cfg = new Config();
    JCommander commander = JCommander.newBuilder()
        .addObject(cfg)
        .build();

    try {
      commander.parse(args);
    } catch (Exception e) {
      LOG.error("Error parsing arguments", e);
      commander.usage();
      System.exit(1);
    }

    if (cfg.help) {
      commander.usage();
      System.exit(0);
    }

    // Set default output path if not provided
    if (cfg.outputPath == null) {
      cfg.outputPath = cfg.basePath + "/benchmark-output";
    }

    FileGroupReaderBasedMergeHandleBenchmark benchmark = new FileGroupReaderBasedMergeHandleBenchmark(cfg);

    try {
      benchmark.run();
    } catch (Exception e) {
      LOG.error("Benchmark failed", e);
      throw e;
    }
  }
}