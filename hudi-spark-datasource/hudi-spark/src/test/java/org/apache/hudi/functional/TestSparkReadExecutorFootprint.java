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

package org.apache.hudi.functional;

import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.testutils.SparkExecutorGuards;
import org.apache.hudi.testutils.TaskDeserializationRecorder;

import com.github.benmanes.caffeine.cache.Cache;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards what Spark tasks do on the executors when reading a Hudi table through the file group
 * reader: they must not access the table's {@code .hoodie} folder, and they must not deserialize
 * heavy driver-side objects (meta client, timeline, Hadoop configuration, the file format itself)
 * with every task closure. Both costs scale with the number of tasks, not with the data.
 *
 * <p>Each table is written once per class and read by every case that needs it. The table has
 * several partitions so that a read runs several tasks; on MERGE_ON_READ the second commit
 * updates half of the keys so that every file group has log files to merge.
 *
 * <p>This is the subset of the guards that covers the fixes in this branch: snapshot reads with
 * and without the metadata table on read, schema-on-read snapshot reads, and the per-task
 * deserialization footprint of snapshot reads.
 */
@Slf4j
@Tag("functional")
class TestSparkReadExecutorFootprint extends SparkClientFunctionalTestHarness {

  private static final int CURRENT_VERSION = HoodieTableVersion.current().versionCode();
  private static final int NUM_RECORDS = 200;
  private static final int NUM_UPDATED_RECORDS = 100;
  private static final int NUM_PARTITIONS = 4;

  /**
   * Budget for the task binary, the Java-serialized closure every task deserializes.
   */
  private static final long MAX_TASK_BINARY_BYTES = 12 * 1024;
  private static final String TASK_BINARY_BUDGET_BASIS =
      "the budget is about 1.5x the 7664 to 8081 bytes measured for these reads with the scan state broadcast";

  /**
   * Driver-side classes that a read task must not deserialize with its closure.
   */
  private static final List<String> CLASSES_NOT_DESERIALIZED_PER_TASK = Arrays.asList(
      "org.apache.hudi.common.table.HoodieTableMetaClient",
      "org.apache.hudi.common.table.timeline.HoodieActiveTimeline",
      "org.apache.hudi.storage.StorageConfiguration",
      "org.apache.spark.util.SerializableConfiguration",
      "org.apache.spark.sql.execution.datasources.parquet.HoodieFileGroupReaderBasedFileFormat",
      "org.apache.hudi.config.HoodieWriteConfig");

  private static final StructType SCHEMA = DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField("key", DataTypes.StringType, false),
      DataTypes.createStructField("part", DataTypes.StringType, false),
      DataTypes.createStructField("ts", DataTypes.LongType, false),
      DataTypes.createStructField("value", DataTypes.StringType, true)});

  private static final StructType EVOLVED_SCHEMA = SCHEMA.add(
      DataTypes.createStructField("extra", DataTypes.StringType, true));

  private static final Map<String, TestTable> TABLES = new HashMap<>();

  @TempDir
  static Path tablesDir;

  enum TableKind {
    PLAIN, SCHEMA_ON_READ
  }

  @Override
  public SparkConf conf() {
    return conf(Collections.singletonMap("spark.plugins", SparkExecutorGuards.TASK_START_HOOK_PLUGIN));
  }

  @BeforeEach
  void enableRecording() {
    SparkExecutorGuards.enableMetaFolderAccessRecording(jsc().hadoopConfiguration());
  }

  @AfterEach
  void disableRecording() {
    SparkExecutorGuards.disableMetaFolderAccessRecording(jsc().hadoopConfiguration());
  }

  @AfterAll
  static void forgetTables() {
    TABLES.clear();
  }

  static Stream<Arguments> tableVersionsAndTypes() {
    return Stream.of(6, CURRENT_VERSION).flatMap(version ->
        Stream.of(COPY_ON_WRITE, MERGE_ON_READ).map(type -> Arguments.of(version, type)));
  }

  static Stream<Arguments> snapshotReads() {
    List<Arguments> args = new ArrayList<>();
    for (int version : new int[] {6, CURRENT_VERSION}) {
      for (HoodieTableType type : HoodieTableType.values()) {
        for (String metadataOnRead : new String[] {"default", "false"}) {
          args.add(Arguments.of(version, type, metadataOnRead));
        }
      }
    }
    return args.stream();
  }

  static Stream<Arguments> readsForDeserialization() {
    return Stream.of(
        Arguments.of(CURRENT_VERSION, COPY_ON_WRITE),
        Arguments.of(CURRENT_VERSION, MERGE_ON_READ),
        Arguments.of(6, MERGE_ON_READ));
  }

  @ParameterizedTest(name = "[{index}] version={0}, type={1}, metadata={2}")
  @MethodSource("snapshotReads")
  void testNoExecutorMetaFolderAccess(int tableVersion, HoodieTableType tableType, String metadataOnRead) {
    TestTable table = getOrWriteTable(tableVersion, tableType, TableKind.PLAIN);
    Map<String, String> options = new HashMap<>();
    if (!"default".equals(metadataOnRead)) {
      options.put(HoodieMetadataConfig.ENABLE.key(), metadataOnRead);
    }
    List<Row> rows = SparkExecutorGuards.assertNoExecutorMetaFolderAccess(
        table.name + " snapshot read with metadata " + metadataOnRead,
        () -> read(table, options).collectAsList());
    assertFalse(rows.isEmpty(), "The read must return rows for the guard to be meaningful");
  }

  /**
   * With schema on read, every base and log file needs the internal schema of the commit that wrote
   * it. The schema history has to reach the tasks from the driver rather than be looked up in
   * {@code .hoodie} by each task.
   */
  @ParameterizedTest(name = "[{index}] version={0}, type={1}")
  @MethodSource("tableVersionsAndTypes")
  void testNoExecutorMetaFolderAccessWithSchemaOnRead(int tableVersion, HoodieTableType tableType) {
    TestTable table = getOrWriteTable(tableVersion, tableType, TableKind.SCHEMA_ON_READ);
    Map<String, String> options = new HashMap<>();
    options.put(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key(), "true");
    AtomicInteger tasksStarted = new AtomicInteger();
    SparkExecutorGuards.setTaskStartHook(() -> {
      tasksStarted.incrementAndGet();
      clearHistoricalSchemaCache();
    });
    List<Row> rows;
    try {
      rows = SparkExecutorGuards.assertNoExecutorMetaFolderAccess(
          table.name + " schema-on-read snapshot read",
          () -> read(table, options).collectAsList());
    } finally {
      SparkExecutorGuards.setTaskStartHook(() -> { });
    }
    assertTrue(tasksStarted.get() > 0, "The task start hook must run so that every task starts with an empty"
        + " schema history cache, as it does on an executor that the driver does not share a JVM with");
    assertEquals(NUM_RECORDS, rows.size());
  }

  /**
   * Tasks must not deserialize the meta client, timeline, Hadoop configuration, write config or the
   * file format with their closure, and the task binary must stay within budget.
   */
  @ParameterizedTest(name = "[{index}] version={0}, type={1}")
  @MethodSource("readsForDeserialization")
  void testTaskDeserializationFootprint(int tableVersion, HoodieTableType tableType) {
    TestTable table = getOrWriteTable(tableVersion, tableType, TableKind.PLAIN);
    Dataset<Row> df = read(table, new HashMap<>());
    // Plan and list files on the driver first, so that the recorded window holds only the scan.
    df.queryExecution().executedPlan().execute();
    List<Row> rows = new ArrayList<>();
    TaskDeserializationRecorder.Result result = SparkExecutorGuards.recordTaskDeserialization(
        spark().sparkContext(), () -> rows.addAll(df.collectAsList()));
    assertFalse(rows.isEmpty(), "The read must return rows for the guard to be meaningful");
    SparkExecutorGuards.TaskBinary taskBinary = SparkExecutorGuards.inspectTaskBinary(df);
    log.info("Snapshot read of {}: task binary {} bytes, largest task stream seen {} bytes, stages kept {}, ignored {}",
        table.name, taskBinary.getBytes(), result.getMaxStreamBytes(), result.getKeptScopes(), result.getIgnoredScopes());
    SparkExecutorGuards.assertTaskDeserializationFootprint(
        table.name + " snapshot read (" + TASK_BINARY_BUDGET_BASIS + ")", result, taskBinary,
        CLASSES_NOT_DESERIALIZED_PER_TASK, MAX_TASK_BINARY_BYTES);
  }

  private Dataset<Row> read(TestTable table, Map<String, String> options) {
    return spark().read().format("hudi").options(options)
        .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL())
        .load(table.basePath);
  }

  private TestTable getOrWriteTable(int tableVersion, HoodieTableType tableType, TableKind kind) {
    String name = kind.name().toLowerCase() + "_" + tableType.name().toLowerCase() + "_v" + tableVersion;
    return TABLES.computeIfAbsent(name, n -> writeTable(n, tableVersion, tableType, kind));
  }

  private TestTable writeTable(String name, int tableVersion, HoodieTableType tableType, TableKind kind) {
    String basePath = tablesDir.resolve(name).toUri().toString();
    Map<String, String> options = new HashMap<>();
    options.put(HoodieTableConfig.NAME.key(), name);
    options.put(DataSourceWriteOptions.TABLE_TYPE().key(), tableType.name());
    options.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "key");
    options.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "part");
    options.put(DataSourceWriteOptions.ORDERING_FIELDS().key(), "ts");
    options.put(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), String.valueOf(tableVersion));
    options.put(HoodieWriteConfig.AUTO_UPGRADE_VERSION.key(), "false");
    options.put("hoodie.insert.shuffle.parallelism", "2");
    options.put("hoodie.upsert.shuffle.parallelism", "2");
    if (kind == TableKind.SCHEMA_ON_READ) {
      // Reconciling makes the writer record an internal schema in the commit metadata.
      options.put(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key(), "true");
      options.put(DataSourceWriteOptions.RECONCILE_SCHEMA().key(), "true");
    }

    List<Row> inserts = IntStream.range(0, NUM_RECORDS)
        .mapToObj(i -> RowFactory.create(key(i), "p" + (i % NUM_PARTITIONS), 1L, "v1"))
        .collect(Collectors.toList());
    write(inserts, SCHEMA, options, basePath);
    // The second commit updates half of the keys; with schema on read it also adds a column, so
    // that the files of the two commits carry different schema versions.
    if (kind == TableKind.SCHEMA_ON_READ) {
      List<Row> updates = IntStream.range(0, NUM_UPDATED_RECORDS)
          .mapToObj(i -> RowFactory.create(key(i), "p" + (i % NUM_PARTITIONS), 2L, "v2", "e2"))
          .collect(Collectors.toList());
      write(updates, EVOLVED_SCHEMA, options, basePath);
    } else {
      List<Row> updates = IntStream.range(0, NUM_UPDATED_RECORDS)
          .mapToObj(i -> RowFactory.create(key(i), "p" + (i % NUM_PARTITIONS), 2L, "v2"))
          .collect(Collectors.toList());
      write(updates, SCHEMA, options, basePath);
    }

    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf()).build();
    assertEquals(tableVersion, metaClient.getTableConfig().getTableVersion().versionCode());
    List<HoodieInstant> commits = metaClient.getCommitsTimeline().filterCompletedInstants().getInstants();
    assertEquals(2, commits.size(), "Expected two completed commits in " + name);
    if (kind == TableKind.SCHEMA_ON_READ) {
      assertTrue(new TableSchemaResolver(metaClient).getTableInternalSchemaFromCommitMetadata().isPresent(),
          "The schema-on-read table " + name + " should carry an internal schema");
    }
    if (tableType == MERGE_ON_READ) {
      assertTrue(countLogFiles(tablesDir.resolve(name)) >= NUM_PARTITIONS,
          "Every file group of " + name + " should have log files to merge");
    }
    return new TestTable(name, basePath);
  }

  private void write(List<Row> rows, StructType schema, Map<String, String> options, String basePath) {
    spark().createDataset(rows, SparkAdapterSupport$.MODULE$.sparkAdapter().getCatalystExpressionUtils().getEncoder(schema))
        .write()
        .format("hudi")
        .options(options)
        .mode(SaveMode.Append)
        .save(basePath);
  }

  private static long countLogFiles(Path tableDir) {
    try (Stream<Path> files = Files.walk(tableDir)) {
      return files.filter(p -> !p.toString().contains("/.hoodie/") && p.getFileName().toString().contains(".log.")).count();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Empties the JVM-wide cache of historical internal schemas. In local mode the driver and the
   * executors share it, which would hide executor-side schema history reads.
   */
  private static void clearHistoricalSchemaCache() {
    try {
      Field field = InternalSchemaCache.class.getDeclaredField("HISTORICAL_SCHEMA_CACHE");
      field.setAccessible(true);
      ((Cache<?, ?>) field.get(null)).invalidateAll();
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Cannot clear the historical schema cache", e);
    }
  }

  private static String key(int i) {
    return String.format("key%03d", i);
  }

  private static final class TestTable {
    private final String name;
    private final String basePath;

    private TestTable(String name, String basePath) {
      this.name = name;
      this.basePath = basePath;
    }
  }
}
