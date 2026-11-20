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

package org.apache.hudi.client.functional;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.avro.HoodieAvroReaderContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.tableformat.TestTableFormat;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.common.testutils.RowLineageTestUtils.NO_ROW_LINEAGE;
import static org.apache.hudi.common.testutils.RowLineageTestUtils.assertNewRowLineage;
import static org.apache.hudi.common.testutils.RowLineageTestUtils.assertRowLineageChangesWithCommitSeqNo;
import static org.apache.hudi.common.testutils.RowLineageTestUtils.deriveRowLineage;
import static org.apache.hudi.common.testutils.RowLineageTestUtils.hasRowLineageColumns;
import static org.apache.hudi.common.testutils.RowLineageTestUtils.readParquetSchema;
import static org.apache.hudi.common.testutils.RowLineageTestUtils.readStoredRowLineage;
import static org.apache.hudi.tableformat.TestTableFormat.LAST_UPDATED_SEQUENCE_NUMBER_COLUMN;
import static org.apache.hudi.tableformat.TestTableFormat.ROW_ID_COLUMN;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Functional tests for the row lineage columns of an external table format, see
 * {@link org.apache.hudi.common.HoodieTableFormat#getRowLineageColumns()}: a record that a rewrite carries over
 * unchanged into a new base file keeps its row lineage, and a new or updated record gets a new one.
 *
 * <p>The tests read the row lineage columns straight from the parquet files, and derive the row lineage of a
 * record that stores none the way the table format does, from the values {@link TestTableFormat} assigned to the
 * base file and the position of the record in the file.
 */
public class TestHoodieJavaClientRowLineageForTestFormat extends HoodieJavaClientTestHarness {

  private static final String PARTITION = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;

  @Override
  protected void initMetaClient() {
    // Each test creates its table through initTable
  }

  @AfterAll
  public static void tearDownAll() throws IOException {
    TestTableFormat.tearDown();
    FileSystem.closeAll();
  }

  private void initTable(HoodieTableType tableType, HoodieTableVersion tableVersion, boolean rowLineageEnabled) throws IOException {
    storageConf.set(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "false");
    storageConf.set(HoodieTableConfig.TABLE_FORMAT.key(), "test-format");
    storageConf.set(HoodieMetadataConfig.ENABLE.key(), "false");
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.TABLE_FORMAT.key(), "test-format");
    properties.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), String.valueOf(tableVersion.versionCode()));
    metaClient = HoodieTestUtils.getMetaClientBuilder(tableType, properties, null)
        .set(Collections.singletonMap(TestTableFormat.ROW_LINEAGE_ENABLED, String.valueOf(rowLineageEnabled)))
        .initTable(storageConf.newInstance(), basePath);
    assertEquals(tableVersion, metaClient.getTableConfig().getTableVersion());
    assertEquals(rowLineageEnabled ? 2 : 0, metaClient.getTableFormat().getRowLineageColumns().size());
  }

  private HoodieWriteConfig buildConfig(HoodieTableVersion tableVersion) {
    return getConfigBuilder(TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.SIMPLE)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .compactionSmallFileSize(1024 * 1024).withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withWriteTableVersion(tableVersion.versionCode())
        .withAutoUpgradeVersion(false)
        .build();
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableVersion.class, names = {"NINE", "TEN"})
  void testCopyOnWriteUpsertKeepsRowLineageOfUnchangedRecords(HoodieTableVersion tableVersion) throws Exception {
    initTable(HoodieTableType.COPY_ON_WRITE, tableVersion, true);
    HoodieWriteConfig config = buildConfig(tableVersion);
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    List<HoodieRecord> inserts = dataGen.generateInsertsForPartition("001", 20, PARTITION);
    write(client, inserts, false);
    HoodieBaseFile baseFile1 = getOnlyLatestBaseFile();
    // A base file with new records only has no row lineage columns, so the table format derives the row lineage
    assertFalse(hasRowLineageColumns(storageConf, baseFile1));
    Map<String, Pair<Long, Long>> rowLineage1 = deriveRowLineage(storageConf, basePath, baseFile1);
    assertEquals(keys(inserts), rowLineage1.keySet());
    assertEquals(20, new HashSet<>(rowLineage1.values()).size());

    // The first rewrite updates and deletes records in the middle of the base file, and adds new records
    List<HoodieRecord> updates1 = Arrays.asList(inserts.get(1), inserts.get(7), inserts.get(13));
    List<HoodieRecord> deletes1 = Arrays.asList(inserts.get(4), inserts.get(10));
    List<HoodieRecord> inserts1 = dataGen.generateInsertsForPartition("002", 4, PARTITION);
    write(client, concat(dataGen.generateUpdates("002", updates1), dataGen.generateDeletesFromExistingRecords(deletes1), inserts1), true);
    HoodieBaseFile baseFile2 = getOnlyLatestBaseFile();
    assertEquals(baseFile1.getFileId(), baseFile2.getFileId());
    assertNotEquals(baseFile1.getFileName(), baseFile2.getFileName());

    Map<String, Pair<Long, Long>> expectedStored2 = new LinkedHashMap<>(rowLineage1);
    expectedStored2.keySet().removeAll(keys(deletes1));
    keys(updates1).forEach(key -> expectedStored2.put(key, NO_ROW_LINEAGE));
    keys(inserts1).forEach(key -> expectedStored2.put(key, NO_ROW_LINEAGE));
    assertEquals(expectedStored2, readStoredRowLineage(storageConf, baseFile2));
    Map<String, Pair<Long, Long>> rowLineage2 = deriveRowLineage(storageConf, basePath, baseFile2);
    assertNewRowLineage(basePath, baseFile2, rowLineage2, union(keys(updates1), keys(inserts1)), rowLineage1.values());
    assertRowLineageChangesWithCommitSeqNo(storageConf, basePath, baseFile1, baseFile2, 15, 3);

    // The second rewrite of the file group carries over records that store their row lineage in the base file
    // and records that do not: the ones the first rewrite updated and inserted
    List<HoodieRecord> updates2 = Arrays.asList(inserts.get(0), inserts.get(1), inserts1.get(0));
    List<HoodieRecord> deletes2 = Arrays.asList(inserts.get(2));
    List<HoodieRecord> inserts2 = dataGen.generateInsertsForPartition("003", 2, PARTITION);
    write(client, concat(dataGen.generateUpdates("003", updates2), dataGen.generateDeletesFromExistingRecords(deletes2), inserts2), true);
    HoodieBaseFile baseFile3 = getOnlyLatestBaseFile();
    assertEquals(baseFile1.getFileId(), baseFile3.getFileId());

    Map<String, Pair<Long, Long>> expectedStored3 = new LinkedHashMap<>(rowLineage2);
    expectedStored3.keySet().removeAll(keys(deletes2));
    keys(updates2).forEach(key -> expectedStored3.put(key, NO_ROW_LINEAGE));
    keys(inserts2).forEach(key -> expectedStored3.put(key, NO_ROW_LINEAGE));
    Map<String, Pair<Long, Long>> stored3 = readStoredRowLineage(storageConf, baseFile3);
    assertEquals(expectedStored3, stored3);
    // A record no write has changed still has the row lineage it got when it was inserted
    String unchangedKey = inserts.get(19).getRecordKey();
    assertEquals(rowLineage1.get(unchangedKey), stored3.get(unchangedKey));
    // A record the first rewrite updated has the row lineage derived from the second base file
    String updatedOnceKey = inserts.get(7).getRecordKey();
    assertEquals(rowLineage2.get(updatedOnceKey), stored3.get(updatedOnceKey));
    assertNotEquals(rowLineage1.get(updatedOnceKey), stored3.get(updatedOnceKey));
    assertNewRowLineage(basePath, baseFile3, deriveRowLineage(storageConf, basePath, baseFile3), union(keys(updates2), keys(inserts2)), rowLineage2.values());
    assertRowLineageChangesWithCommitSeqNo(storageConf, basePath, baseFile2, baseFile3, 18, 3);

    assertRowLineageColumnsNotInTable(23);
  }

  @Test
  void testInsertIntoSmallFileKeepsRowLineageOfExistingRecords() throws Exception {
    initTable(HoodieTableType.COPY_ON_WRITE, HoodieTableVersion.current(), true);
    HoodieWriteConfig config = buildConfig(HoodieTableVersion.current());
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    List<HoodieRecord> inserts = dataGen.generateInsertsForPartition("001", 10, PARTITION);
    write(client, inserts, false);
    HoodieBaseFile baseFile1 = getOnlyLatestBaseFile();
    Map<String, Pair<Long, Long>> rowLineage1 = deriveRowLineage(storageConf, basePath, baseFile1);

    // An upsert with new records only, which are routed into the existing small base file
    List<HoodieRecord> newInserts = dataGen.generateInsertsForPartition("002", 5, PARTITION);
    write(client, newInserts, true);
    HoodieBaseFile baseFile2 = getOnlyLatestBaseFile();
    assertEquals(baseFile1.getFileId(), baseFile2.getFileId());
    assertNotEquals(baseFile1.getFileName(), baseFile2.getFileName());

    Map<String, Pair<Long, Long>> expectedStored = new LinkedHashMap<>(rowLineage1);
    keys(newInserts).forEach(key -> expectedStored.put(key, NO_ROW_LINEAGE));
    assertEquals(expectedStored, readStoredRowLineage(storageConf, baseFile2));
    assertNewRowLineage(basePath, baseFile2, deriveRowLineage(storageConf, basePath, baseFile2), keys(newInserts), rowLineage1.values());
    assertRowLineageChangesWithCommitSeqNo(storageConf, basePath, baseFile1, baseFile2, 10, 0);
  }

  /**
   * With the partial update mode, the merge builds an updated record out of the record in the log and the
   * record in the base file, which has a row lineage.
   */
  @ParameterizedTest
  @CsvSource({"NINE,", "TEN,", "TEN,IGNORE_DEFAULTS"})
  void testCompactionKeepsRowLineageOfUnchangedRecords(HoodieTableVersion tableVersion, PartialUpdateMode partialUpdateMode) throws Exception {
    initTable(HoodieTableType.MERGE_ON_READ, tableVersion, true);
    if (partialUpdateMode != null) {
      Properties partialUpdateProperties = new Properties();
      partialUpdateProperties.setProperty(HoodieTableConfig.PARTIAL_UPDATE_MODE.key(), partialUpdateMode.name());
      HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(), partialUpdateProperties);
      metaClient = HoodieTestUtils.createMetaClient(storageConf, basePath);
      assertEquals(Option.of(partialUpdateMode), metaClient.getTableConfig().getPartialUpdateMode());
    }
    HoodieWriteConfig config = buildConfig(tableVersion);
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    List<HoodieRecord> inserts = dataGen.generateInsertsForPartition("001", 20, PARTITION);
    write(client, inserts, false);
    HoodieBaseFile baseFile1 = getOnlyLatestBaseFile();
    assertFalse(hasRowLineageColumns(storageConf, baseFile1));
    Map<String, Pair<Long, Long>> rowLineage1 = deriveRowLineage(storageConf, basePath, baseFile1);
    assertEquals(keys(inserts), rowLineage1.keySet());

    // The updates and deletes go to the log files of the file group and compaction merges them into a new base file
    List<HoodieRecord> updates1 = Arrays.asList(inserts.get(1), inserts.get(7), inserts.get(13));
    List<HoodieRecord> deletes1 = Arrays.asList(inserts.get(4), inserts.get(10));
    write(client, concat(dataGen.generateUpdates("002", updates1), dataGen.generateDeletesFromExistingRecords(deletes1)), true);
    assertEquals(baseFile1.getFileName(), getOnlyLatestBaseFile().getFileName());
    compact(client);
    HoodieBaseFile baseFile2 = getOnlyLatestBaseFile();
    assertEquals(baseFile1.getFileId(), baseFile2.getFileId());
    assertNotEquals(baseFile1.getFileName(), baseFile2.getFileName());

    Map<String, Pair<Long, Long>> expectedStored2 = new LinkedHashMap<>(rowLineage1);
    expectedStored2.keySet().removeAll(keys(deletes1));
    keys(updates1).forEach(key -> expectedStored2.put(key, NO_ROW_LINEAGE));
    assertEquals(expectedStored2, readStoredRowLineage(storageConf, baseFile2));
    Map<String, Pair<Long, Long>> rowLineage2 = deriveRowLineage(storageConf, basePath, baseFile2);
    assertNewRowLineage(basePath, baseFile2, rowLineage2, keys(updates1), rowLineage1.values());
    assertRowLineageChangesWithCommitSeqNo(storageConf, basePath, baseFile1, baseFile2, 15, 3);

    // The second compaction carries over records that store their row lineage in the base file and records
    // that do not: the ones the first compaction merged an update into
    List<HoodieRecord> updates2 = Arrays.asList(inserts.get(0), inserts.get(1));
    List<HoodieRecord> deletes2 = Arrays.asList(inserts.get(2));
    write(client, concat(dataGen.generateUpdates("003", updates2), dataGen.generateDeletesFromExistingRecords(deletes2)), true);
    compact(client);
    HoodieBaseFile baseFile3 = getOnlyLatestBaseFile();
    assertNotEquals(baseFile2.getFileName(), baseFile3.getFileName());

    Map<String, Pair<Long, Long>> expectedStored3 = new LinkedHashMap<>(rowLineage2);
    expectedStored3.keySet().removeAll(keys(deletes2));
    keys(updates2).forEach(key -> expectedStored3.put(key, NO_ROW_LINEAGE));
    Map<String, Pair<Long, Long>> stored3 = readStoredRowLineage(storageConf, baseFile3);
    assertEquals(expectedStored3, stored3);
    String updatedOnceKey = inserts.get(7).getRecordKey();
    assertEquals(rowLineage2.get(updatedOnceKey), stored3.get(updatedOnceKey));
    assertNotEquals(rowLineage1.get(updatedOnceKey), stored3.get(updatedOnceKey));
    assertNewRowLineage(basePath, baseFile3, deriveRowLineage(storageConf, basePath, baseFile3), keys(updates2), rowLineage2.values());
    assertRowLineageChangesWithCommitSeqNo(storageConf, basePath, baseFile2, baseFile3, 15, 2);

    assertRowLineageColumnsNotInTable(17);
  }

  @Test
  void testRewriteFailsWithoutRowLineageOfBaseFile() throws Exception {
    initTable(HoodieTableType.COPY_ON_WRITE, HoodieTableVersion.current(), true);
    HoodieWriteConfig config = buildConfig(HoodieTableVersion.current());
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    List<HoodieRecord> inserts = dataGen.generateInsertsForPartition("001", 10, PARTITION);
    write(client, inserts, false);
    HoodieBaseFile baseFile1 = getOnlyLatestBaseFile();

    TestTableFormat.forgetBaseFileRowLineage(basePath);
    String instantTime = WriteClientTestUtils.createNewInstantTime();
    WriteClientTestUtils.startCommitWithTime(client, instantTime);
    Throwable failure = assertThrows(Exception.class,
        () -> client.upsert(dataGen.generateUpdates("002", inserts.subList(0, 2)), instantTime));
    String expectedMessage = "Table format test-format declares row lineage columns but has no row lineage for base file "
        + baseFile1.getPath();
    assertTrue(exceptionChainContains(failure, expectedMessage), () -> "Unexpected failure: " + failure);
    assertEquals(1, metaClient.reloadActiveTimeline().getCommitsTimeline().filterCompletedInstants().countInstants());
  }

  @Test
  void testRewrittenBaseFileIsUnchangedWithoutRowLineageColumns() throws Exception {
    initTable(HoodieTableType.COPY_ON_WRITE, HoodieTableVersion.current(), false);
    HoodieWriteConfig config = buildConfig(HoodieTableVersion.current());
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    List<HoodieRecord> inserts = dataGen.generateInsertsForPartition("001", 10, PARTITION);
    write(client, inserts, false);
    HoodieBaseFile baseFile1 = getOnlyLatestBaseFile();
    write(client, concat(dataGen.generateUpdates("002", inserts.subList(0, 3)), dataGen.generateInsertsForPartition("002", 2, PARTITION)), true);
    HoodieBaseFile baseFile2 = getOnlyLatestBaseFile();
    assertEquals(baseFile1.getFileId(), baseFile2.getFileId());
    assertNotEquals(baseFile1.getFileName(), baseFile2.getFileName());

    List<String> tableColumns = HoodieSchemaUtils.addMetadataFields(HoodieSchema.parse(TRIP_EXAMPLE_SCHEMA)).getFields().stream()
        .map(HoodieSchemaField::name).collect(Collectors.toList());
    MessageType rewrittenFileSchema = readParquetSchema(storageConf, baseFile2);
    assertEquals(tableColumns, rewrittenFileSchema.getFields().stream().map(Type::getName).collect(Collectors.toList()));
    assertEquals(readParquetSchema(storageConf, baseFile1), rewrittenFileSchema);
  }

  private void write(HoodieJavaWriteClient client, List<HoodieRecord> records, boolean upsert) {
    String instantTime = WriteClientTestUtils.createNewInstantTime();
    WriteClientTestUtils.startCommitWithTime(client, instantTime);
    List<WriteStatus> statuses = upsert ? client.upsert(records, instantTime) : client.insert(records, instantTime);
    assertNoWriteErrors(statuses);
    assertTrue(client.commit(instantTime, statuses));
  }

  private void compact(HoodieJavaWriteClient client) {
    Option<String> compactionTime = client.scheduleCompaction(Option.empty());
    assertTrue(compactionTime.isPresent());
    HoodieWriteMetadata writeMetadata = client.compact(compactionTime.get());
    client.commitCompaction(compactionTime.get(), writeMetadata, Option.empty());
  }

  @SafeVarargs
  private static <E> List<E> concat(List<? extends E>... lists) {
    List<E> result = new ArrayList<>();
    Arrays.stream(lists).forEach(result::addAll);
    return result;
  }

  private static Set<String> keys(List<HoodieRecord> records) {
    return records.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toSet());
  }

  private static Set<String> union(Set<String> first, Set<String> second) {
    Set<String> result = new HashSet<>(first);
    result.addAll(second);
    return result;
  }

  private HoodieBaseFile getOnlyLatestBaseFile() throws IOException {
    try (SyncableFileSystemView fsView = getFileSystemView(metaClient.reloadActiveTimeline())) {
      List<FileSlice> fileSlices = Arrays.stream(dataGen.getPartitionPaths())
          .flatMap(fsView::getLatestFileSlices).collect(Collectors.toList());
      assertEquals(1, fileSlices.size(), "The records of every write are expected in one file group");
      assertEquals(PARTITION, fileSlices.get(0).getPartitionPath());
      return fileSlices.get(0).getBaseFile().get();
    }
  }

  /**
   * Asserts that the row lineage columns of the rewritten base files are not columns of the table: the table
   * schema, the schema in the commit metadata and the records a reader returns do not have them.
   */
  private void assertRowLineageColumnsNotInTable(int expectedRecords) throws Exception {
    assertTrue(hasRowLineageColumns(storageConf, getOnlyLatestBaseFile()));
    metaClient.reloadActiveTimeline();
    TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
    for (HoodieSchema schema : Arrays.asList(schemaResolver.getTableSchema(), schemaResolver.getTableSchemaFromDataFile())) {
      assertTrue(schema.getField(HoodieRecord.RECORD_KEY_METADATA_FIELD).isPresent());
      assertFalse(schema.getField(ROW_ID_COLUMN).isPresent(), schema::toString);
      assertFalse(schema.getField(LAST_UPDATED_SEQUENCE_NUMBER_COLUMN).isPresent(), schema::toString);
    }
    List<HoodieInstant> commits = metaClient.getCommitsTimeline().filterCompletedInstants().getInstants();
    assertFalse(commits.isEmpty());
    for (HoodieInstant commit : commits) {
      String schema = metaClient.getActiveTimeline().readCommitMetadata(commit).getMetadata(HoodieCommitMetadata.SCHEMA_KEY);
      assertFalse(schema.contains(ROW_ID_COLUMN) || schema.contains(LAST_UPDATED_SEQUENCE_NUMBER_COLUMN), schema);
    }
    HoodieSchema tableSchema = schemaResolver.getTableSchema();
    HoodieBaseFile baseFile = getOnlyLatestBaseFile();
    int records = 0;
    try (HoodieFileGroupReader<IndexedRecord> reader = HoodieFileGroupReader.<IndexedRecord>builder()
        .withReaderContext(new HoodieAvroReaderContext(storageConf, metaClient.getTableConfig(), Option.empty(), Option.empty()))
        .withHoodieTableMetaClient(metaClient).withLatestCommitTime(commits.get(commits.size() - 1).requestedTime())
        .withPartitionPath(PARTITION).withBaseFileOption(Option.of(baseFile))
        .withDataSchema(tableSchema).withRequestedSchema(tableSchema).withProps(new TypedProperties()).build();
         ClosableIterator<IndexedRecord> iterator = reader.getClosableIterator()) {
      while (iterator.hasNext()) {
        IndexedRecord record = iterator.next();
        assertEquals(tableSchema.getFields().size(), record.getSchema().getFields().size());
        assertNull(record.getSchema().getField(ROW_ID_COLUMN));
        assertNull(record.getSchema().getField(LAST_UPDATED_SEQUENCE_NUMBER_COLUMN));
        records++;
      }
    }
    assertEquals(expectedRecords, records);
  }

  private static boolean exceptionChainContains(Throwable throwable, String text) {
    for (Throwable current = throwable; current != null; current = current.getCause()) {
      if (current.getMessage() != null && current.getMessage().contains(text)) {
        return true;
      }
    }
    return false;
  }
}
