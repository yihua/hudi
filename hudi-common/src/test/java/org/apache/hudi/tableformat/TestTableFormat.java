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

package org.apache.hudi.tableformat;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.HoodieTableFormat;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BaseFileRowLineage;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.TimelineFactory;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.TableMetadataFactory;
import org.apache.hudi.storage.StoragePath;

import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

/**
 * Test implementation of HoodieTableFormat that records all Hoodie instants in memory.
 * Used for functional testing of HoodieTableFormat.
 *
 * <p>With {@link #ROW_LINEAGE_ENABLED} set in the table config, the table format also tracks row lineage: when
 * a commit completes, it assigns the next sequence number to the commit and to every base file the commit
 * added, and the next range of row IDs, one per record, to each of these base files.
 */
@NoArgsConstructor
public class TestTableFormat implements HoodieTableFormat {

  public static final String ROW_LINEAGE_ENABLED = HoodieTableConfig.TABLE_FORMAT.key() + ".test.row.lineage.enabled";
  public static final String ROW_ID_COLUMN = "_test_row_id";
  public static final String LAST_UPDATED_SEQUENCE_NUMBER_COLUMN = "_test_last_updated_sequence_number";

  private static final Map<String, List<HoodieInstant>> RECORDED_INSTANTS = new ConcurrentHashMap<>();
  private static final Map<String, Properties> INIT_PROPERTIES = new ConcurrentHashMap<>();
  private static final Map<String, TableRowLineage> ROW_LINEAGE = new ConcurrentHashMap<>();

  private boolean rowLineageEnabled;

  public static List<HoodieInstant> getRecordedInstants(String basePath) {
    return RECORDED_INSTANTS.getOrDefault(basePath, Collections.emptyList());
  }

  /**
   * Returns the properties of the latest {@link #init} call for the table with the given name.
   */
  public static Properties getInitProperties(String tableName) {
    return INIT_PROPERTIES.get(tableName);
  }

  /**
   * Returns the row lineage values assigned to a base file of the table, if a completed commit added the file.
   */
  public static Option<BaseFileRowLineage> getBaseFileRowLineage(String basePath, String baseFileName) {
    TableRowLineage tableRowLineage = ROW_LINEAGE.get(basePath);
    return tableRowLineage == null ? Option.empty() : Option.ofNullable(tableRowLineage.baseFiles.get(baseFileName));
  }

  /**
   * Makes the table format lose the row lineage values it assigned to the base files of the table.
   */
  public static void forgetBaseFileRowLineage(String basePath) {
    TableRowLineage tableRowLineage = ROW_LINEAGE.get(basePath);
    if (tableRowLineage != null) {
      tableRowLineage.baseFiles.clear();
    }
  }

  public static void tearDown() {
    RECORDED_INSTANTS.clear();
    INIT_PROPERTIES.clear();
    ROW_LINEAGE.clear();
  }

  @Override
  public String getName() {
    return "test-format";
  }

  @Override
  public void init(Properties properties) {
    Properties copy = new Properties();
    copy.putAll(properties);
    INIT_PROPERTIES.put(properties.getProperty(HoodieTableConfig.NAME.key(), ""), copy);
    rowLineageEnabled = Boolean.parseBoolean(properties.getProperty(ROW_LINEAGE_ENABLED, "false"));
  }

  @Override
  public List<HoodieSchemaField> getRowLineageColumns() {
    if (!rowLineageEnabled) {
      return Collections.emptyList();
    }
    HoodieSchema nullableLong = HoodieSchema.createNullable(HoodieSchemaType.LONG);
    return Arrays.asList(
        HoodieSchemaField.of(ROW_ID_COLUMN, nullableLong, null, HoodieSchema.NULL_VALUE),
        HoodieSchemaField.of(LAST_UPDATED_SEQUENCE_NUMBER_COLUMN, nullableLong, null, HoodieSchema.NULL_VALUE));
  }

  @Override
  public Option<BaseFileRowLineage> getBaseFileRowLineage(HoodieTableMetaClient metaClient, String partitionPath, HoodieBaseFile baseFile) {
    return getBaseFileRowLineage(metaClient.getBasePath().toString(), baseFile.getFileName());
  }

  @Override
  public void commit(HoodieCommitMetadata commitMetadata, HoodieInstant completedInstant, 
                    HoodieEngineContext engineContext, HoodieTableMetaClient metaClient, 
                    FileSystemViewManager viewManager) {
    RECORDED_INSTANTS.putIfAbsent(metaClient.getBasePath().toString(), new CopyOnWriteArrayList<>());
    RECORDED_INSTANTS.get(metaClient.getBasePath().toString()).add(completedInstant);
    if (rowLineageEnabled) {
      ROW_LINEAGE.computeIfAbsent(metaClient.getBasePath().toString(), basePath -> new TableRowLineage()).assign(commitMetadata);
    }
  }

  @Override
  public void clean(HoodieCleanMetadata cleanMetadata, HoodieInstant completedInstant, 
                   HoodieEngineContext engineContext, HoodieTableMetaClient metaClient, 
                   FileSystemViewManager viewManager) {
    RECORDED_INSTANTS.get(metaClient.getBasePath().toString()).add(completedInstant);
  }

  @Override
  public void archive(Supplier<List<HoodieInstant>> archivedInstants, HoodieEngineContext engineContext,
                      HoodieTableMetaClient metaClient, FileSystemViewManager viewManager) {
    RECORDED_INSTANTS.get(metaClient.getBasePath().toString()).removeAll(archivedInstants.get());
  }

  @Override
  public void rollback(HoodieInstant instantToRollback, HoodieEngineContext engineContext,
                      HoodieTableMetaClient metaClient, FileSystemViewManager viewManager) {
    RECORDED_INSTANTS.get(metaClient.getBasePath().toString()).remove(instantToRollback);
  }

  @Override
  public void completedRollback(HoodieInstant rollbackInstant, HoodieEngineContext engineContext, 
                               HoodieTableMetaClient metaClient, FileSystemViewManager viewManager) {
    RECORDED_INSTANTS.putIfAbsent(metaClient.getBasePath().toString(), new CopyOnWriteArrayList<>());
    RECORDED_INSTANTS.get(metaClient.getBasePath().toString()).add(rollbackInstant);
  }

  @Override
  public void savepoint(HoodieInstant savepointInstant, HoodieEngineContext engineContext, 
                       HoodieTableMetaClient metaClient, FileSystemViewManager viewManager) {
    RECORDED_INSTANTS.get(metaClient.getBasePath().toString()).add(savepointInstant);
  }

  @Override
  public void restore(HoodieInstant restoreCompletedInstant, HoodieEngineContext engineContext, HoodieTableMetaClient metaClient, FileSystemViewManager viewManager) {
    RECORDED_INSTANTS.get(metaClient.getBasePath().toString()).add(restoreCompletedInstant);
  }

  @Override
  public TimelineFactory getTimelineFactory() {
    return new TestTimelineFactory(null);
  }

  @Override
  public TableMetadataFactory getMetadataFactory() {
    return TestTableMetadataFactory.getInstance();
  }

  /**
   * The row lineage state of a table: the next row ID and sequence number to assign, and the values assigned to
   * each base file by name.
   */
  private static class TableRowLineage {
    private final Map<String, BaseFileRowLineage> baseFiles = new ConcurrentHashMap<>();
    private long nextRowId = 0;
    private long lastSequenceNumber = 0;

    private synchronized void assign(HoodieCommitMetadata commitMetadata) {
      long sequenceNumber = ++lastSequenceNumber;
      commitMetadata.getWriteStats().stream()
          .filter(writeStat -> FSUtils.isBaseFile(new StoragePath(writeStat.getPath())))
          .forEach(writeStat -> {
            baseFiles.put(new StoragePath(writeStat.getPath()).getName(), new BaseFileRowLineage(nextRowId, sequenceNumber));
            nextRowId += writeStat.getNumWrites();
          });
    }
  }
}
