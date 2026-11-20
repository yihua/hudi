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

package org.apache.hudi.common;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.BaseFileRowLineage;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.TimelineFactory;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.TableMetadataFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

/**
 * External Table Format needs to implement this interface
 */
public interface HoodieTableFormat extends Serializable {

  /**
   * Returns the name of the table format.
   */
  String getName();

  /**
   * Initializes the table format implementation with the properties supplied from {@link org.apache.hudi.common.table.HoodieTableConfig}
   */
  default void init(Properties properties) {
  }

  /**
   * Called just after marking the write action as complete in hoodie timeline. Implementation expected to save additional state needed in
   * extraMetadata.
   *
   * @param commitMetadata HoodieCommitMetadata for commit or clustering action.
   * @param completedInstant completed instant in hoodie timeline
   * @param engineContext engine context used for execution - local,spark or flink etc.
   * @param metaClient metaClient from HoodieTable.
   * @param viewManager viewManager from HoodieTable.
   */

  default void commit(
      HoodieCommitMetadata commitMetadata,
      HoodieInstant completedInstant,
      HoodieEngineContext engineContext,
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager) {
  }

  /**
   * Called after marking the clean action as complete in hoodie timeline.
   *
   * @param cleanMetadata HoodieCleanMetadata for clean action.
   * @param completedInstant completed instant in hoodie timeline
   * @param engineContext engine context used for execution - local,spark or flink etc.
   * @param metaClient metaClient from HoodieTable.
   * @param viewManager viewManager from HoodieTable.
   */
  default void clean(
      HoodieCleanMetadata cleanMetadata,
      HoodieInstant completedInstant,
      HoodieEngineContext engineContext,
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager) {
  }

  /**
   * Called after archiving the instants in hoodie timeline.
   *
   * @param archivedInstants List of instants archived in hoodie timeline
   * @param engineContext engine context used for execution - local,spark or flink etc.
   * @param metaClient metaClient from HoodieTable.
   * @param viewManager  viewManager from HoodieTable.
   */
  default void archive(
      Supplier<List<HoodieInstant>> archivedInstants,
      HoodieEngineContext engineContext,
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager) {
  }

  /**
   * Called before rolling back the instant in hoodie timeline.
   *
   * @param instantToRollback instant to be rolled back in hoodie timeline.
   * @param engineContext engine context used for execution - local,spark or flink etc.
   * @param metaClient metaClient from HoodieTable.
   * @param viewManager viewManager from HoodieTable.
   */
  default void rollback(
      HoodieInstant instantToRollback,
      HoodieEngineContext engineContext,
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager) {
  }

  /**
   * Called after marking a rollback action as complete in hoodie timeline.
   *
   * @param rollbackInstant The completed rollback instant in hoodie timeline.
   * @param engineContext engine context used for execution - local,spark or flink etc.
   * @param metaClient  metaClient from HoodieTable.
   * @param viewManager viewManager from HoodieTable.
   */
  default void completedRollback(
      HoodieInstant rollbackInstant,
      HoodieEngineContext engineContext,
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager) {
  }

  /**
   * Called after marking a complete write action as "savepoint" in the hoodie timeline.
   *
   * @param savepointInstant The completed instant to be marked as savepoint.
   * @param engineContext engine context used for execution - local,spark or flink etc.
   * @param metaClient metaClient from HoodieTable.
   * @param viewManager viewManager from HoodieTable.
   */
  default void savepoint(
      HoodieInstant savepointInstant,
      HoodieEngineContext engineContext,
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager) {
  }

  /**
   * Called after marking a "restore" action as complete in the hoodie timeline.
   *
   * @param restoreCompletedInstant The completed restore instant in hoodie timeline.
   * @param engineContext           engine context used for execution - local,spark or flink etc.
   * @param metaClient              metaClient from HoodieTable.
   * @param viewManager             viewManager from HoodieTable.
   */

  default void restore(
      HoodieInstant restoreCompletedInstant,
      HoodieEngineContext engineContext,
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager) {
  }

  /**
   * Returns the row lineage columns this table format declares for base files, or an empty list, the default,
   * when the table format does not track row lineage.
   *
   * <p>A table format that tracks row lineage gives every row a row ID and the sequence number of the commit
   * that last updated the row. For a row that stores neither in its base file, the table format derives the
   * row ID as the first row ID of the base file plus the position of the row in the file, and the sequence
   * number as the one of the base file, see {@link #getBaseFileRowLineage}. A row that is carried over
   * unchanged into a new base file keeps its row lineage only if the new base file stores it. A rewrite that
   * preserves row lineage therefore adds these columns to the base file it writes, after the columns of the
   * table, fills them for the rows it carries over unchanged, and leaves them null for new and updated rows,
   * which get a new row lineage. The columns are not columns of the table.
   *
   * @return an empty list, or exactly two nullable {@code long} fields that default to null: the row ID column
   * followed by the last updated sequence number column.
   */
  default List<HoodieSchemaField> getRowLineageColumns() {
    return Collections.emptyList();
  }

  /**
   * Returns the values this table format assigned to the given base file, from which the row lineage of the rows
   * in the file is derived, or an empty option if the table format has none for the base file. A rewrite that
   * preserves row lineage calls this for the base file it rewrites, possibly from a distributed task, and fails
   * on an empty option, as the rows it carries over would get a new row lineage.
   *
   * @param metaClient    metaClient of the table.
   * @param partitionPath partition path of the base file.
   * @param baseFile      the base file being rewritten.
   */
  default Option<BaseFileRowLineage> getBaseFileRowLineage(
      HoodieTableMetaClient metaClient,
      String partitionPath,
      HoodieBaseFile baseFile) {
    return Option.empty();
  }

  /**
   * Return the timeline factory for table format.
   */
  TimelineFactory getTimelineFactory();

  /**
   * Return the table metadata factory for table format.
   */
  TableMetadataFactory getMetadataFactory();
}
