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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieData;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.spark.Partitioner;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;

public class SparkInsertOverwriteCommitHelper<T extends HoodieRecordPayload<T>> extends SparkCommitHelper<T> {
  public SparkInsertOverwriteCommitHelper(
      HoodieEngineContext context, HoodieWriteConfig config, HoodieTable table,
      String instantTime, WriteOperationType operationType) {
    super(context, config, table, instantTime, operationType);
  }

  @Override
  protected Partitioner getPartitioner(WorkloadProfile profile) {
    return new SparkInsertOverwritePartitioner(profile, context, table, config);
  }

  @Override
  protected String getCommitActionType() {
    return HoodieTimeline.REPLACE_COMMIT_ACTION;
  }

  @Override
  public Map<String, List<String>> getPartitionToReplacedFileIds(
      HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata) {
    return getRdd(writeMetadata.getWriteStatuses()).map(
        status -> status.getStat().getPartitionPath()).distinct().mapToPair(partitionPath ->
        new Tuple2<>(partitionPath, getAllExistingFileIds(partitionPath))).collectAsMap();
  }

  protected List<String> getAllExistingFileIds(String partitionPath) {
    // because new commit is not complete. it is safe to mark all existing file Ids as old files
    return table.getSliceView().getLatestFileSlices(partitionPath).map(
        fg -> fg.getFileId()).distinct().collect(Collectors.toList());
  }
}
