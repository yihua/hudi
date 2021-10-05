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

package org.apache.hudi.table.action.compact;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.AbstractHoodieWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;

import java.util.List;

/**
 * Compacts a hoodie table with merge on read storage. Computes all possible compactions,
 * passes it through a CompactionFilter and executes all the compactions and writes a new version of base files and make
 * a normal commit.
 *
 * <p>Note: the compaction logic is invoked through the flink pipeline.
 */
@SuppressWarnings("checkstyle:LineLength")
public class HoodieFlinkMergeOnReadTableCompactor<T extends HoodieRecordPayload>
    extends HoodieCompactor<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> {

  @Override
  public Schema getReaderSchema(HoodieWriteConfig config) {
    return HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()), config.allowOperationMetadataField());
  }

  @Override
  public void updateReaderSchema(HoodieWriteConfig config, HoodieTableMetaClient metaClient) {
    // No OP
  }

  @Override
  public void handleCompactionTimeline(
      HoodieTable table, HoodieTimeline pendingCompactionTimeline,
      String compactionInstantTime, AbstractHoodieWriteClient writeClient) {
    HoodieInstant inflightInstant = HoodieTimeline.getCompactionInflightInstant(compactionInstantTime);
    if (pendingCompactionTimeline.containsInstant(inflightInstant)) {
      writeClient.rollbackInflightCompaction(inflightInstant, table);
      table.getMetaClient().reloadActiveTimeline();
    }
  }
}
