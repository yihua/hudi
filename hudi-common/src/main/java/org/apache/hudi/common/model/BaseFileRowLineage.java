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

package org.apache.hudi.common.model;

import org.apache.hudi.common.HoodieTableFormat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.HoodieException;

import lombok.Value;

import java.io.Serializable;

/**
 * The values an external table format assigned to a base file, from which it derives the row lineage of
 * every row in the file that stores none, see
 * {@link org.apache.hudi.common.HoodieTableFormat#getRowLineageColumns()}.
 */
@Value
public class BaseFileRowLineage implements Serializable {

  // Row ID of the first row in the base file; the row at position p of the file has the row ID firstRowId + p
  long firstRowId;
  // Sequence number the table format assigned to the base file
  long sequenceNumber;

  /**
   * Returns the values the table format of the table assigned to the base file, see
   * {@link HoodieTableFormat#getBaseFileRowLineage}, and fails if it has none, as the records a rewrite carries
   * over from the base file would get a new row lineage.
   */
  public static BaseFileRowLineage getRequired(HoodieTableMetaClient metaClient, String partitionPath, HoodieBaseFile baseFile) {
    HoodieTableFormat tableFormat = metaClient.getTableFormat();
    return tableFormat.getBaseFileRowLineage(metaClient, partitionPath, baseFile).orElseThrow(() -> new HoodieException(
        "Table format " + tableFormat.getName() + " declares row lineage columns but has no row lineage for base file "
            + baseFile.getPath() + ", so the records carried over from it cannot keep their row lineage"));
  }
}
