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

package org.apache.hudi;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.catalyst.InternalRow;

import java.util.Map;

public class HoodieSparkReaderContext extends HoodieReaderContext<InternalRow> {
  @Override
  public FileSystem getFs(String tablePath) {
    return null;
  }

  @Override
  public ClosableIterator<InternalRow> getFileRecordIterator(String baseFilePath) {
    return null;
  }

  @Override
  public HoodieRecordMerger getRecordMerger(String mergeStrategy) {
    return null;
  }

  @Override
  public String getStringValue(InternalRow record, int ord) {
    return null;
  }

  @Override
  public String getRecordKey(InternalRow record) {
    return null;
  }

  @Override
  public Comparable getOrderingValue(Pair<Option<InternalRow>, Map<String, Object>> recordInfo, TypedProperties props) {
    return null;
  }

  @Override
  public HoodieRecord<InternalRow> constructHoodieRecord(Pair<Option<InternalRow>, Map<String, Object>> record) {
    return null;
  }
}
