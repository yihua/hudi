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

package org.apache.hudi.common.engine;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.hadoop.fs.FileSystem;

import java.util.HashMap;
import java.util.Map;

public abstract class HoodieReaderContext<T> {
  // These are only used in memory for merging and should not be persisted to storage
  public static final String INTERNAL_META_RECORD_KEY = "_0";
  public static final String INTERNAL_META_PARTITION_PATH = "_1";
  public static final String INTERNAL_META_ORDERING_FIELD = "_2";
  public static final String INTERNAL_META_OPERATION = "_3";
  public static final String INTERNAL_META_INSTANT_TIME = "_4";

  public abstract FileSystem getFs(String tablePath);

  public abstract ClosableIterator<T> getFileRecordIterator(
      String baseFilePath);

  public abstract HoodieRecordMerger getRecordMerger(String mergeStrategy);

  public abstract String getStringValue(T record, int ord);

  public abstract String getRecordKey(T record);

  public abstract Comparable getOrderingValue(Pair<Option<T>, Map<String, Object>> recordInfo, TypedProperties props);

  public abstract HoodieRecord<T> constructHoodieRecord(Pair<Option<T>, Map<String, Object>> record);

  public abstract T copy(T record);

  public Map<String, Object> generateMetaForDelete(
      String recordKey, String partitionPath, Comparable orderingVal) {
    Map<String, Object> meta = new HashMap<>();
    meta.put(INTERNAL_META_RECORD_KEY, recordKey);
    meta.put(INTERNAL_META_PARTITION_PATH, partitionPath);
    meta.put(INTERNAL_META_ORDERING_FIELD, orderingVal);
    return meta;
  }

  public Map<String, Object> generateMetaForUpdate(
      String recordKey, String partitionPath, Comparable orderingVal) {
    Map<String, Object> meta = new HashMap<>();
    meta.put(INTERNAL_META_RECORD_KEY, recordKey);
    meta.put(INTERNAL_META_PARTITION_PATH, partitionPath);
    meta.put(INTERNAL_META_ORDERING_FIELD, orderingVal);
    return meta;
  }
}
