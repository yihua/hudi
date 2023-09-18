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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieTableQueryType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordReader;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.EmptyIterator;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE;
import static org.apache.hudi.common.config.HoodieReaderConfig.COMPACTION_LAZY_BLOCK_READ_ENABLE;
import static org.apache.hudi.common.fs.FSUtils.getRelativePartitionPath;
import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getIntWithAltKeys;

public final class HoodieFileGroupReader<T> {
  private final HoodieReaderContext<T> readerContext;
  private final Option<String> baseFilePath;
  private final Option<List<String>> logFilePathList;
  private final TypedProperties props;
  private final long start;
  private final long length;
  private ClosableIterator<T> baseFileIterator;
  // This is only initialized and used after all records from the base file are iterated
  private Iterator<Pair<Option<T>, Map<String, Object>>> logRecordIterator;
  private HoodieRecordMerger recordMerger;
  // key to record mapping from log files
  private Map<String, Pair<Option<T>, Map<String, Object>>> logFileRecordMapping;
  private FileGroupReaderState readerState;

  T nextRecord;

  public HoodieFileGroupReader(HoodieReaderContext<T> readerContext,
                               HoodieTableMetaClient metaClient,
                               String fileGroupId,
                               TypedProperties props,
                               HoodieTimeline timeline,
                               HoodieTableQueryType queryType,
                               Option<String> instantTime,
                               Option<String> startInstantTime) {
    // Derive base and log files and call the corresponding constructor
    this.readerContext = readerContext;
    this.baseFilePath = Option.empty();
    this.logFilePathList = Option.empty();
    this.props = props;
    this.start = 0;
    this.length = Long.MAX_VALUE;
    this.baseFileIterator = new EmptyIterator<>();
  }

  public HoodieFileGroupReader(HoodieReaderContext<T> readerContext,
                               Option<String> baseFilePath,
                               Option<List<String>> logFilePathList,
                               TypedProperties props,
                               long start,
                               long length) {
    this.readerContext = readerContext;
    this.baseFilePath = baseFilePath;
    this.logFilePathList = logFilePathList;
    this.props = props;
    this.start = start;
    this.length = length;
    this.recordMerger = readerContext.getRecordMerger(readerState.recordMergeStrategy);
  }

  public void initRecordIterators() {
    this.baseFileIterator = baseFilePath.isPresent()
        ? readerContext.getFileRecordIterator(baseFilePath.get())
        : new EmptyIterator<>();
    scanLogFiles();
  }

  public boolean hasNext() {
    while (baseFileIterator.hasNext()) {
      T baseRecord = baseFileIterator.next();
      String recordKey = readerContext.getStringValue(baseRecord, readerState.recordKeyOrdInBase);
      Pair<Option<T>, Map<String, Object>> logRecordInfo = logFileRecordMapping.remove(recordKey);
      merge(Option.of(baseRecord),
          logRecordInfo != null ? logRecordInfo.getLeft() : Option.empty());
      Option<T> resultRecord = logRecordInfo != null
          ? merge(Option.of(baseRecord), logRecordInfo.getLeft()) : Option.of(baseRecord);
      if (resultRecord.isPresent()) {
        nextRecord = resultRecord.get();
        return true;
      }
    }

    if (logRecordIterator == null) {
      logRecordIterator = logFileRecordMapping.values().iterator();
    }

    while (logRecordIterator.hasNext()) {
      Pair<Option<T>, Map<String, Object>> nextRecordInfo = logRecordIterator.next();
      Option<T> resultRecord = merge(Option.empty(), nextRecordInfo.getLeft());
      if (resultRecord.isPresent()) {
        nextRecord = resultRecord.get();
        return true;
      }
    }

    return false;
  }

  public T next() {
    T result = nextRecord;
    nextRecord = null;
    return result;
  }

  private void scanLogFiles() {
    if (logFilePathList.isPresent()) {
      // TODO(yihua): remove Hadoop APIs
      FileSystem fs = readerContext.getFs(readerState.tablePath);
      // TODO(yihua): extend to different merge modes; support full schema on read
      HoodieMergedLogRecordReader<T> logRecordReader = HoodieMergedLogRecordReader.newBuilder()
          .withHoodieReaderContext(readerContext)
          .withFileSystem(fs)
          .withBasePath(readerState.tablePath)
          .withLogFilePaths(logFilePathList.get())
          .withLatestInstantTime(readerState.latestCommitTime)
          .withReadBlocksLazily(getBooleanWithAltKeys(props, COMPACTION_LAZY_BLOCK_READ_ENABLE))
          .withReverseReader(false)
          .withBufferSize(getIntWithAltKeys(props, MAX_DFS_STREAM_BUFFER_SIZE))
          .withPartition(getRelativePartitionPath(
              new Path(readerState.tablePath), new Path(logFilePathList.get().get(0)).getParent()
          ))
          .withRecordMerger(recordMerger)
          .build();
      logFileRecordMapping = logRecordReader.getRecords();
    } else {
      logFileRecordMapping = new HashMap<>();
    }
  }

  private Option<T> merge(Option<T> older, Option<T> newer) {
    // TODO(yihua): integrate with merger API
    return recordMerger.merge(older, oldSchema, newer, newSchema);
  }
}
