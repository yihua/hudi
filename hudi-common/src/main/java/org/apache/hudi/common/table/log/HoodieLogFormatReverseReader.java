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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.InternalSchema;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Hoodie log format reader by reading log files and blocks in reserve order.
 */
public class HoodieLogFormatReverseReader implements HoodieLogFormat.Reader {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieLogFormatReader.class);
  private final List<HoodieLogFile> logFiles;
  // Readers for previously scanned log-files that are still open
  private final List<HoodieLogFileReader> prevReadersInOpenState;
  private HoodieLogFileReader currentReader;
  private final FileSystem fs;
  private final Schema readerSchema;
  private InternalSchema internalSchema = InternalSchema.getEmptyInternalSchema();
  private final boolean readBlocksLazily;
  private final boolean reverseLogReader;
  private final String recordKeyField;
  private final boolean enableInlineReading;
  private int bufferSize;
  private int logFilePos = -1;

  HoodieLogFormatReverseReader(FileSystem fs, List<HoodieLogFile> logFiles, Schema readerSchema, boolean readBlocksLazily,
                               boolean reverseLogReader, int bufferSize, boolean enableRecordLookups,
                               String recordKeyField, InternalSchema internalSchema) throws IOException {
    this.logFiles = logFiles;
    this.fs = fs;
    this.readerSchema = readerSchema;
    this.readBlocksLazily = readBlocksLazily;
    this.reverseLogReader = reverseLogReader;
    this.bufferSize = bufferSize;
    this.prevReadersInOpenState = new ArrayList<>();
    this.recordKeyField = recordKeyField;
    this.enableInlineReading = enableRecordLookups;
    this.internalSchema = internalSchema == null ? InternalSchema.getEmptyInternalSchema() : internalSchema;
    if (logFiles.size() > 0) {
      HoodieLogFile nextLogFile = logFiles.remove(0);
      this.currentReader = new HoodieLogFileReader(fs, nextLogFile, readerSchema, bufferSize, readBlocksLazily, false,
          enableRecordLookups, recordKeyField, internalSchema);
    }
    logFilePos = logFiles.size() - 1;
    if (logFilePos >= 0) {
      HoodieLogFile nextLogFile = logFiles.get(logFilePos);
      logFilePos--;
      this.currentReader = new HoodieLogFileReader(fs, nextLogFile, readerSchema, bufferSize, readBlocksLazily, false,
          enableRecordLookups, recordKeyField, internalSchema);
    }
  }

  @Override
  /**
   * Note : In lazy mode, clients must ensure close() should be called only after processing all log-blocks as the
   * underlying inputstream will be closed. TODO: We can introduce invalidate() API at HoodieLogBlock and this object
   * can call invalidate on all returned log-blocks so that we check this scenario specifically in HoodieLogBlock
   */
  public void close() throws IOException {

    for (HoodieLogFileReader reader : prevReadersInOpenState) {
      reader.close();
    }

    prevReadersInOpenState.clear();

    if (currentReader != null) {
      currentReader.close();
    }
  }

  @Override
  public boolean hasNext() {

    if (currentReader == null) {
      return false;
    } else if (currentReader.hasNext()) {
      return true;
    } else if (logFilePos >= 0) {
      try {
        HoodieLogFile nextLogFile = logFiles.get(logFilePos);
        logFilePos--;
        // First close previous reader only if readBlockLazily is false
        if (!readBlocksLazily) {
          this.currentReader.close();
        } else {
          this.prevReadersInOpenState.add(currentReader);
        }
        // TODO(yihua): here we assume only one log block per log file. Otherwise, we have to
        //              reverse the order of log blocks from the same log file, which is not handled yet
        this.currentReader = new HoodieLogFileReader(fs, nextLogFile, readerSchema, bufferSize, readBlocksLazily, false,
            enableInlineReading, recordKeyField, internalSchema);
      } catch (IOException io) {
        throw new HoodieIOException("unable to initialize read with log file ", io);
      }
      LOG.info("Moving to the next reader for logfile " + currentReader.getLogFile());
      return hasNext();
    }
    return false;
  }

  @Override
  public HoodieLogBlock next() {
    return currentReader.next();
  }

  @Override
  public HoodieLogFile getLogFile() {
    return currentReader.getLogFile();
  }

  @Override
  public void remove() {
  }

  @Override
  public boolean hasPrev() {
    return this.currentReader.hasPrev();
  }

  @Override
  public HoodieLogBlock prev() throws IOException {
    return this.currentReader.prev();
  }
}
