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

package org.apache.hudi.io;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.HoodieWriteStat.RuntimeStats;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.model.MetadataValues;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.read.HoodieFileGroupReader.HoodieFileGroupReaderIterator;
import org.apache.hudi.common.table.read.HoodieFileGroupReaderStats;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCorruptedDataException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS;

@SuppressWarnings("Duplicates")
/**
 * Handle to merge incoming records to those in storage.
 * <p>
 * Simplified Logic:
 * For every existing record
 *     Check if there is a new record coming in. If yes, merge two records and write to file
 *     else write the record as is
 * For all pending records from incoming batch, write to file.
 *
 * Illustration with simple data.
 * Incoming data:
 *     rec1_2, rec4_2, rec5_1, rec6_1
 * Existing data:
 *     rec1_1, rec2_1, rec3_1, rec4_1
 *
 * For every existing record, merge w/ incoming if required and write to storage.
 *    => rec1_1 and rec1_2 is merged to write rec1_2 to storage
 *    => rec2_1 is written as is
 *    => rec3_1 is written as is
 *    => rec4_2 and rec4_1 is merged to write rec4_2 to storage
 * Write all pending records from incoming set to storage
 *    => rec5_1 and rec6_1
 *
 * Final snapshot in storage
 * rec1_2, rec2_1, rec3_1, rec4_2, rec5_1, rec6_1
 *
 * </p>
 */
@NotThreadSafe
public class HoodieSparkMergeHandleV2<T, I, K, O> extends HoodieWriteHandle<T, I, K, O> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieMergeHandle.class);

  protected Set<String> writtenRecordKeys;
  protected HoodieReaderContext readerContext;
  protected HoodieFileWriter fileWriter;
  protected boolean preserveMetadata = false;

  protected StoragePath newFilePath;
  protected StoragePath oldFilePath;
  protected long recordsWritten = 0;
  // TODO(yihua): audit delete stats because file group reader may not return deletes
  protected long recordsDeleted = 0;
  protected long updatedRecordsWritten = 0;
  protected long insertRecordsWritten = 0;
  protected Option<BaseKeyGenerator> keyGeneratorOpt;
  protected FileSlice fileSlice;
  private HoodieBaseFile baseFileToMerge;

  protected Option<String[]> partitionFields = Option.empty();
  protected Object[] partitionValues = new Object[0];
  protected Configuration conf;

  /**
   * Called by compactor code path using the file group reader.
   */
  public HoodieSparkMergeHandleV2(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                  CompactionOperation operation, TaskContextSupplier taskContextSupplier, Option<BaseKeyGenerator> keyGeneratorOpt,
                                  HoodieReaderContext readerContext, Configuration conf) {
    super(config, instantTime, operation.getPartitionPath(), operation.getFileId(), hoodieTable, taskContextSupplier);
    this.readerContext = readerContext;
    this.conf = conf;
    Option<HoodieBaseFile> baseFileOpt =
        operation.getBaseFile(config.getBasePath(), operation.getPartitionPath());
    List<HoodieLogFile> logFiles = operation.getDeltaFileNames().stream().map(p ->
            new HoodieLogFile(new StoragePath(FSUtils.constructAbsolutePath(
                config.getBasePath(), operation.getPartitionPath()), p)))
        .collect(Collectors.toList());
    this.fileSlice = new FileSlice(
        operation.getFileGroupId(),
        operation.getBaseInstantTime(),
        baseFileOpt.isPresent() ? baseFileOpt.get() : null,
        logFiles);
    ValidationUtils.checkArgument(baseFileOpt.isPresent(),
        "Only supporting compaction with base file using file group reader in HoodieMergeHandleV2");
    this.preserveMetadata = true;
    init(fileId, this.partitionPath, baseFileOpt.get());
    validateAndSetAndKeyGenProps(keyGeneratorOpt, config.populateMetaFields());
  }

  private void validateAndSetAndKeyGenProps(Option<BaseKeyGenerator> keyGeneratorOpt, boolean populateMetaFields) {
    ValidationUtils.checkArgument(populateMetaFields == !keyGeneratorOpt.isPresent());
    this.keyGeneratorOpt = keyGeneratorOpt;
  }

  /**
   * Extract old file path, initialize StorageWriter and WriteStatus.
   */
  private void init(String fileId, String partitionPath, HoodieBaseFile baseFileToMerge) {
    LOG.info("partitionPath:" + partitionPath + ", fileId to be merged:" + fileId);
    this.baseFileToMerge = baseFileToMerge;
    this.writtenRecordKeys = new HashSet<>();
    writeStatus.setStat(new HoodieWriteStat());
    try {
      String latestValidFilePath = baseFileToMerge.getFileName();
      writeStatus.getStat().setPrevCommit(baseFileToMerge.getCommitTime());
      // At the moment, we only support SI for overwrite with latest payload. So, we don't need to embed entire file slice here.
      // HUDI-8518 will be taken up to fix it for any payload during which we might require entire file slice to be set here.
      // Already AppendHandle adds all logs file from current file slice to HoodieDeltaWriteStat.
      writeStatus.getStat().setPrevBaseFile(latestValidFilePath);

      HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(storage, instantTime,
          new StoragePath(config.getBasePath()),
          FSUtils.constructAbsolutePath(config.getBasePath(), partitionPath),
          hoodieTable.getPartitionMetafileFormat());
      partitionMetadata.trySave();

      String newFileName = FSUtils.makeBaseFileName(instantTime, writeToken, fileId, hoodieTable.getBaseFileExtension());
      makeOldAndNewFilePaths(partitionPath, latestValidFilePath, newFileName);

      LOG.info(String.format("Merging new data into oldPath %s, as newPath %s", oldFilePath.toString(),
          newFilePath.toString()));
      // file name is same for all records, in this bunch
      writeStatus.setFileId(fileId);
      writeStatus.setPartitionPath(partitionPath);
      writeStatus.getStat().setPartitionPath(partitionPath);
      writeStatus.getStat().setFileId(fileId);
      setWriteStatusPath();

      // Create Marker file,
      // uses name of `newFilePath` instead of `newFileName`
      // in case the sub-class may roll over the file handle name.
      createMarkerFile(partitionPath, newFilePath.getName());

      // Create the writer for writing the new version file
      fileWriter = HoodieFileWriterFactory.getFileWriter(instantTime, newFilePath, hoodieTable.getStorage(),
          config, writeSchemaWithMetaFields, taskContextSupplier, HoodieRecord.HoodieRecordType.SPARK);
    } catch (IOException io) {
      LOG.error("Error in update task at commit " + instantTime, io);
      writeStatus.setGlobalError(io);
      throw new HoodieUpsertException("Failed to initialize HoodieUpdateHandle for FileId: " + fileId + " on commit "
          + instantTime + " on path " + hoodieTable.getMetaClient().getBasePath(), io);
    }
  }

  protected void setWriteStatusPath() {
    writeStatus.getStat().setPath(new StoragePath(config.getBasePath()), newFilePath);
  }

  protected void makeOldAndNewFilePaths(String partitionPath, String oldFileName, String newFileName) {
    oldFilePath = makeNewFilePath(partitionPath, oldFileName);
    newFilePath = makeNewFilePath(partitionPath, newFileName);
  }

  // TODO(yihua): is this still needed
  /*
  if (needsUpdateLocation()) {
        record.unseal();
        record.setNewLocation(new HoodieRecordLocation(instantTime, fileId));
        record.seal();
      }
   */

  public void write() {
    boolean usePosition = config.getBooleanOrDefault(MERGE_USE_RECORD_POSITIONS);
    Schema readerSchema;
    Option<InternalSchema> internalSchemaOption = Option.empty();
    if (!StringUtils.isNullOrEmpty(config.getInternalSchema())) {
      readerSchema = HoodieAvroUtils.addMetadataFields(
          new Schema.Parser().parse(config.getSchema()), config.allowOperationMetadataField());
      internalSchemaOption = SerDeHelper.fromJson(config.getInternalSchema());
    } else {
      readerSchema = HoodieAvroUtils.addMetadataFields(
          new Schema.Parser().parse(config.getSchema()), config.allowOperationMetadataField());
    }
    // TODO(yihua): reader schema is good enough for writer?
    HoodieFileGroupReader<T> fileGroupReader = new HoodieFileGroupReader<>(
        readerContext,
        storage.newInstance(hoodieTable.getMetaClient().getBasePath(), new HadoopStorageConfiguration(conf)),
        hoodieTable.getMetaClient().getBasePath().toString(),
        instantTime,
        fileSlice,
        readerSchema,
        readerSchema,
        internalSchemaOption,
        hoodieTable.getMetaClient(),
        hoodieTable.getMetaClient().getTableConfig().getProps(),
        0,
        Long.MAX_VALUE,
        usePosition);
    try {
      fileGroupReader.initRecordIterators();
      HoodieFileGroupReaderIterator<InternalRow> recordIterator
          = (HoodieFileGroupReaderIterator<InternalRow>) fileGroupReader.getClosableIterator();
      StructType sparkSchema = AvroConversionUtils.convertAvroSchemaToStructType(readerSchema);
      while (recordIterator.hasNext()) {
        InternalRow row = recordIterator.next();
        HoodieKey recordKey = new HoodieKey(
            row.getString(HoodieRecord.RECORD_KEY_META_FIELD_ORD),
            row.getString(HoodieRecord.PARTITION_PATH_META_FIELD_ORD));
        HoodieSparkRecord record = new HoodieSparkRecord(recordKey, row, sparkSchema, false);
        Option recordMetadata = record.getMetadata();
        if (!partitionPath.equals(record.getPartitionPath())) {
          HoodieUpsertException failureEx = new HoodieUpsertException("mismatched partition path, record partition: "
              + record.getPartitionPath() + " but trying to insert into partition: " + partitionPath);
          writeStatus.markFailure(record, failureEx, recordMetadata);
          continue;
        }
        try {
          writeToFile(recordKey, record, readerSchema, config.getPayloadConfig().getProps(), preserveMetadata);
          writeStatus.markSuccess(record, recordMetadata);
        } catch (Exception e) {
          LOG.error("Error writing record  " + record, e);
          writeStatus.markFailure(record, e, recordMetadata);
        }

      }
      // The stats of inserts, updates, and deletes are updated once at the end
      HoodieFileGroupReaderStats stats = fileGroupReader.getStats();
      this.insertRecordsWritten = stats.getNumInserts();
      this.updatedRecordsWritten = stats.getNumUpdates();
      this.recordsDeleted = stats.getNumDeletes();
      this.recordsWritten = stats.getNumInserts() + stats.getNumUpdates();
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to compact file slice: " + fileSlice, e);
    }
  }

  protected void writeToFile(HoodieKey key, HoodieSparkRecord record, Schema schema, Properties prop, boolean shouldPreserveRecordMetadata)
      throws IOException {
    // NOTE: `FILENAME_METADATA_FIELD` has to be rewritten to correctly point to the
    //       file holding this record even in cases when overall metadata is preserved
    MetadataValues metadataValues = new MetadataValues().setFileName(newFilePath.getName());
    HoodieRecord populatedRecord = record.prependMetaFields(schema, writeSchemaWithMetaFields, metadataValues, prop);

    if (shouldPreserveRecordMetadata) {
      fileWriter.write(key.getRecordKey(), populatedRecord, writeSchemaWithMetaFields);
    } else {
      fileWriter.writeWithMetadata(key, populatedRecord, writeSchemaWithMetaFields);
    }
  }

  @Override
  public List<WriteStatus> close() {
    try {
      if (isClosed()) {
        // Handle has already been closed
        return Collections.emptyList();
      }

      markClosed();

      writtenRecordKeys = null;

      fileWriter.close();
      fileWriter = null;

      long fileSizeInBytes = storage.getPathInfo(newFilePath).getLength();
      HoodieWriteStat stat = writeStatus.getStat();

      stat.setTotalWriteBytes(fileSizeInBytes);
      stat.setFileSizeInBytes(fileSizeInBytes);
      stat.setNumWrites(recordsWritten);
      stat.setNumDeletes(recordsDeleted);
      stat.setNumUpdateWrites(updatedRecordsWritten);
      stat.setNumInserts(insertRecordsWritten);
      stat.setTotalWriteErrors(writeStatus.getTotalErrorRecords());
      RuntimeStats runtimeStats = new RuntimeStats();
      runtimeStats.setTotalUpsertTime(timer.endTimer());
      stat.setRuntimeStats(runtimeStats);

      performMergeDataValidationCheck(writeStatus);

      LOG.info(String.format("MergeHandle for partitionPath %s fileID %s, took %d ms.", stat.getPartitionPath(),
          stat.getFileId(), runtimeStats.getTotalUpsertTime()));

      return Collections.singletonList(writeStatus);
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to close UpdateHandle", e);
    }
  }

  public void performMergeDataValidationCheck(WriteStatus writeStatus) {
    if (!config.isMergeDataValidationCheckEnabled()) {
      return;
    }

    long oldNumWrites = 0;
    try (HoodieFileReader reader = HoodieIOFactory.getIOFactory(hoodieTable.getStorage())
        .getReaderFactory(this.recordMerger.getRecordType())
        .getFileReader(config, oldFilePath)) {
      oldNumWrites = reader.getTotalRecords();
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to check for merge data validation", e);
    }

    if ((writeStatus.getStat().getNumWrites() + writeStatus.getStat().getNumDeletes()) < oldNumWrites) {
      throw new HoodieCorruptedDataException(
          String.format("Record write count decreased for file: %s, Partition Path: %s (%s:%d + %d < %s:%d)",
              writeStatus.getFileId(), writeStatus.getPartitionPath(),
              instantTime, writeStatus.getStat().getNumWrites(), writeStatus.getStat().getNumDeletes(),
              baseFileToMerge.getCommitTime(), oldNumWrites));
    }
  }

  public Iterator<List<WriteStatus>> getWriteStatusesAsIterator() {
    List<WriteStatus> statuses = getWriteStatuses();
    // TODO(vc): This needs to be revisited
    if (getPartitionPath() == null) {
      LOG.info("Upsert Handle has partition path as null {}, {}", getOldFilePath(), statuses);
    }
    return Collections.singletonList(statuses).iterator();
  }

  public StoragePath getOldFilePath() {
    return oldFilePath;
  }

  @Override
  public IOType getIOType() {
    return IOType.MERGE;
  }

  public HoodieBaseFile baseFileForMerge() {
    return baseFileToMerge;
  }

  public void setPartitionFields(Option<String[]> partitionFields) {
    this.partitionFields = partitionFields;
  }

  public Option<String[]> getPartitionFields() {
    return this.partitionFields;
  }

  public void setPartitionValues(Object[] partitionValues) {
    this.partitionValues = partitionValues;
  }

  public Object[] getPartitionValues() {
    return this.partitionValues;
  }
}
