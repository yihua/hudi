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

import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.BaseFileRowLineage;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;

import java.util.List;

/**
 * Maintains the row lineage columns, see {@link org.apache.hudi.common.HoodieTableFormat#getRowLineageColumns()},
 * of the records a {@link HoodieFileGroupReader} returns.
 *
 * <p>A record returned the way it is stored in the base file keeps its row lineage: the values the base file
 * stores for it, and for a value the base file does not store, the value the table format derives from the
 * {@link BaseFileRowLineage} of the base file and the position of the record in the file. Every other record,
 * i.e., one the read inserts or changes, is returned with null in the row lineage columns.
 */
final class RowLineageHandler<T> {
  private final RecordContext<T> recordContext;
  private final String rowIdColumn;
  private final String sequenceNumberColumn;
  // Positions of the row lineage columns in the schema of the last changed record, -1 if the schema has none
  private Integer changedRecordSchemaId;
  private int changedRecordRowIdPos;
  private int changedRecordSequenceNumberPos;

  RowLineageHandler(RecordContext<T> recordContext, List<HoodieSchemaField> rowLineageColumns) {
    ValidationUtils.checkArgument(rowLineageColumns.size() == 2,
        () -> "Expected a row ID column and a last updated sequence number column, got: " + rowLineageColumns);
    this.recordContext = recordContext;
    this.rowIdColumn = rowLineageColumns.get(0).name();
    this.sequenceNumberColumn = rowLineageColumns.get(1).name();
  }

  /**
   * Fills the row lineage columns that the base file does not store.
   *
   * @param baseFileRecords    iterator over all the records of the base file, in the order of the file
   * @param schema             schema of the records, which has the row lineage columns
   * @param baseFileRowLineage values the table format assigned to the base file
   * @return an iterator over the same records with the row lineage columns filled
   */
  ClosableIterator<T> fillRowLineage(ClosableIterator<T> baseFileRecords, HoodieSchema schema, BaseFileRowLineage baseFileRowLineage) {
    int rowIdPos = getFieldPos(schema, rowIdColumn);
    int sequenceNumberPos = getFieldPos(schema, sequenceNumberColumn);
    ValidationUtils.checkArgument(rowIdPos >= 0 && sequenceNumberPos >= 0,
        () -> "The row lineage columns " + rowIdColumn + " and " + sequenceNumberColumn + " have to be read from the base file, got: " + schema);
    long firstRowId = baseFileRowLineage.getFirstRowId();
    long sequenceNumber = baseFileRowLineage.getSequenceNumber();
    return new ClosableIterator<T>() {
      private long position = 0;

      @Override
      public boolean hasNext() {
        return baseFileRecords.hasNext();
      }

      @Override
      public T next() {
        T record = recordContext.setLongIfNull(baseFileRecords.next(), rowIdPos, firstRowId + position++);
        return recordContext.setLongIfNull(record, sequenceNumberPos, sequenceNumber);
      }

      @Override
      public void close() {
        baseFileRecords.close();
      }
    };
  }

  /**
   * Sets the row lineage columns to null if the record is not returned the way it is stored in the base file.
   */
  void clearRowLineageIfChanged(BufferedRecord<T> record) {
    if (record.getHoodieOperation() == null || record.isDelete() || record.isEmpty() || record.getSchemaId() == null) {
      return;
    }
    if (!record.getSchemaId().equals(changedRecordSchemaId)) {
      HoodieSchema schema = recordContext.getSchemaFromBufferRecord(record);
      changedRecordSchemaId = record.getSchemaId();
      changedRecordRowIdPos = getFieldPos(schema, rowIdColumn);
      changedRecordSequenceNumberPos = getFieldPos(schema, sequenceNumberColumn);
    }
    if (changedRecordRowIdPos >= 0) {
      record.replaceRecord(recordContext.setNullAt(record.getRecord(), changedRecordRowIdPos));
    }
    if (changedRecordSequenceNumberPos >= 0) {
      record.replaceRecord(recordContext.setNullAt(record.getRecord(), changedRecordSequenceNumberPos));
    }
  }

  private static int getFieldPos(HoodieSchema schema, String fieldName) {
    Option<HoodieSchemaField> field = schema.getField(fieldName);
    return field.isPresent() ? field.get().pos() : -1;
  }
}
