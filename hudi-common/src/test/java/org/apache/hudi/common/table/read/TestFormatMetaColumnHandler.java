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

import org.apache.hudi.common.BaseFileColumnResolver;
import org.apache.hudi.common.FormatMetaColumn;
import org.apache.hudi.common.avro.AvroRecordContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.collection.ClosableIterator;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests {@link FormatMetaColumnHandler} with two columns that keep a stored value and otherwise resolve one from the
 * position of a record in the base file, or a value of the base file.
 */
class TestFormatMetaColumnHandler {
  private static final HoodieSchema SCHEMA = HoodieSchema.createRecord("rec", null, null, Arrays.asList(
      HoodieSchemaField.of("key", HoodieSchema.create(HoodieSchemaType.STRING)),
      nullableLongField("row_id"),
      HoodieSchemaField.of("value", HoodieSchema.create(HoodieSchemaType.STRING)),
      nullableLongField("seq")));
  private static final HoodieSchema SCHEMA_WITHOUT_FORMAT_META_COLUMNS = HoodieSchema.createRecord("rec", null, null, Arrays.asList(
      HoodieSchemaField.of("key", HoodieSchema.create(HoodieSchemaType.STRING)),
      HoodieSchemaField.of("value", HoodieSchema.create(HoodieSchemaType.STRING))));
  private static final List<FormatMetaColumn> COLUMNS = Arrays.asList(column("row_id", null), column("seq", null));
  private static final List<BaseFileColumnResolver> RESOLVERS = Arrays.asList(
      (rowPosition, storedValue) -> storedValue != null ? storedValue : (Object) (100L + rowPosition),
      (rowPosition, storedValue) -> storedValue != null ? storedValue : (Object) 5L);

  private final AvroRecordContext recordContext = new AvroRecordContext();
  private final FormatMetaColumnHandler<IndexedRecord> handler = new FormatMetaColumnHandler<>(recordContext, COLUMNS, RESOLVERS, "001");

  private static HoodieSchemaField nullableLongField(String name) {
    return HoodieSchemaField.of(name, HoodieSchema.createNullable(HoodieSchemaType.LONG), null, HoodieSchema.NULL_VALUE);
  }

  private static FormatMetaColumn column(String name, Long valueOnWrite) {
    return new FormatMetaColumn() {
      @Override
      public HoodieSchemaField field() {
        return nullableLongField(name);
      }

      @Override
      public Object valueOnWrite(String instantTime) {
        return valueOnWrite;
      }

      @Override
      public BaseFileColumnResolver resolverFor(HoodieTableMetaClient metaClient, HoodieBaseFile baseFile) {
        throw new UnsupportedOperationException();
      }
    };
  }

  private static IndexedRecord record(String key, Long rowId, Long sequenceNumber) {
    GenericData.Record record = new GenericData.Record(SCHEMA.toAvroSchema());
    record.put("key", key);
    record.put("row_id", rowId);
    record.put("value", "value_" + key);
    record.put("seq", sequenceNumber);
    return record;
  }

  @Test
  void testResolveBaseFileRecordsKeepsStoredValuesAndResolvesTheOthersFromThePosition() {
    List<IndexedRecord> baseFileRecords = Arrays.asList(
        record("key0", 7L, 3L), record("key1", null, null), record("key2", 9L, null), record("key3", null, 4L), record("key4", null, null));
    List<IndexedRecord> resolved = new ArrayList<>();
    try (ClosableIterator<IndexedRecord> iterator = handler.resolveBaseFileRecords(ClosableIterator.wrap(baseFileRecords.iterator()), SCHEMA)) {
      iterator.forEachRemaining(resolved::add);
    }
    assertEquals(Arrays.asList(
        record("key0", 7L, 3L), record("key1", 101L, 5L), record("key2", 9L, 5L), record("key3", 103L, 4L), record("key4", 104L, 5L)), resolved);
  }

  @Test
  void testResolveBaseFileRecordsRequiresTheColumnsAndTheResolvers() {
    List<IndexedRecord> withoutColumns = Collections.singletonList(new GenericData.Record(SCHEMA_WITHOUT_FORMAT_META_COLUMNS.toAvroSchema()));
    assertThrows(IllegalArgumentException.class, () -> handler.resolveBaseFileRecords(
        ClosableIterator.wrap(withoutColumns.iterator()), SCHEMA_WITHOUT_FORMAT_META_COLUMNS).next());
    FormatMetaColumnHandler<IndexedRecord> withoutBaseFile = new FormatMetaColumnHandler<>(recordContext, COLUMNS, Collections.emptyList(), "001");
    assertThrows(IllegalArgumentException.class, () -> withoutBaseFile.resolveNextBaseFileRecord(record("key0", null, null), SCHEMA));
  }

  @Test
  void testSetValuesOnWriteOfChangedRecords() {
    Integer schemaId = recordContext.encodeSchema(SCHEMA);
    // A record returned the way it is stored in the base file has no operation and keeps its values
    BufferedRecord<IndexedRecord> unchanged = new BufferedRecord<>("key0", 0, record("key0", 7L, 3L), schemaId, null);
    handler.setValuesOnWriteIfChanged(unchanged);
    assertEquals(record("key0", 7L, 3L), unchanged.getRecord());

    for (HoodieOperation operation : Arrays.asList(HoodieOperation.UPDATE_AFTER, HoodieOperation.INSERT)) {
      BufferedRecord<IndexedRecord> changed = new BufferedRecord<>("key1", 0, record("key1", 7L, 3L), schemaId, operation);
      handler.setValuesOnWriteIfChanged(changed);
      assertEquals(record("key1", null, null), changed.getRecord());
    }

    // A changed record in a schema without the format meta columns has no values to set
    GenericData.Record withoutColumns = new GenericData.Record(SCHEMA_WITHOUT_FORMAT_META_COLUMNS.toAvroSchema());
    withoutColumns.put("key", "key2");
    withoutColumns.put("value", "value_key2");
    BufferedRecord<IndexedRecord> changedWithoutColumns = new BufferedRecord<>(
        "key2", 0, withoutColumns, recordContext.encodeSchema(SCHEMA_WITHOUT_FORMAT_META_COLUMNS), HoodieOperation.UPDATE_AFTER);
    handler.setValuesOnWriteIfChanged(changedWithoutColumns);
    assertSame(withoutColumns, changedWithoutColumns.getRecord());
    assertEquals("value_key2", withoutColumns.get("value"));

    // The schema of the previous changed record does not apply to the next one
    BufferedRecord<IndexedRecord> changedAgain = new BufferedRecord<>("key3", 0, record("key3", 8L, 4L), schemaId, HoodieOperation.UPDATE_AFTER);
    handler.setValuesOnWriteIfChanged(changedAgain);
    assertEquals(record("key3", null, null), changedAgain.getRecord());

    BufferedRecord<IndexedRecord> deleted = new BufferedRecord<>("key4", 0, record("key4", 7L, 3L), schemaId, HoodieOperation.DELETE);
    handler.setValuesOnWriteIfChanged(deleted);
    assertEquals(record("key4", 7L, 3L), deleted.getRecord());
  }

  @Test
  void testNonNullValuesOnWrite() {
    FormatMetaColumnHandler<IndexedRecord> withValues = new FormatMetaColumnHandler<>(
        recordContext, Arrays.asList(column("row_id", null), column("seq", 42L)), RESOLVERS, "001");
    BufferedRecord<IndexedRecord> changed = new BufferedRecord<>("key1", 0, record("key1", 7L, 3L), recordContext.encodeSchema(SCHEMA), HoodieOperation.UPDATE_AFTER);
    withValues.setValuesOnWriteIfChanged(changed);
    assertEquals(record("key1", null, 42L), changed.getRecord());

    // A record written as new gets the non-null values on write in place
    IndexedRecord inserted = record("key5", null, null);
    assertSame(inserted, withValues.setNonNullValuesOnWrite(inserted, SCHEMA));
    assertEquals(record("key5", null, 42L), inserted);
    assertThrows(IllegalArgumentException.class, () -> withValues.setNonNullValuesOnWrite(
        new GenericData.Record(SCHEMA_WITHOUT_FORMAT_META_COLUMNS.toAvroSchema()), SCHEMA_WITHOUT_FORMAT_META_COLUMNS));
  }
}
