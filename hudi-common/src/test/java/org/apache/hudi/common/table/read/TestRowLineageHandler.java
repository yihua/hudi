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

import org.apache.hudi.common.avro.AvroRecordContext;
import org.apache.hudi.common.model.BaseFileRowLineage;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.collection.ClosableIterator;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestRowLineageHandler {
  private static final HoodieSchema SCHEMA = HoodieSchema.createRecord("rec", null, null, Arrays.asList(
      HoodieSchemaField.of("key", HoodieSchema.create(HoodieSchemaType.STRING)),
      nullableLongField("row_id"),
      HoodieSchemaField.of("value", HoodieSchema.create(HoodieSchemaType.STRING)),
      nullableLongField("seq")));
  private static final HoodieSchema SCHEMA_WITHOUT_ROW_LINEAGE = HoodieSchema.createRecord("rec", null, null, Arrays.asList(
      HoodieSchemaField.of("key", HoodieSchema.create(HoodieSchemaType.STRING)),
      HoodieSchemaField.of("value", HoodieSchema.create(HoodieSchemaType.STRING))));

  private final AvroRecordContext recordContext = new AvroRecordContext();
  private final RowLineageHandler<IndexedRecord> handler =
      new RowLineageHandler<>(recordContext, Arrays.asList(nullableLongField("row_id"), nullableLongField("seq")));

  private static HoodieSchemaField nullableLongField(String name) {
    return HoodieSchemaField.of(name, HoodieSchema.createNullable(HoodieSchemaType.LONG), null, HoodieSchema.NULL_VALUE);
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
  void testFillRowLineageKeepsStoredValuesAndDerivesTheOthersFromThePosition() {
    List<IndexedRecord> baseFileRecords = Arrays.asList(
        record("key0", 7L, 3L), record("key1", null, null), record("key2", 9L, null), record("key3", null, 4L), record("key4", null, null));
    List<IndexedRecord> filled = new ArrayList<>();
    try (ClosableIterator<IndexedRecord> iterator = handler.fillRowLineage(
        ClosableIterator.wrap(baseFileRecords.iterator()), SCHEMA, new BaseFileRowLineage(100L, 5L))) {
      iterator.forEachRemaining(filled::add);
    }
    assertEquals(Arrays.asList(
        record("key0", 7L, 3L), record("key1", 101L, 5L), record("key2", 9L, 5L), record("key3", 103L, 4L), record("key4", 104L, 5L)), filled);
  }

  @Test
  void testFillRowLineageRequiresTheRowLineageColumns() {
    assertThrows(IllegalArgumentException.class, () -> handler.fillRowLineage(
        ClosableIterator.wrap(new ArrayList<IndexedRecord>().iterator()), SCHEMA_WITHOUT_ROW_LINEAGE, new BaseFileRowLineage(100L, 5L)));
  }

  @Test
  void testClearRowLineageOfChangedRecords() {
    Integer schemaId = recordContext.encodeSchema(SCHEMA);
    // A record returned the way it is stored in the base file has no operation and keeps its row lineage
    BufferedRecord<IndexedRecord> unchanged = new BufferedRecord<>("key0", 0, record("key0", 7L, 3L), schemaId, null);
    handler.clearRowLineageIfChanged(unchanged);
    assertEquals(record("key0", 7L, 3L), unchanged.getRecord());

    for (HoodieOperation operation : Arrays.asList(HoodieOperation.UPDATE_AFTER, HoodieOperation.INSERT)) {
      BufferedRecord<IndexedRecord> changed = new BufferedRecord<>("key1", 0, record("key1", 7L, 3L), schemaId, operation);
      handler.clearRowLineageIfChanged(changed);
      assertEquals(record("key1", null, null), changed.getRecord());
    }

    // A changed record in a schema without the row lineage columns has no row lineage to clear
    GenericData.Record withoutRowLineage = new GenericData.Record(SCHEMA_WITHOUT_ROW_LINEAGE.toAvroSchema());
    withoutRowLineage.put("key", "key2");
    withoutRowLineage.put("value", "value_key2");
    BufferedRecord<IndexedRecord> changedWithoutRowLineage = new BufferedRecord<>(
        "key2", 0, withoutRowLineage, recordContext.encodeSchema(SCHEMA_WITHOUT_ROW_LINEAGE), HoodieOperation.UPDATE_AFTER);
    handler.clearRowLineageIfChanged(changedWithoutRowLineage);
    assertSame(withoutRowLineage, changedWithoutRowLineage.getRecord());
    assertEquals("value_key2", withoutRowLineage.get("value"));

    // The schema of the previous changed record does not apply to the next one
    BufferedRecord<IndexedRecord> changedAgain = new BufferedRecord<>("key3", 0, record("key3", 8L, 4L), schemaId, HoodieOperation.UPDATE_AFTER);
    handler.clearRowLineageIfChanged(changedAgain);
    assertEquals(record("key3", null, null), changedAgain.getRecord());

    BufferedRecord<IndexedRecord> deleted = new BufferedRecord<>("key4", 0, record("key4", 7L, 3L), schemaId, HoodieOperation.DELETE);
    handler.clearRowLineageIfChanged(deleted);
    assertEquals(record("key4", 7L, 3L), deleted.getRecord());
  }
}
