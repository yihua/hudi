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

package org.apache.hudi.common.testutils;

import org.apache.hudi.common.model.HoodieKey;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Validates that {@link HoodieTestDataGenerator} populates the unstructured-type columns
 * (VARIANT/VECTOR/BLOB) by default, while still supporting the legacy
 * {@code TRIP_EXAMPLE_SCHEMA_NO_UNSTRUCTURED} fallback for tests that have not yet been
 * migrated.
 */
class TestHoodieTestDataGenerator {

  @Test
  void defaultSchemaIncludesUnstructuredTypes() {
    Schema schema = HoodieTestDataGenerator.AVRO_SCHEMA;
    Schema variantField = unwrap(schema.getField("variant_data").schema());
    assertEquals(Schema.Type.RECORD, variantField.getType());
    assertEquals("variant", variantField.getLogicalType().getName());

    Schema vectorField = unwrap(schema.getField("embedding").schema());
    assertEquals(Schema.Type.FIXED, vectorField.getType());
    assertEquals("vector", vectorField.getLogicalType().getName());

    Schema blobField = unwrap(schema.getField("blob_data").schema());
    assertEquals(Schema.Type.RECORD, blobField.getType());
    assertEquals("blob", blobField.getLogicalType().getName());
  }

  @Test
  void defaultRecordsHaveUnstructuredFieldsPopulated() {
    HoodieTestDataGenerator gen = new HoodieTestDataGenerator(0L);
    HoodieKey key = new HoodieKey(UUID.randomUUID().toString(), HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH);
    GenericRecord rec = (GenericRecord) gen.generateRandomValue(key, "001");

    Object variant = rec.get("variant_data");
    assertNotNull(variant);
    assertEquals(Schema.Type.RECORD, ((GenericRecord) variant).getSchema().getType());

    Object embedding = rec.get("embedding");
    assertNotNull(embedding);
    assertEquals(8 * Float.BYTES, ((GenericData.Fixed) embedding).bytes().length);

    GenericRecord blob = (GenericRecord) rec.get("blob_data");
    assertNotNull(blob);
    assertEquals("INLINE", blob.get("type").toString());
    assertNotNull(blob.get("data"));
    assertNull(blob.get("reference"));
  }

  @Test
  void legacySchemaOmitsUnstructuredTypes() {
    Schema schema = HoodieTestDataGenerator.AVRO_SCHEMA_NO_UNSTRUCTURED;
    assertNull(schema.getField("variant_data"));
    assertNull(schema.getField("embedding"));
    assertNull(schema.getField("blob_data"));
  }

  private static Schema unwrap(Schema s) {
    if (s.getType() != Schema.Type.UNION) {
      return s;
    }
    for (Schema branch : s.getTypes()) {
      if (branch.getType() != Schema.Type.NULL) {
        return branch;
      }
    }
    throw new IllegalStateException("union has no non-null branch: " + s);
  }
}
