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

package org.apache.hudi.merge;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.spark.sql.HoodieInternalRowUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Util class to merge records that may contain partial updates.
 * This can be plugged into any Spark {@link HoodieRecordMerger} implementation.
 */
public class SparkRecordMergingUtils {
  private static final Map<Schema, Map<Integer, StructField>> FIELD_ID_TO_FIELD_MAPPING_CACHE = new ConcurrentHashMap<>();
  private static final Map<Schema, Map<String, Integer>> FIELD_NAME_TO_ID_MAPPING_CACHE = new ConcurrentHashMap<>();
  private static final Map<Pair<Schema, Schema>,
      Pair<Map<Integer, StructField>, Pair<StructType, Schema>>> MERGED_SCHEMA_CACHE = new ConcurrentHashMap<>();

  /**
   * Merges records which can contain partial updates.
   *
   * @param older     Older {@link HoodieSparkRecord}.
   * @param oldSchema Old schema.
   * @param newer     Newer {@link HoodieSparkRecord}.
   * @param newSchema New schema.
   * @param props     Configuration in {@link TypedProperties}.
   * @return The merged record.
   */
  public static Pair<HoodieRecord, Schema> mergeCompleteOrPartialRecords(HoodieSparkRecord older,
                                                                         Schema oldSchema,
                                                                         HoodieSparkRecord newer,
                                                                         Schema newSchema,
                                                                         TypedProperties props) {
    Pair<Map<Integer, StructField>, Pair<StructType, Schema>> mappingSchemaPair =
        getCachedMergedSchema(oldSchema, newSchema);
    boolean isNewerPartial = isPartial(newSchema, mappingSchemaPair.getRight().getRight());
    if (isNewerPartial) {
      InternalRow oldRow = older.getData();
      InternalRow newPartialRow = newer.getData();

      Map<Integer, StructField> mergedIdToFieldMapping = mappingSchemaPair.getLeft();
      Map<String, Integer> newPartialNameToIdMapping = getCachedFieldNameToIdMapping(newSchema);
      List<Object> values = new ArrayList<>(mergedIdToFieldMapping.size());
      for (int fieldId = 0; fieldId < mergedIdToFieldMapping.size(); fieldId++) {
        StructField structField = mergedIdToFieldMapping.get(fieldId);
        Integer ordInPartialUpdate = newPartialNameToIdMapping.get(structField.name());
        if (ordInPartialUpdate != null) {
          // pick from new
          values.add(newPartialRow.get(ordInPartialUpdate, structField.dataType()));
        } else {
          // pick from old
          values.add(oldRow.get(fieldId, structField.dataType()));
        }
      }
      InternalRow mergedRow = new GenericInternalRow(values.toArray());

      HoodieSparkRecord mergedSparkRecord = new HoodieSparkRecord(
          mergedRow, mappingSchemaPair.getRight().getLeft());
      return Pair.of(mergedSparkRecord, mappingSchemaPair.getRight().getRight());
    } else {
      return Pair.of(newer, newSchema);
    }
  }

  /**
   * @param avroSchema Avro schema.
   * @return The field ID to {@link StructField} instance mapping.
   */
  public static Map<Integer, StructField> getCachedFieldIdToFieldMapping(Schema avroSchema) {
    return FIELD_ID_TO_FIELD_MAPPING_CACHE.computeIfAbsent(avroSchema, schema -> {
      StructType structType = HoodieInternalRowUtils.getCachedSchema(schema);
      Map<Integer, StructField> schemaFieldIdMapping = new HashMap<>();
      int fieldId = 0;

      for (StructField field : structType.fields()) {
        schemaFieldIdMapping.put(fieldId, field);
        fieldId++;
      }

      return schemaFieldIdMapping;
    });
  }

  /**
   * @param avroSchema Avro schema.
   * @return The field name to ID mapping.
   */
  public static Map<String, Integer> getCachedFieldNameToIdMapping(Schema avroSchema) {
    return FIELD_NAME_TO_ID_MAPPING_CACHE.computeIfAbsent(avroSchema, schema -> {
      StructType structType = HoodieInternalRowUtils.getCachedSchema(schema);
      Map<String, Integer> schemaFieldIdMapping = new HashMap<>();
      int fieldId = 0;

      for (StructField field : structType.fields()) {
        schemaFieldIdMapping.put(field.name(), fieldId);
        fieldId++;
      }

      return schemaFieldIdMapping;
    });
  }

  /**
   * Merges the two schemas so the merged schema contains all the fields from the two schemas,
   * with the same ordering of fields, and the fields of the old schema comes first.
   *
   * @param oldSchema Old schema.
   * @param newSchema New schema.
   * @return The ID to {@link StructField} instance mapping of the merged schema, and the
   * {@link StructType} and Avro schema of the merged schema.
   */
  public static Pair<Map<Integer, StructField>, Pair<StructType, Schema>> getCachedMergedSchema(Schema oldSchema,
                                                                                                Schema newSchema) {
    return MERGED_SCHEMA_CACHE.computeIfAbsent(Pair.of(oldSchema, newSchema), schemaPair -> {
      Schema schema1 = schemaPair.getLeft();
      Schema schema2 = schemaPair.getRight();
      Map<Integer, StructField> idToFieldMapping1 = getCachedFieldIdToFieldMapping(schema1);
      Map<Integer, StructField> idToFieldMapping2 = getCachedFieldIdToFieldMapping(schema2);
      Map<String, Integer> nameToIdMapping1 = getCachedFieldNameToIdMapping(schema1);
      Map<String, Integer> nameToIdMapping2 = getCachedFieldNameToIdMapping(schema2);
      List<Integer> newFieldIdList = new ArrayList<>();
      for (String name : nameToIdMapping2.keySet()) {
        if (!nameToIdMapping1.containsKey(name)) {
          newFieldIdList.add(nameToIdMapping2.get(name));
        }
      }

      if (newFieldIdList.isEmpty()) {
        return Pair.of(idToFieldMapping1, Pair.of(HoodieInternalRowUtils.getCachedSchema(oldSchema), schema1));
      } else {
        Map<Integer, StructField> mergedMapping = new HashMap<>(idToFieldMapping1);
        int newFieldId = mergedMapping.size();
        newFieldIdList.sort(Comparator.naturalOrder());
        for (Integer fieldId : newFieldIdList) {
          mergedMapping.put(newFieldId, idToFieldMapping2.get(fieldId));
          newFieldId++;
        }
        StructField[] fields = new StructField[mergedMapping.size()];
        for (int i = 0; i < fields.length; i++) {
          fields[i] = mergedMapping.get(i);
        }
        StructType mergedStructType = new StructType(fields);
        Schema mergedSchema = AvroConversionUtils.convertStructTypeToAvroSchema(
            mergedStructType, schema2.getName(), schema2.getNamespace());
        return Pair.of(mergedMapping, Pair.of(mergedStructType, mergedSchema));
      }
    });
  }

  /**
   * @param schema       Avro schema to check.
   * @param mergedSchema The merged schema for the merged record.
   * @return whether the Avro schema is partial compared to the merged schema.
   */
  public static boolean isPartial(Schema schema, Schema mergedSchema) {
    return !schema.equals(mergedSchema);
  }
}
