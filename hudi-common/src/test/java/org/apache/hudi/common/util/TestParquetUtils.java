/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util;

import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.model.HoodieRecord;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.util.List;

/**
 * Tests parquet utils.
 */
public class TestParquetUtils extends TestBaseFileUtilsBase {

  @Override
  public void initBaseFileUtils() {
    baseFileUtils = new ParquetUtils();
    fileName = "test.parquet";
  }

  @Override
  protected void writeFileForTesting(String typeCode, String filePath, List<String> rowKeys,
                                     Schema schema, boolean addPartitionPathField,
                                     String partitionPathValue,
                                     boolean useMetaFields, String recordFieldName,
                                     String partitionFieldName) throws Exception {
    // Write out a parquet file
    BloomFilter filter = BloomFilterFactory
        .createBloomFilter(1000, 0.0001, 10000, typeCode);
    HoodieAvroWriteSupport writeSupport =
        new HoodieAvroWriteSupport(new AvroSchemaConverter().convert(schema), schema, Option.of(filter));
    try (ParquetWriter writer =
        new ParquetWriter(new Path(filePath), writeSupport, CompressionCodecName.GZIP,
            120 * 1024 * 1024, ParquetWriter.DEFAULT_PAGE_SIZE)) {
      for (String rowKey : rowKeys) {
        GenericRecord rec = new GenericData.Record(schema);
        rec.put(useMetaFields ? HoodieRecord.RECORD_KEY_METADATA_FIELD : recordFieldName, rowKey);
        if (addPartitionPathField) {
          rec.put(useMetaFields ? HoodieRecord.PARTITION_PATH_METADATA_FIELD : partitionFieldName,
              partitionPathValue);
        }
        writer.write(rec);
        writeSupport.add(rowKey);
      }
    }
  }
}
