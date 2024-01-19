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

package org.apache.hudi.io.storage;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.io.ByteBufferBackedInputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.CollectionUtils.toStream;
import static org.apache.hudi.common.util.StringUtils.getStringFromUTF8Bytes;

public abstract class BaseHoodieAvroHFileReader extends HoodieAvroFileReaderBase
    implements HoodieSeekingFileReader<IndexedRecord> {
  // TODO HoodieHFileReader right now tightly coupled to MT, we should break that coupling
  public static final String SCHEMA_KEY = "schema";
  public static final String KEY_BLOOM_FILTER_META_BLOCK = "bloomFilter";
  public static final String KEY_BLOOM_FILTER_TYPE_CODE = "bloomFilterTypeCode";

  public static final String KEY_FIELD_NAME = "key";
  public static final String KEY_MIN_RECORD = "minRecordKey";
  public static final String KEY_MAX_RECORD = "maxRecordKey";

  /**
   * NOTE: THIS SHOULD ONLY BE USED FOR TESTING, RECORDS ARE MATERIALIZED EAGERLY
   * <p>
   * Reads all the records with given schema
   */
  public static List<IndexedRecord> readAllRecords(HoodieAvroFileReaderBase reader)
      throws IOException {
    Schema schema = reader.getSchema();
    return toStream(reader.getIndexedRecordIterator(schema))
        .collect(Collectors.toList());
  }

  /**
   * NOTE: THIS SHOULD ONLY BE USED FOR TESTING, RECORDS ARE MATERIALIZED EAGERLY
   * <p>
   * Reads all the records with given schema and filtering keys.
   */
  public static List<IndexedRecord> readRecords(BaseHoodieAvroHFileReader reader,
                                                List<String> keys) throws IOException {
    return readRecords(reader, keys, reader.getSchema());
  }

  /**
   * NOTE: THIS SHOULD ONLY BE USED FOR TESTING, RECORDS ARE MATERIALIZED EAGERLY
   * <p>
   * Reads all the records with given schema and filtering keys.
   */
  public static List<IndexedRecord> readRecords(BaseHoodieAvroHFileReader reader,
                                                List<String> keys,
                                                Schema schema) throws IOException {
    Collections.sort(keys);
    return toStream(reader.getIndexedRecordsByKeysIterator(keys, schema))
        .collect(Collectors.toList());
  }

  public abstract ClosableIterator<IndexedRecord> getIndexedRecordsByKeysIterator(List<String> keys,
                                                                                  Schema readerSchema)
      throws IOException;

  public abstract ClosableIterator<IndexedRecord> getIndexedRecordsByKeyPrefixIterator(
      List<String> sortedKeyPrefixes, Schema readerSchema) throws IOException;

  protected static GenericRecord deserialize(final byte[] keyBytes,
                                             final byte[] valueBytes,
                                             Schema writerSchema,
                                             Schema readerSchema) throws IOException {
    return deserialize(
        keyBytes, 0, keyBytes.length, valueBytes, 0, valueBytes.length, writerSchema, readerSchema);
  }

  protected static GenericRecord deserialize(final byte[] keyBytes, int keyOffset, int keyLength,
                                             final byte[] valueBytes, int valueOffset, int valueLength,
                                             Schema writerSchema,
                                             Schema readerSchema) throws IOException {
    GenericRecord record = HoodieAvroUtils.bytesToAvro(
        valueBytes, valueOffset, valueLength, writerSchema, readerSchema);

    getKeySchema(readerSchema).ifPresent(keyFieldSchema -> {
      final Object keyObject = record.get(keyFieldSchema.pos());
      if (keyObject != null && keyObject.toString().isEmpty()) {
        record.put(keyFieldSchema.pos(), getStringFromUTF8Bytes(keyBytes, keyOffset, keyLength));
      }
    });

    return record;
  }

  private static Option<Schema.Field> getKeySchema(Schema schema) {
    return Option.ofNullable(schema.getField(KEY_FIELD_NAME));
  }

  static class SeekableByteArrayInputStream extends ByteBufferBackedInputStream
      implements Seekable, PositionedReadable {
    public SeekableByteArrayInputStream(byte[] buf) {
      super(buf);
    }

    @Override
    public long getPos() throws IOException {
      return getPosition();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
      return copyFrom(position, buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      read(position, buffer, 0, buffer.length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      read(position, buffer, offset, length);
    }
  }
}
