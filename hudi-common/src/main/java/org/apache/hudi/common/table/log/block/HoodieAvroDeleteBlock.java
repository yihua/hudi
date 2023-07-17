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

package org.apache.hudi.common.table.log.block;

import org.apache.hudi.avro.model.HoodieDeleteRecord;
import org.apache.hudi.avro.model.OrderingBooleanWrapper;
import org.apache.hudi.avro.model.OrderingBytesWrapper;
import org.apache.hudi.avro.model.OrderingDateWrapper;
import org.apache.hudi.avro.model.OrderingDecimalWrapper;
import org.apache.hudi.avro.model.OrderingDoubleWrapper;
import org.apache.hudi.avro.model.OrderingFloatWrapper;
import org.apache.hudi.avro.model.OrderingIntWrapper;
import org.apache.hudi.avro.model.OrderingLongWrapper;
import org.apache.hudi.avro.model.OrderingStringWrapper;
import org.apache.hudi.avro.model.OrderingTimestampMicrosWrapper;
import org.apache.hudi.common.fs.SizeAwareDataInputStream;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.util.Lazy;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.util.DateTimeUtils.instantToMicros;
import static org.apache.hudi.common.util.DateTimeUtils.microsToInstant;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.tryUpcastDecimal;

public class HoodieAvroDeleteBlock extends HoodieLogBlock {
  private static final Lazy<OrderingStringWrapper.Builder> STRING_WRAPPER_BUILDER_STUB = Lazy.lazily(OrderingStringWrapper::newBuilder);
  private static final Lazy<OrderingBytesWrapper.Builder> BYTES_WRAPPER_BUILDER_STUB = Lazy.lazily(OrderingBytesWrapper::newBuilder);
  private static final Lazy<OrderingDoubleWrapper.Builder> DOUBLE_WRAPPER_BUILDER_STUB = Lazy.lazily(OrderingDoubleWrapper::newBuilder);
  private static final Lazy<OrderingFloatWrapper.Builder> FLOAT_WRAPPER_BUILDER_STUB = Lazy.lazily(OrderingFloatWrapper::newBuilder);
  private static final Lazy<OrderingLongWrapper.Builder> LONG_WRAPPER_BUILDER_STUB = Lazy.lazily(OrderingLongWrapper::newBuilder);
  private static final Lazy<OrderingIntWrapper.Builder> INT_WRAPPER_BUILDER_STUB = Lazy.lazily(OrderingIntWrapper::newBuilder);
  private static final Lazy<OrderingBooleanWrapper.Builder> BOOLEAN_WRAPPER_BUILDER_STUB = Lazy.lazily(OrderingBooleanWrapper::newBuilder);
  private static final Lazy<OrderingTimestampMicrosWrapper.Builder> TIMESTAMP_MICROS_WRAPPER_BUILDER_STUB = Lazy.lazily(OrderingTimestampMicrosWrapper::newBuilder);
  private static final Lazy<OrderingDecimalWrapper.Builder> DECIMAL_WRAPPER_BUILDER_STUB = Lazy.lazily(OrderingDecimalWrapper::newBuilder);
  private static final Lazy<OrderingDateWrapper.Builder> DATE_WRAPPER_BUILDER_STUB = Lazy.lazily(OrderingDateWrapper::newBuilder);
  private static final Conversions.DecimalConversion AVRO_DECIMAL_CONVERSION = new Conversions.DecimalConversion();

  private final ThreadLocal<BinaryEncoder> encoderCache = new ThreadLocal<>();
  private final ThreadLocal<BinaryDecoder> decoderCache = new ThreadLocal<>();
  private DeleteRecord[] recordsToDelete;

  public HoodieAvroDeleteBlock(DeleteRecord[] recordsToDelete, Map<HeaderMetadataType, String> header) {
    this(Option.empty(), null, false, Option.empty(), header, new HashMap<>());
    this.recordsToDelete = recordsToDelete;
  }

  public HoodieAvroDeleteBlock(Option<byte[]> content, FSDataInputStream inputStream, boolean readBlockLazily,
                               Option<HoodieLogBlockContentLocation> blockContentLocation, Map<HeaderMetadataType, String> header,
                               Map<HeaderMetadataType, String> footer) {
    super(header, footer, blockContentLocation, content, inputStream, readBlockLazily);
  }

  @Override
  public byte[] getContentBytes() throws IOException {
    Option<byte[]> content = getContent();

    // In case this method is called before realizing keys from content
    if (content.isPresent()) {
      return content.get();
    } else if (readBlockLazily && recordsToDelete == null) {
      // read block lazily
      getRecordsToDelete();
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(baos);
    output.writeInt(version);
    output.writeInt(getRecordsToDelete().length);
    serialize(output, getRecordsToDelete());
    return baos.toByteArray();
  }

  public DeleteRecord[] getRecordsToDelete() {
    try {
      if (recordsToDelete == null) {
        if (!getContent().isPresent() && readBlockLazily) {
          // read content from disk
          inflate();
        }
        SizeAwareDataInputStream dis =
            new SizeAwareDataInputStream(new DataInputStream(new ByteArrayInputStream(getContent().get())));
        int version = dis.readInt();
        int numDeletes = dis.readInt();
        this.recordsToDelete = new DeleteRecord[numDeletes];
        DatumReader<HoodieDeleteRecord> reader = new SpecificDatumReader<>(HoodieDeleteRecord.class);
        for (int i = 0; i < numDeletes; i++) {
          int recordLength = dis.readInt();
          BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(
              getContent().get(), dis.getNumberOfBytesRead(), recordLength, decoderCache.get());
          decoderCache.set(decoder);
          HoodieDeleteRecord deserRecord = reader.read(null, decoder);
          recordsToDelete[i] = DeleteRecord.create(
              deserRecord.getRecordKey(), deserRecord.getPartitionPath(), unwrapOrderingValueWrapper(deserRecord.getOrderingVal()));
          dis.skipBytes(recordLength);
        }
        deflate();
      }
      return recordsToDelete;
    } catch (IOException io) {
      throw new HoodieIOException("Unable to generate keys to delete from block content", io);
    }
  }

  private void serialize(DataOutputStream output, DeleteRecord[] deleteRecords) {
    DatumWriter<HoodieDeleteRecord> writer = new SpecificDatumWriter<>(HoodieDeleteRecord.class);
    for (DeleteRecord delete : deleteRecords) {
      ByteArrayOutputStream temp = new ByteArrayOutputStream();
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(temp, encoderCache.get());
      encoderCache.set(encoder);
      try {
        writer.write(HoodieDeleteRecord.newBuilder()
                .setRecordKey(delete.getRecordKey())
                .setPartitionPath(delete.getPartitionPath())
                .setOrderingVal(wrapOrderingValue(delete.getOrderingValue()))
                .build(),
            encoder);
        encoder.flush();

        // Get the size of the bytes
        int size = temp.toByteArray().length;
        // Write the record size
        output.writeInt(size);
        // Write the content
        output.write(temp.toByteArray());
      } catch (IOException e) {
        throw new HoodieIOException("IOException converting delete record to bytes", e);
      }
    }
    encoderCache.remove();
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.AVRO_DELETE_BLOCK;
  }

  private static Object wrapOrderingValue(Comparable<?> orderingValue) {
    if (orderingValue == null) {
      return null;
    } else if (orderingValue instanceof Date || orderingValue instanceof LocalDate) {
      // NOTE: Due to breaking changes in code-gen b/w Avro 1.8.2 and 1.10, we can't
      //       rely on logical types to do proper encoding of the native Java types,
      //       and hereby have to encode statistic manually
      LocalDate localDate = orderingValue instanceof LocalDate
          ? (LocalDate) orderingValue
          : ((Date) orderingValue).toLocalDate();
      return OrderingDateWrapper.newBuilder(DATE_WRAPPER_BUILDER_STUB.get())
          .setValue((int) localDate.toEpochDay())
          .build();
    } else if (orderingValue instanceof BigDecimal) {
      Schema valueSchema = OrderingDecimalWrapper.SCHEMA$.getField("value").schema();
      BigDecimal upcastDecimal = tryUpcastDecimal((BigDecimal) orderingValue, (LogicalTypes.Decimal) valueSchema.getLogicalType());
      return OrderingDecimalWrapper.newBuilder(DECIMAL_WRAPPER_BUILDER_STUB.get())
          .setValue(AVRO_DECIMAL_CONVERSION.toBytes(upcastDecimal, valueSchema, valueSchema.getLogicalType()))
          .build();
    } else if (orderingValue instanceof Timestamp) {
      // NOTE: Due to breaking changes in code-gen b/w Avro 1.8.2 and 1.10, we can't
      //       rely on logical types to do proper encoding of the native Java types,
      //       and hereby have to encode statistic manually
      Instant instant = ((Timestamp) orderingValue).toInstant();
      return OrderingTimestampMicrosWrapper.newBuilder(TIMESTAMP_MICROS_WRAPPER_BUILDER_STUB.get())
          .setValue(instantToMicros(instant))
          .build();
    } else if (orderingValue instanceof Boolean) {
      return OrderingBooleanWrapper.newBuilder(BOOLEAN_WRAPPER_BUILDER_STUB.get()).setValue((Boolean) orderingValue).build();
    } else if (orderingValue instanceof Integer) {
      return OrderingIntWrapper.newBuilder(INT_WRAPPER_BUILDER_STUB.get()).setValue((Integer) orderingValue).build();
    } else if (orderingValue instanceof Long) {
      return OrderingLongWrapper.newBuilder(LONG_WRAPPER_BUILDER_STUB.get()).setValue((Long) orderingValue).build();
    } else if (orderingValue instanceof Float) {
      return OrderingFloatWrapper.newBuilder(FLOAT_WRAPPER_BUILDER_STUB.get()).setValue((Float) orderingValue).build();
    } else if (orderingValue instanceof Double) {
      return OrderingDoubleWrapper.newBuilder(DOUBLE_WRAPPER_BUILDER_STUB.get()).setValue((Double) orderingValue).build();
    } else if (orderingValue instanceof ByteBuffer) {
      return OrderingBytesWrapper.newBuilder(BYTES_WRAPPER_BUILDER_STUB.get()).setValue((ByteBuffer) orderingValue).build();
    } else if (orderingValue instanceof String || orderingValue instanceof Utf8) {
      return OrderingStringWrapper.newBuilder(STRING_WRAPPER_BUILDER_STUB.get()).setValue(orderingValue.toString()).build();
    } else {
      throw new UnsupportedOperationException(String.format("Unsupported type of the statistic (%s)", orderingValue.getClass()));
    }
  }

  public static Comparable<?> unwrapOrderingValueWrapper(Object orderingValueWrapper) {
    if (orderingValueWrapper == null) {
      return null;
    } else if (orderingValueWrapper instanceof OrderingDateWrapper) {
      return LocalDate.ofEpochDay(((OrderingDateWrapper) orderingValueWrapper).getValue());
    } else if (orderingValueWrapper instanceof OrderingDecimalWrapper) {
      Schema valueSchema = OrderingDecimalWrapper.SCHEMA$.getField("value").schema();
      return AVRO_DECIMAL_CONVERSION.fromBytes(((OrderingDecimalWrapper) orderingValueWrapper).getValue(), valueSchema, valueSchema.getLogicalType());
    } else if (orderingValueWrapper instanceof OrderingTimestampMicrosWrapper) {
      return microsToInstant(((OrderingTimestampMicrosWrapper) orderingValueWrapper).getValue());
    } else if (orderingValueWrapper instanceof OrderingBooleanWrapper) {
      return ((OrderingBooleanWrapper) orderingValueWrapper).getValue();
    } else if (orderingValueWrapper instanceof OrderingIntWrapper) {
      return ((OrderingIntWrapper) orderingValueWrapper).getValue();
    } else if (orderingValueWrapper instanceof OrderingLongWrapper) {
      return ((OrderingLongWrapper) orderingValueWrapper).getValue();
    } else if (orderingValueWrapper instanceof OrderingFloatWrapper) {
      return ((OrderingFloatWrapper) orderingValueWrapper).getValue();
    } else if (orderingValueWrapper instanceof OrderingDoubleWrapper) {
      return ((OrderingDoubleWrapper) orderingValueWrapper).getValue();
    } else if (orderingValueWrapper instanceof OrderingBytesWrapper) {
      return ((OrderingBytesWrapper) orderingValueWrapper).getValue();
    } else if (orderingValueWrapper instanceof OrderingStringWrapper) {
      return ((OrderingStringWrapper) orderingValueWrapper).getValue();
    } else if (orderingValueWrapper instanceof GenericRecord) {
      // NOTE: This branch could be hit b/c Avro records could be reconstructed
      //       as {@code GenericRecord)
      // TODO add logical type decoding
      GenericRecord record = (GenericRecord) orderingValueWrapper;
      return (Comparable<?>) record.get("value");
    } else {
      throw new UnsupportedOperationException(String.format("Unsupported type of the statistic (%s)", orderingValueWrapper.getClass()));
    }
  }
}
