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

package org.apache.hudi.io.vortex;

import org.apache.hudi.avro.HoodieBloomFilterWriteSupport;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.memory.HoodieArrowAllocator;
import org.apache.hudi.storage.StoragePath;

import dev.vortex.api.Session;
import dev.vortex.api.VortexWriter;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Base class for Hudi Vortex file writers supporting different record types.
 *
 * This class handles common Vortex file operations including:
 * - VortexWriter lifecycle management
 * - BufferAllocator management
 * - Record buffering and batch flushing
 * - File size checks
 * - Bloom filter metadata writing
 *
 * Subclasses must implement type-specific conversion to Arrow format.
 *
 * @param <R> The record type (e.g., GenericRecord, InternalRow)
 */
@NotThreadSafe
public abstract class HoodieBaseVortexWriter<R, K extends Comparable<K>> implements Closeable {
  protected static final int DEFAULT_BATCH_SIZE = 1000;
  private final StoragePath path;
  private final BufferAllocator allocator;
  private final int batchSize;
  private final long flushByteWatermark;
  @Getter(value = AccessLevel.PROTECTED)
  private long writtenRecordCount = 0;
  private long totalFlushedDataSize = 0;
  private int currentBatchSize = 0;
  private VectorSchemaRoot root;
  private ArrowWriter<R> arrowWriter;
  protected final Option<HoodieBloomFilterWriteSupport<K>> bloomFilterWriteSupportOpt;

  private Session session;
  private VortexWriter writer;

  /**
   * Constructor for base Vortex writer.
   *
   * @param path Path where Vortex file will be written
   * @param batchSize Row-count threshold; the current batch is flushed when this many records have been buffered
   * @param allocatorSize Maximum bytes the per-writer Arrow child allocator may hold at once
   * @param flushByteWatermark Byte-size threshold; the current batch is flushed when the sum of in-flight
   *                           FieldVector buffer sizes reaches this value
   * @param bloomFilterWriteSupportOpt Optional bloom filter write support for record key tracking
   */
  protected HoodieBaseVortexWriter(StoragePath path, int batchSize, long allocatorSize, long flushByteWatermark,
                                   Option<HoodieBloomFilterWriteSupport<K>> bloomFilterWriteSupportOpt) {
    this.path = path;
    this.allocator = HoodieArrowAllocator.newChildAllocator(
        getClass().getSimpleName() + "-data-" + path.getName(), allocatorSize);
    this.batchSize = batchSize;
    this.flushByteWatermark = flushByteWatermark;
    this.bloomFilterWriteSupportOpt = bloomFilterWriteSupportOpt;
  }

  /**
   * Create and initialize the arrow writer for writing records to VectorSchemaRoot.
   * Called once during lazy initialization when the first record is written.
   *
   * @param root The VectorSchemaRoot to write into
   * @return An arrow writer implementation that writes records of type R to the root
   */
  protected abstract ArrowWriter<R> createArrowWriter(VectorSchemaRoot root);

  /**
   * Get the Arrow schema for this writer.
   * Subclasses must provide the Arrow schema corresponding to their record type.
   *
   * @return Arrow schema
   */
  protected abstract Schema getArrowSchema();

  /**
   * Subclass hook for emitting additional Vortex file-footer key-value metadata
   * alongside any bloom-filter entries. Called once during {@link #close()}.
   *
   * @return map of footer metadata key-value pairs, or empty map for none
   */
  protected Map<String, String> additionalSchemaMetadata() {
    return Collections.emptyMap();
  }

  /**
   * Write a single record. Records are buffered and flushed in batches.
   *
   * @param record Record to write
   * @throws IOException if write fails
   */
  public void write(R record) throws IOException {
    // Lazy initialization on first write
    if (writer == null) {
      initializeWriter();
    }
    if (root == null) {
      root = VectorSchemaRoot.create(getArrowSchema(), allocator);
    }
    if (arrowWriter == null) {
      arrowWriter = createArrowWriter(root);
    }

    // Reset arrow writer at the start of each new batch
    if (currentBatchSize == 0) {
      arrowWriter.reset();
    }

    arrowWriter.write(record);
    currentBatchSize++;
    writtenRecordCount++;

    // Flush when row-count batch is full OR in-flight Arrow buffers cross the byte watermark.
    if (currentBatchSize >= batchSize || currentBufferBytes() >= flushByteWatermark) {
      flushBatch();
    }
  }

  /**
   * Bytes currently held by the writer's Arrow child allocator.
   */
  private long currentBufferBytes() {
    return allocator.getAllocatedMemory();
  }

  /**
   * Close the writer, flushing any remaining buffered records.
   *
   * @throws IOException if close fails
   */
  @Override
  public void close() throws IOException {
    Exception primaryException = null;

    // 1. Flush remaining records
    try {
      if (currentBatchSize > 0) {
        flushBatch();
      }

      // Ensure writer is initialized even if no data was written
      if (writer == null && root == null) {
        initializeWriter();
        root = VectorSchemaRoot.create(getArrowSchema(), allocator);
        root.setRowCount(0);
        writer.writeBatch(root);
      }

      if (writer != null) {
        // Finalize and write bloom filter metadata
        if (bloomFilterWriteSupportOpt.isPresent()) {
          Map<String, String> metadata = bloomFilterWriteSupportOpt.get().finalizeMetadata();
          if (!metadata.isEmpty()) {
            writer.setMetadata(metadata);
          }
        }

        Map<String, String> extra = additionalSchemaMetadata();
        if (extra != null && !extra.isEmpty()) {
          writer.setMetadata(extra);
        }
      }
    } catch (Exception e) {
      primaryException = e;
    }

    // Close Vortex writer
    if (writer != null) {
      try {
        writer.close();
      } catch (Exception e) {
        if (primaryException == null) {
          primaryException = e;
        } else {
          primaryException.addSuppressed(e);
        }
      }
    }

    // Close Session
    if (session != null) {
      try {
        session.close();
      } catch (Exception e) {
        if (primaryException == null) {
          primaryException = e;
        } else {
          primaryException.addSuppressed(e);
        }
      }
    }

    // Close VectorSchemaRoot
    if (root != null) {
      try {
        root.close();
      } catch (Exception e) {
        if (primaryException == null) {
          primaryException = e;
        } else {
          primaryException.addSuppressed(e);
        }
      }
    }

    // Always close allocator
    try {
      allocator.close();
    } catch (Exception e) {
      if (primaryException == null) {
        primaryException = e;
      } else {
        primaryException.addSuppressed(e);
      }
    }

    if (primaryException != null) {
      throw new HoodieException("Failed to close Vortex writer: " + path, primaryException);
    }
  }

  /**
   * Returns the estimated data size in bytes, including both flushed batches and the current
   * in-progress batch.
   */
  protected long getDataSize() {
    long currentBufferSize = currentBatchSize > 0 ? allocator.getAllocatedMemory() : 0;
    return totalFlushedDataSize + currentBufferSize;
  }

  /**
   * Flush buffered records to Vortex file.
   */
  private void flushBatch() throws IOException {
    if (currentBatchSize == 0) {
      return;
    }

    arrowWriter.finishBatch();

    for (FieldVector vector : root.getFieldVectors()) {
      totalFlushedDataSize += vector.getBufferSize();
    }

    writer.writeBatch(root);

    // Release Arrow buffers so capacity does not accumulate across batches.
    root.close();
    root = null;
    arrowWriter = null;

    currentBatchSize = 0;
  }

  /**
   * Initialize VortexWriter (lazy initialization).
   */
  private void initializeWriter() throws IOException {
    session = Session.create();
    writer = session.newWriter(path.toString(), getArrowSchema());
  }

  /**
   * Arrow writer interface for writing records of type T to a VectorSchemaRoot.
   * @param <T> the record type
   */
  protected interface ArrowWriter<T> {
    void write(T row) throws IOException;

    void finishBatch() throws IOException;

    void reset();
  }
}
