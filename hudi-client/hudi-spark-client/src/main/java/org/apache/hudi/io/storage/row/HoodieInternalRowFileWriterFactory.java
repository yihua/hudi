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

package org.apache.hudi.io.storage.row;

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.storage.HoodieLocation;
import org.apache.hudi.io.storage.HoodieParquetConfig;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.sql.types.StructType;

import java.io.IOException;

import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;

/**
 * Factory to assist in instantiating a new {@link HoodieInternalRowFileWriter}.
 */
public class HoodieInternalRowFileWriterFactory {

  /**
   * Factory method to assist in instantiating an instance of {@link HoodieInternalRowFileWriter}.
   * @param location location of the RowFileWriter.
   * @param hoodieTable instance of {@link HoodieTable} in use.
   * @param writeConfig instance of {@link HoodieWriteConfig} to use.
   * @param schema schema of the dataset in use.
   * @return the instantiated {@link HoodieInternalRowFileWriter}.
   * @throws IOException if format is not supported or if any exception during instantiating the RowFileWriter.
   *
   */
  public static HoodieInternalRowFileWriter getInternalRowFileWriter(HoodieLocation location,
                                                                     HoodieTable hoodieTable,
                                                                     HoodieWriteConfig writeConfig,
                                                                     StructType schema)
      throws IOException {
    final String extension = FSUtils.getFileExtension(location.getName());
    if (PARQUET.getFileExtension().equals(extension)) {
      return newParquetInternalRowFileWriter(location, hoodieTable, writeConfig, schema, tryInstantiateBloomFilter(writeConfig));
    }
    throw new UnsupportedOperationException(extension + " format not supported yet.");
  }

  private static HoodieInternalRowFileWriter newParquetInternalRowFileWriter(HoodieLocation location,
                                                                             HoodieTable table,
                                                                             HoodieWriteConfig writeConfig,
                                                                             StructType structType,
                                                                             Option<BloomFilter> bloomFilterOpt
  )
      throws IOException {
    HoodieRowParquetWriteSupport writeSupport =
            new HoodieRowParquetWriteSupport(table.getHadoopConf(), structType, bloomFilterOpt, writeConfig.getStorageConfig());

    return new HoodieInternalRowParquetWriter(
        location,
        new HoodieParquetConfig<>(
            writeSupport,
            writeConfig.getParquetCompressionCodec(),
            writeConfig.getParquetBlockSize(),
            writeConfig.getParquetPageSize(),
            writeConfig.getParquetMaxFileSize(),
            writeSupport.getHadoopConf(),
            writeConfig.getParquetCompressionRatio(),
            writeConfig.parquetDictionaryEnabled()
        ));
  }

  private static Option<BloomFilter> tryInstantiateBloomFilter(HoodieWriteConfig writeConfig) {
    // NOTE: Currently Bloom Filter is only going to be populated if meta-fields are populated
    if (writeConfig.populateMetaFields()) {
      BloomFilter bloomFilter = BloomFilterFactory.createBloomFilter(
          writeConfig.getBloomFilterNumEntries(),
          writeConfig.getBloomFilterFPP(),
          writeConfig.getDynamicBloomFilterMaxNumEntries(),
          writeConfig.getBloomFilterType());

      return Option.of(bloomFilter);
    }

    return Option.empty();
  }
}
