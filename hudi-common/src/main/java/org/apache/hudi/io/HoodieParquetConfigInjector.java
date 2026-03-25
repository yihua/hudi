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

package org.apache.hudi.io;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

/**
 * A pluggable interface that all parquet-based writers (Spark/Flink) will invoke before creating write support
 * or parquet file writer objects.
 * <p>
 * This allows users to inject custom configurations into the Parquet writer pipeline at runtime, enabling
 * fine-grained control over Parquet file properties such as bloom filters, compression settings, encoding
 * options, and other advanced Parquet configurations.
 * <p>
 * Implementations of this interface can modify both the storage configuration (e.g., Hadoop Configuration)
 * and the Hudi-specific configuration before the Parquet writer is created.
 * <p>
 * Example use cases:
 * <ul>
 *   <li>Enabling column-specific Parquet bloom filters</li>
 *   <li>Setting custom compression codecs per file or partition</li>
 *   <li>Adjusting page sizes or row group sizes based on data characteristics</li>
 *   <li>Injecting custom metadata into Parquet files</li>
 * </ul>
 *
 * @since 1.2.0
 */
public interface HoodieParquetConfigInjector {

  /**
   * Injects custom configurations into the Parquet writer pipeline.
   * <p>
   * This method is invoked before creating the Parquet write support and writer objects, allowing
   * implementations to modify both the storage-level and Hudi-level configurations.
   *
   * @param path the file path where the Parquet file will be written
   * @param storageConf the storage configuration (e.g., Hadoop Configuration) that will be used by the writer
   * @param hoodieConfig the Hudi configuration containing write settings and table properties
   * @return a pair containing the potentially modified storage configuration and Hudi configuration.
   *         Both configurations will be used to create the Parquet writer.
   */
  Pair<StorageConfiguration, HoodieConfig> withProps(StoragePath path, StorageConfiguration storageConf, HoodieConfig hoodieConfig);
}
