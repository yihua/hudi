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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.HoodieLocation;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;

import java.io.IOException;

public class HoodieAvroFileReaderFactory extends HoodieFileReaderFactory {
  protected HoodieFileReader newParquetFileReader(Configuration conf, HoodieLocation location) {
    return new HoodieAvroParquetReader(conf, location);
  }

  protected HoodieFileReader newHFileFileReader(boolean useNativeHFileReader,
                                                Configuration conf,
                                                HoodieLocation location,
                                                Option<Schema> schemaOption) throws IOException {
    if (useNativeHFileReader) {
      return new HoodieNativeAvroHFileReader(conf, location, schemaOption);
    }
    CacheConfig cacheConfig = new CacheConfig(conf);
    if (schemaOption.isPresent()) {
      return new HoodieHBaseAvroHFileReader(conf, location, cacheConfig, HoodieStorageUtils.getHoodieStorage(location, conf), schemaOption);
    }
    return new HoodieHBaseAvroHFileReader(conf, location, cacheConfig);
  }

  protected HoodieFileReader newHFileFileReader(boolean useNativeHFileReader,
                                                Configuration conf,
                                                HoodieLocation location,
                                                HoodieStorage storage,
                                                byte[] content,
                                                Option<Schema> schemaOption)
      throws IOException {
    if (useNativeHFileReader) {
      return new HoodieNativeAvroHFileReader(conf, content, schemaOption);
    }
    CacheConfig cacheConfig = new CacheConfig(conf);
    return new HoodieHBaseAvroHFileReader(conf, location, cacheConfig, storage, content, schemaOption);
  }

  @Override
  protected HoodieFileReader newOrcFileReader(Configuration conf, HoodieLocation location) {
    return new HoodieAvroOrcReader(conf, location);
  }

  @Override
  public HoodieFileReader newBootstrapFileReader(HoodieFileReader skeletonFileReader, HoodieFileReader dataFileReader, Option<String[]> partitionFields, Object[] partitionValues) {
    return new HoodieAvroBootstrapFileReader(skeletonFileReader, dataFileReader, partitionFields, partitionValues);
  }
}
