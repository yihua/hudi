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

import org.apache.hudi.common.util.Option;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HoodiePositionDeleteBlock extends HoodieLogBlock {

  private int[] positionsToDelete;

  public HoodiePositionDeleteBlock(int[] positionsToDelete, Map<HeaderMetadataType, String> header) {
    this(Option.empty(), null, false, Option.empty(), header, new HashMap<>());
    this.positionsToDelete = positionsToDelete;
  }

  public HoodiePositionDeleteBlock(Option<byte[]> content, FSDataInputStream inputStream, boolean readBlockLazily,
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
    } else if (readBlockLazily && positionsToDelete == null) {
      // read block lazily
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(baos);
    // TODO(HUDI-5760) avoid using Kryo for serialization here
    output.writeInt(version);
    return baos.toByteArray();
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.POS_DELETE_BLOCK;
  }

}
