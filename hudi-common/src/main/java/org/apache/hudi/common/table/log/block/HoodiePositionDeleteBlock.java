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

import org.apache.hudi.common.fs.SizeAwareDataInputStream;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
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
      getPositionsToDelete();
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(baos);
    // TODO(HUDI-5760) avoid using Kryo for serialization here
    byte[] bytesToWrite = positionsToBytes();
    output.writeInt(version);
    output.writeInt(bytesToWrite.length);
    output.write(bytesToWrite);
    return baos.toByteArray();
  }

  public int[] getPositionsToDelete() {
    try {
      if (positionsToDelete == null) {
        if (!getContent().isPresent() && readBlockLazily) {
          // read content from disk
          inflate();
        }
        SizeAwareDataInputStream dis =
            new SizeAwareDataInputStream(new DataInputStream(new ByteArrayInputStream(getContent().get())));
        int version = dis.readInt();
        int dataLength = dis.readInt();
        byte[] data = new byte[dataLength];
        dis.readFully(data);
        positionsToDelete = new int[dataLength / 4];
        int dataPos = 0;
        for (int i = 0; i < positionsToDelete.length; i++) {
          int ch1 = (data[dataPos++] & 0xFF);
          int ch2 = (data[dataPos++] & 0xFF);
          int ch3 = (data[dataPos++] & 0xFF);
          int ch4 = (data[dataPos++] & 0xFF);
          if ((ch1 | ch2 | ch3 | ch4) < 0) {
            throw new EOFException();
          }
          positionsToDelete[i] = ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + ch4);
        }
        deflate();
      }
      return positionsToDelete;
    } catch (IOException io) {
      throw new HoodieIOException("Unable to generate keys to delete from block content", io);
    }
  }

  private byte[] positionsToBytes() {
    byte[] result = new byte[positionsToDelete.length * 4];
    int dataPos = 0;
    for (int pos : positionsToDelete) {
      result[dataPos++] = (byte) ((pos >>> 24) & 0xFF);
      result[dataPos++] = (byte) ((pos >>> 16) & 0xFF);
      result[dataPos++] = (byte) ((pos >>> 8) & 0xFF);
      result[dataPos++] = (byte) (pos & 0xFF);
    }
    return result;
  }

  @Override
  public HoodieLogBlockType getBlockType() {
    return HoodieLogBlockType.POS_DELETE_BLOCK;
  }

}
