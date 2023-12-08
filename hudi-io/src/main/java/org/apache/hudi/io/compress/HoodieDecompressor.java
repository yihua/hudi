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

package org.apache.hudi.io.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Provides decompression on input data.
 */
public interface HoodieDecompressor {
  /**
   * Decompresses the data from {@link InputStream} and writes the decompressed data to the target.
   * {@link OutputStream}.
   *
   * @param compressedInput Compressed data in {@link InputStream}.
   * @param targetByteArray Target byte array to store the decompressed data.
   * @param offset          Offset in the target byte array to start to write data.
   * @param length          Maximum amount of decompressed data to write.
   * @throws IOException upon error.
   */
  void decompress(InputStream compressedInput, byte[] targetByteArray, int offset, int length) throws IOException;
}
