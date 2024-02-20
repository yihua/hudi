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

package org.apache.hudi.common.model;

import org.apache.hudi.storage.HoodieFileStatus;
import org.apache.hudi.storage.HoodieLocation;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieLogFile {
  private final String pathStr = "file:///tmp/hoodie/2021/01/01/.136281f3-c24e-423b-a65a-95dbfbddce1d_100.log.2_1-0-1";
  private final String fileId = "136281f3-c24e-423b-a65a-95dbfbddce1d";
  private final String baseCommitTime = "100";
  private final int logVersion = 2;
  private final String writeToken = "1-0-1";
  private final String fileExtension = "log";

  private final int length = 10;
  private final long blockSize = 1000000L;

  @Test
  void createFromLogFile() {
    HoodieFileStatus fileInfo = new HoodieFileStatus(new HoodieLocation(pathStr), length, blockSize, false, 0);
    HoodieLogFile hoodieLogFile = new HoodieLogFile(fileInfo);
    assertFileGetters(fileInfo, new HoodieLogFile(hoodieLogFile), length);
  }

  @Test
  void createFromFileStatus() {
    HoodieFileStatus fileInfo = new HoodieFileStatus(new HoodieLocation(pathStr), length, blockSize, false, 0);
    HoodieLogFile hoodieLogFile = new HoodieLogFile(fileInfo);
    assertFileGetters(fileInfo, hoodieLogFile, length);
  }

  @Test
  void createFromPath() {
    HoodieLogFile hoodieLogFile = new HoodieLogFile(new HoodieLocation(pathStr));
    assertFileGetters(null, hoodieLogFile, -1);
  }

  @Test
  void createFromPathAndLength() {
    HoodieLogFile hoodieLogFile = new HoodieLogFile(new HoodieLocation(pathStr), length);
    assertFileGetters(null, hoodieLogFile, length);
  }

  @Test
  void createFromString() {
    HoodieLogFile hoodieLogFile = new HoodieLogFile(pathStr);
    assertFileGetters(null, hoodieLogFile, -1);
  }

  @Test
  void createFromStringWithSuffix() {
    String suffix = ".cdc";
    String pathWithSuffix = pathStr + suffix;
    HoodieLogFile hoodieLogFile = new HoodieLogFile(pathWithSuffix);
    assertFileGetters(pathWithSuffix, null, hoodieLogFile, -1, suffix);
  }

  private void assertFileGetters(HoodieFileStatus fileInfo, HoodieLogFile hoodieLogFile,
                                 long fileLength) {
    assertFileGetters(pathStr, fileInfo, hoodieLogFile, fileLength, "");
  }

  private void assertFileGetters(String pathStr, HoodieFileStatus fileInfo,
                                 HoodieLogFile hoodieLogFile,
                                 long fileLength, String suffix) {
    assertEquals(fileId, hoodieLogFile.getFileId());
    assertEquals(baseCommitTime, hoodieLogFile.getDeltaCommitTime());
    assertEquals(logVersion, hoodieLogFile.getLogVersion());
    assertEquals(writeToken, hoodieLogFile.getLogWriteToken());
    assertEquals(fileExtension, hoodieLogFile.getFileExtension());
    assertEquals(new HoodieLocation(pathStr), hoodieLogFile.getLocation());
    assertEquals(fileLength, hoodieLogFile.getFileSize());
    assertEquals(fileInfo, hoodieLogFile.getFileInfo());
    assertEquals(suffix, hoodieLogFile.getSuffix());
  }
}
