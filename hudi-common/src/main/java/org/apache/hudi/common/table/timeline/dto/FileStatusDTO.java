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

package org.apache.hudi.common.table.timeline.dto;

import org.apache.hudi.io.storage.HoodieFileStatus;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The data transfer object of file status.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FileStatusDTO {

  @JsonProperty("path")
  FilePathDTO path;
  @JsonProperty("length")
  long length;
  @JsonProperty("isdir")
  boolean isdir;
  @JsonProperty("modificationTime")
  long modificationTime;

  public static FileStatusDTO fromFileStatus(HoodieFileStatus fileStatus) {
    if (null == fileStatus) {
      return null;
    }

    FileStatusDTO dto = new FileStatusDTO();
    dto.path = FilePathDTO.fromHoodieLocation(fileStatus.getLocation());
    dto.length = fileStatus.getLength();
    dto.isdir = fileStatus.isDirectory();
    dto.modificationTime = fileStatus.getModificationTime();

    return dto;
  }

  public static HoodieFileStatus toHoodieFileStatus(FileStatusDTO dto) {
    return new HoodieFileStatus(
        FilePathDTO.toHoodieLocation(dto.path), dto.length, dto.isdir, dto.modificationTime);
  }
}
