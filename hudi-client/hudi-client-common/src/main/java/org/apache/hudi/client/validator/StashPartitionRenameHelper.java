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

package org.apache.hudi.client.validator;

import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import java.io.IOException;
import java.io.Serializable;

/**
 * Interface for moving partition files from a source path to a target stash path.
 * <p>
 * This is used by {@link StashPartitionsPreCommitValidator} to move data files
 * to a stash location before a deletePartitions commit completes.
 * <p>
 * Implementations must be idempotent: if some files already exist in the target
 * from a prior partial attempt, only the remaining files should be moved.
 * <p>
 * The default implementation ({@link DefaultStashPartitionRenameHelper}) copies
 * files individually and then deletes from source. Custom implementations can
 * leverage efficient filesystem-level rename/move APIs where available.
 */
public interface StashPartitionRenameHelper extends Serializable {

  /**
   * Move all files from the source partition path to the target stash path.
   * <p>
   * Must be idempotent — if files already exist in target from a prior partial
   * attempt, only move files not yet present in target.
   *
   * @param storage    the storage instance to use for file operations
   * @param sourcePath the source partition directory (e.g., basepath/datestr=2023-01-01)
   * @param targetPath the target stash directory (e.g., stashpath/datestr=2023-01-01)
   * @throws IOException if any file operation fails
   */
  void movePartitionFiles(HoodieStorage storage, StoragePath sourcePath, StoragePath targetPath) throws IOException;
}
