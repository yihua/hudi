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

package org.apache.spark.sql.execution.datasources

import org.apache.hudi.storage.HoodieLocation

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.catalyst.InternalRow

/**
 * Utils on Spark [[PartitionedFile]] and [[PartitionDirectory]] for Spark 3.4.
 */
object HoodieSpark34PartitionedFileUtils extends HoodieSparkPartitionedFileUtils {
  override def getPathFromPartitionedFile(partitionedFile: PartitionedFile): HoodieLocation = {
    new HoodieLocation(partitionedFile.filePath.toPath.toUri)
  }

  override def getStringPathFromPartitionedFile(partitionedFile: PartitionedFile): String = {
    partitionedFile.filePath.toString
  }

  override def createPartitionedFile(partitionValues: InternalRow,
                                     location: HoodieLocation,
                                     start: Long,
                                     length: Long): PartitionedFile = {
    PartitionedFile(partitionValues, SparkPath.fromUri(location.toUri), start, length)
  }

  override def toFileStatuses(partitionDirs: Seq[PartitionDirectory]): Seq[FileStatus] = {
    partitionDirs.flatMap(_.files)
  }

  override def newPartitionDirectory(internalRow: InternalRow, statuses: Seq[FileStatus]): PartitionDirectory = {
    PartitionDirectory(internalRow, statuses)
  }
}
