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

package org.apache.hudi.functional;

import org.apache.hudi.client.functional.TestSparkCopyOnWriteRowLineageForTestFormat;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;

import org.junit.jupiter.api.Tag;

/**
 * Runs {@link TestSparkCopyOnWriteRowLineageForTestFormat} with Spark records, which the merge reads through
 * the Spark parquet reader of the Spark version adapter.
 */
@Tag("functional")
class TestSparkRecordCopyOnWriteRowLineageForTestFormat extends TestSparkCopyOnWriteRowLineageForTestFormat {

  @Override
  protected HoodieRecordType getRecordType() {
    return HoodieRecordType.SPARK;
  }
}
