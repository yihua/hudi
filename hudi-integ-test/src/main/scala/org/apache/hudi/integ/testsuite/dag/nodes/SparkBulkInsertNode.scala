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

package org.apache.hudi.integ.testsuite.dag.nodes

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config

/**
 * Spark datasource based bulk insert node
 *
 * @param dagNodeConfig DAG node configurations.
 */
class SparkBulkInsertNode(dagNodeConfig: Config) extends SparkInsertNode(dagNodeConfig) {

  override def getOperation(): String = {
    DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL
  }
}
