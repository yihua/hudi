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

package org.apache.hudi.config;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.LockConfiguration;

/**
 * Hoodie Configs for Azure based storage locks.
 */
@ConfigClassProperty(name = "Azure based Locks Configurations",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    subGroupName = ConfigGroups.SubGroupNames.LOCK,
    description = "Configs that control Azure Blob/ADLS based locking mechanisms "
        + "required for concurrency control between writers to a Hudi table.")
public class AzureStorageLockConfig extends HoodieConfig {

  private static final String AZURE_BASED_LOCK_PROPERTY_PREFIX = LockConfiguration.LOCK_PREFIX + "azure.";

  public static final ConfigProperty<String> AZURE_CONNECTION_STRING = ConfigProperty
      .key(AZURE_BASED_LOCK_PROPERTY_PREFIX + "connection.string")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("For Azure based lock provider, optional Azure Storage connection string used "
          + "for authenticating BlobServiceClient.");

  public static final ConfigProperty<String> AZURE_SAS_TOKEN = ConfigProperty
      .key(AZURE_BASED_LOCK_PROPERTY_PREFIX + "sas.token")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("For Azure based lock provider, optional SAS token used for "
          + "authenticating BlobServiceClient when connection string is not provided.");
}
