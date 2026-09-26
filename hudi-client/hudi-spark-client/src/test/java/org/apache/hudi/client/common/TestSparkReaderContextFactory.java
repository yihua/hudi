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

package org.apache.hudi.client.common;

import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.client.utils.SparkInternalSchemaConverter;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.ActionType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantFileNameGeneratorV2;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantFileNameParserV2;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantGeneratorV2;
import org.apache.hudi.common.util.InternalSchemaHistory;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.fs.inline.InLineFileSystem;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.io.FileBasedInternalSchemaStorageManager;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.execution.datasources.SparkColumnarFileReader;
import org.apache.spark.sql.hudi.SparkAdapter;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestSparkReaderContextFactory extends HoodieClientTestBase {
  @Test
  void testGetSchemaEvolutionConfigurations() throws Exception {
    TableSchemaResolver schemaResolver = mock(TableSchemaResolver.class);
    HoodieTimeline timeline = mock(HoodieTimeline.class);
    InstantFileNameGenerator fileNameGenerator = new InstantFileNameGeneratorV2();
    // The table the harness created on storage; the schema history is loaded from it.
    HoodieTableMetaClient tableMetaClient = metaClient;
    metaClient = mock(HoodieTableMetaClient.class, RETURNS_DEEP_STUBS);
    when(metaClient.getBasePath()).thenReturn(tableMetaClient.getBasePath());
    when(metaClient.getMetaPath()).thenReturn(tableMetaClient.getMetaPath());
    when(metaClient.getTimelinePath()).thenReturn(tableMetaClient.getTimelinePath());
    when(metaClient.getStorage()).thenReturn(tableMetaClient.getStorage());
    when(metaClient.getInstantFileNameParser()).thenReturn(new InstantFileNameParserV2());
    when(metaClient.getCommitsAndCompactionTimeline().filterCompletedInstants()).thenReturn(timeline);
    when(metaClient.getTimelineLayout().getInstantFileNameGenerator()).thenReturn(fileNameGenerator);
    when(metaClient.getTableConfig()).thenReturn(new HoodieTableConfig());

    InstantGeneratorV2 instantGen = new InstantGeneratorV2();
    Types.RecordType record = Types.RecordType.get(Collections.singletonList(
        Types.Field.get(0, "col1", Types.BooleanType.get())));
    List<HoodieInstant> instants = Arrays.asList(
        instantGen.createNewInstant(
            HoodieInstant.State.COMPLETED, ActionType.deltacommit.name(), "0001", "0005"),
        instantGen.createNewInstant(
            HoodieInstant.State.COMPLETED, ActionType.deltacommit.name(), "0002", "0006"),
        instantGen.createNewInstant(
            HoodieInstant.State.COMPLETED, ActionType.compaction.name(), "0003", "0007"));
    InternalSchema internalSchema = new InternalSchema(record);
    // Schema history written by the commit "0002", one of the valid commits.
    StoragePath schemaHistoryFile = new StoragePath(new StoragePath(tableMetaClient.getMetaPath(),
        FileBasedInternalSchemaStorageManager.SCHEMA_NAME), "0002." + HoodieTimeline.SCHEMA_COMMIT_ACTION);
    try (OutputStream out = tableMetaClient.getStorage().create(schemaHistoryFile)) {
      out.write(SerDeHelper.inheritSchemas(new InternalSchema(2L, record), "").getBytes(StandardCharsets.UTF_8));
    }
    when(schemaResolver.getTableInternalSchemaFromCommitMetadata()).thenReturn(Option.of(internalSchema));
    when(timeline.getInstants()).thenReturn(instants);
    SparkAdapter sparkAdapter = mock(SparkAdapter.class);
    scala.collection.immutable.Map<String, String> options =
        scala.collection.immutable.Map$.MODULE$.<String, String>empty()
            .$plus(new Tuple2<>(FileFormat.OPTION_RETURNING_BATCH(), Boolean.toString(true)));
    ArgumentCaptor<Configuration> configurationArgumentCaptor = ArgumentCaptor.forClass(Configuration.class);
    SparkColumnarFileReader sparkParquetReader = mock(SparkColumnarFileReader.class);
    when(sparkAdapter.createParquetFileReader(eq(false), eq(context.getSqlContext().sparkSession().sessionState().conf()), eq(options), configurationArgumentCaptor.capture()))
        .thenReturn(sparkParquetReader);

    SparkReaderContextFactory sparkHoodieReaderContextFactory = new SparkReaderContextFactory(context, metaClient, schemaResolver, sparkAdapter);
    HoodieReaderContext<InternalRow> readerContext = sparkHoodieReaderContextFactory.getContext();

    Configuration createdConfig = readerContext.getStorageConfiguration().unwrapAs(Configuration.class);
    assertEquals(createdConfig, configurationArgumentCaptor.getValue());

    assertFalse(createdConfig.getBoolean(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED().key(), true));
    assertFalse(createdConfig.getBoolean(SQLConf.CASE_SENSITIVE().key(), true));
    assertFalse(createdConfig.getBoolean(SQLConf.PARQUET_BINARY_AS_STRING().key(), true));
    assertTrue(createdConfig.getBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP().key(), false));
    assertFalse(createdConfig.getBoolean("spark.sql.legacy.parquet.nanosAsLong", true));
    if (HoodieSparkUtils.gteqSpark3_4()) {
      assertFalse(createdConfig.getBoolean("spark.sql.parquet.inferTimestampNTZ.enabled", true));
    }

    String inlineClassName = createdConfig.get("fs." + InLineFileSystem.SCHEME + ".impl");
    assertEquals(InLineFileSystem.class.getName(), inlineClassName);

    // Internal write-side reads must pin CONTENT; a DESCRIPTOR leak here drops blob bytes (#19232).
    assertEquals(
        HoodieReaderConfig.BLOB_INLINE_READ_MODE_CONTENT,
        createdConfig.get(HoodieReaderConfig.BLOB_INLINE_READ_MODE.key()));

    // The schema history of the valid commits is shipped in the conf instead of the valid commits list.
    assertTrue(InternalSchemaHistory.isPresentIn(createdConfig::get));
    InternalSchema fileSchema = InternalSchemaHistory.resolve(createdConfig::get, 3L);
    assertEquals(2L, fileSchema.schemaId());
    assertEquals(Collections.singletonList("col1"), fileSchema.getAllColsFullName());
    assertEquals(
        tableMetaClient.getBasePath().toString(),
        createdConfig.get(SparkInternalSchemaConverter.HOODIE_TABLE_PATH));
  }
}
