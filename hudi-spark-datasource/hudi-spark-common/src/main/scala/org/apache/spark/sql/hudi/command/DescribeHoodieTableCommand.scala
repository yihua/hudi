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

package org.apache.spark.sql.hudi.command

import org.apache.hudi.SparkAdapterSupport

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition, CatalogTableType, SessionCatalog}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.hudi.command.HoodieLeafRunnableCommand.append
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.PartitioningUtils

import scala.collection.mutable.ArrayBuffer

case class DescribeHoodieTableCommand(catalogTable: CatalogTable,
                                      tableIdentifier: TableIdentifier,
                                      partitionSpec: TablePartitionSpec,
                                      isExtended: Boolean,
                                      override val output: Seq[Attribute])
  extends HoodieLeafRunnableCommand with SparkAdapterSupport {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val result = new ArrayBuffer[Row]
    val catalog = sparkSession.sessionState.catalog

    if (catalog.isTempView(tableIdentifier)) {
      if (partitionSpec.nonEmpty) {
        throw QueryCompilationErrors.descPartitionNotAllowedOnTempView(tableIdentifier.identifier)
      }
      val schema = catalog.getTempViewOrPermanentTableMetadata(tableIdentifier).schema
      describeSchema(schema, result, header = false)
    } else {
      if (catalogTable.schema.isEmpty) {
        // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
        // inferred at runtime. We should still support it.
        describeSchema(sparkSession.table(catalogTable.identifier).schema, result, header = false)
      } else {
        describeSchema(catalogTable.schema, result, header = false)
      }

      describePartitionInfo(catalogTable, result)

      if (partitionSpec.nonEmpty) {
        // Outputs the partition-specific info for the DDL command:
        // "DESCRIBE [EXTENDED|FORMATTED] table_name PARTITION (partitionVal*)"
        describeDetailedPartitionInfo(sparkSession, catalog, catalogTable, result)
      } else if (isExtended) {
        describeFormattedTableInfo(catalogTable, result)
      }

      // If any columns have default values, append them to the result.
      ResolveDefaultColumns.getDescribeMetadata(catalogTable.schema).foreach { row =>
        append(result, row._1, row._2, row._3)
      }
    }

    result.toSeq
  }

  private def describePartitionInfo(table: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    if (table.partitionColumnNames.nonEmpty) {
      append(buffer, "# Partition Information", "", "")
      describeSchema(table.partitionSchema, buffer, header = true)
    }
  }

  private def describeFormattedTableInfo(table: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    // The following information has been already shown in the previous outputs
    val excludedTableInfo = Seq(
      "Partition Columns",
      "Schema"
    )
    append(buffer, "", "", "")
    append(buffer, "# Detailed Table Information", "", "")
    table.toLinkedHashMap.filterKeys(!excludedTableInfo.contains(_)).foreach {
      s => append(buffer, s._1, s._2, "")
    }
  }

  private def describeDetailedPartitionInfo(spark: SparkSession,
                                            catalog: SessionCatalog,
                                            metadata: CatalogTable,
                                            result: ArrayBuffer[Row]): Unit = {
    if (metadata.tableType == CatalogTableType.VIEW) {
      throw QueryCompilationErrors.descPartitionNotAllowedOnView(tableIdentifier.identifier)
    }
    DDLUtils.verifyPartitionProviderIsHive(spark, metadata, "DESC PARTITION")
    val normalizedPartSpec = PartitioningUtils.normalizePartitionSpec(
      partitionSpec,
      metadata.partitionSchema,
      tableIdentifier.quotedString,
      spark.sessionState.conf.resolver)
    val partition = catalog.getPartition(tableIdentifier, normalizedPartSpec)
    if (isExtended) describeFormattedDetailedPartitionInfo(tableIdentifier, metadata, partition, result)
  }

  private def describeFormattedDetailedPartitionInfo(tableIdentifier: TableIdentifier,
                                                     table: CatalogTable,
                                                     partition: CatalogTablePartition,
                                                     buffer: ArrayBuffer[Row]): Unit = {
    append(buffer, "", "", "")
    append(buffer, "# Detailed Partition Information", "", "")
    append(buffer, "Database", table.database, "")
    append(buffer, "Table", tableIdentifier.table, "")
    partition.toLinkedHashMap.foreach(s => append(buffer, s._1, s._2, ""))
    append(buffer, "", "", "")
    append(buffer, "# Storage Information", "", "")
    table.bucketSpec match {
      case Some(spec) =>
        spec.toLinkedHashMap.foreach(s => append(buffer, s._1, s._2, ""))
      case _ =>
    }
    table.storage.toLinkedHashMap.foreach(s => append(buffer, s._1, s._2, ""))
  }

  private def describeSchema(schema: StructType,
                             buffer: ArrayBuffer[Row],
                             header: Boolean): Unit = {
    if (header) {
      append(buffer, s"# ${output.head.name}", output(1).name, output(2).name)
    }
    schema.foreach { column =>
      append(buffer, column.name, column.dataType.simpleString, column.getComment().orNull)
    }
  }
}
