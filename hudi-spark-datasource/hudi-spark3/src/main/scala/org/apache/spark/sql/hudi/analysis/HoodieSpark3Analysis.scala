/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.analysis

import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{ResolvedTable, UnresolvedPartitionSpec}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HoodieCatalogTable}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.connector.catalog.{Table, V1Table}
import org.apache.spark.sql.execution.datasources.PreWriteCheck.failAnalysis
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, V2SessionCatalog}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.{castIfNeeded, getTableLocation, removeMetaFields, tableExistsInPath}
import org.apache.spark.sql.hudi.catalog.HoodieCatalog
import org.apache.spark.sql.hudi.command.{AlterHoodieTableDropPartitionCommand, ShowHoodieTablePartitionsCommand, TruncateHoodieTableCommand}
import org.apache.spark.sql.hudi.{HoodieSqlCommonUtils, ProvidesHoodieConfig}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, SparkSession}

import scala.collection.JavaConverters.mapAsJavaMapConverter

/**
 * Rule for convert the logical plan to command.
 * @param sparkSession
 */
case class HoodieSpark3Analysis(sparkSession: SparkSession) extends Rule[LogicalPlan]
  with SparkAdapterSupport with ProvidesHoodieConfig {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {
//    // NOTE: This step is required since Hudi relations don't currently implement DS V2 Read API
//    case dsv2 @ DataSourceV2Relation(tbl: HoodieInternalV2Table, _, _, _, _) =>
//      val qualifiedTableName = QualifiedTableName(tbl.v1Table.database, tbl.v1Table.identifier.table)
//      val catalog = sparkSession.sessionState.catalog
//
//      val plan1: LogicalRelation = catalog.getCachedPlan(qualifiedTableName, () => {
//        val opts = buildHoodieConfig(tbl.hoodieCatalogTable)
//        val source = new DefaultSource()
//        val relation = source.createRelation(new SQLContext(sparkSession), opts, tbl.hoodieCatalogTable.tableSchema)
//
//        val output = dsv2.output
//        val catalogTable = tbl.catalogTable.map(_ => tbl.v1Table)
//
//        LogicalRelation(relation, output, catalogTable, isStreaming = false)
//      }).asInstanceOf[LogicalRelation]
//
//      plan1.copy(output = dsv2.output)

    case s @ InsertIntoStatement(r @ DataSourceV2Relation(HoodieV1Table(table), _, _, _, _), partitionSpec, _, _, _, _)
      if s.query.resolved && needsSchemaAdjustment(s.query, table, partitionSpec, r.schema) =>
        val projection = resolveQueryColumnsByOrdinal(s.query, r.output)
        if (projection != s.query) {
          s.copy(query = projection)
        } else {
          s
        }
  }

  /**
   * Need to adjust schema based on the query and relation schema, for example,
   * if using insert into xx select 1, 2 here need to map to column names
   */
  private def needsSchemaAdjustment(query: LogicalPlan,
                                    table: CatalogTable,
                                    partitionSpec: Map[String, Option[String]],
                                    schema: StructType): Boolean = {
    val output = query.output
    val queryOutputWithoutMetaFields = removeMetaFields(output)
    val hoodieCatalogTable = HoodieCatalogTable(sparkSession, table)

    val partitionFields = hoodieCatalogTable.partitionFields
    val partitionSchema = hoodieCatalogTable.partitionSchema
    val staticPartitionValues = partitionSpec.filter(p => p._2.isDefined).mapValues(_.get)

    assert(staticPartitionValues.isEmpty ||
      staticPartitionValues.size == partitionSchema.size,
      s"Required partition columns is: ${partitionSchema.json}, Current static partitions " +
        s"is: ${staticPartitionValues.mkString("," + "")}")

    assert(staticPartitionValues.size + queryOutputWithoutMetaFields.size
      == hoodieCatalogTable.tableSchemaWithoutMetaFields.size,
      s"Required select columns count: ${hoodieCatalogTable.tableSchemaWithoutMetaFields.size}, " +
        s"Current select columns(including static partition column) count: " +
        s"${staticPartitionValues.size + queryOutputWithoutMetaFields.size}，columns: " +
        s"(${(queryOutputWithoutMetaFields.map(_.name) ++ staticPartitionValues.keys).mkString(",")})")

    // static partition insert.
    if (staticPartitionValues.nonEmpty) {
      // drop partition fields in origin schema to align fields.
      schema.dropWhile(p => partitionFields.contains(p.name))
    }

    val existingSchemaOutput = output.take(schema.length)
    existingSchemaOutput.map(_.name) != schema.map(_.name) ||
      existingSchemaOutput.map(_.dataType) != schema.map(_.dataType)
  }

  private def resolveQueryColumnsByOrdinal(query: LogicalPlan,
                                           targetAttrs: Seq[Attribute]): LogicalPlan = {
    // always add a Cast. it will be removed in the optimizer if it is unnecessary.
    val project = query.output.zipWithIndex.map { case (attr, i) =>
      if (i < targetAttrs.length) {
        val targetAttr = targetAttrs(i)
        val castAttr = castIfNeeded(attr.withNullability(targetAttr.nullable), targetAttr.dataType, conf)
        Alias(castAttr, targetAttr.name)()
      } else {
        attr
      }
    }
    Project(project, query)
  }
}

/**
 * Rule for resolve hoodie's extended syntax or rewrite some logical plan.
 * @param sparkSession
 */
case class HoodieSpark3ResolveReferences(sparkSession: SparkSession) extends Rule[LogicalPlan]
  with SparkAdapterSupport with ProvidesHoodieConfig {

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsUp {
    // Fill schema for Create Table without specify schema info
    case c @ CreateV2Table(tableCatalog, tableName, schema, partitioning, properties, _)
      if sparkAdapter.isHoodieTable(properties.asJava) =>

      if (schema.isEmpty && partitioning.nonEmpty) {
        failAnalysis("It is not allowed to specify partition columns when the table schema is " +
          "not defined. When the table schema is not provided, schema and partition columns " +
          "will be inferred.")
      }
      val hoodieCatalog = tableCatalog match {
        case catalog: HoodieCatalog => catalog
        case _ => tableCatalog.asInstanceOf[V2SessionCatalog]
      }
      val tablePath = getTableLocation(properties,
        TableIdentifier(tableName.name(), tableName.namespace().lastOption), sparkSession)

      val tableExistInCatalog = hoodieCatalog.tableExists(tableName)
      // Only when the table has not exist in catalog, we need to fill the schema info for creating table.
      if (!tableExistInCatalog && tableExistsInPath(tablePath, sparkSession.sessionState.newHadoopConf())) {
        val metaClient = HoodieTableMetaClient.builder()
          .setBasePath(tablePath)
          .setConf(sparkSession.sessionState.newHadoopConf())
          .build()
        val tableSchema = HoodieSqlCommonUtils.getTableSqlSchema(metaClient)
        if (tableSchema.isDefined && schema.isEmpty) {
          // Fill the schema with the schema from the table
          c.copy(tableSchema = tableSchema.get)
        } else if (tableSchema.isDefined && schema != tableSchema.get) {
          throw new AnalysisException(s"Specified schema in create table statement is not equal to the table schema." +
            s"You should not specify the schema for an exist table: $tableName ")
        } else {
          c
        }
      } else {
        c
      }
    case p => p
  }
}

object HoodieV1Table extends SparkAdapterSupport {
  def unapply(table: Table): Option[CatalogTable] = table match {
    case V1Table(catalogTable) if sparkAdapter.isHoodieTable(catalogTable) => Some(catalogTable)
    case _ => None
  }
}

/**
 * Rule for rewrite some spark commands to hudi's implementation.
 * @param sparkSession
 */
case class HoodieSpark3PostAnalysisRule(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case ShowPartitions(ResolvedTable(_, id, HoodieV1Table(_), _), specOpt, _) =>
        ShowHoodieTablePartitionsCommand(
          id.asTableIdentifier, specOpt.map(s => s.asInstanceOf[UnresolvedPartitionSpec].spec))

      // Rewrite TruncateTableCommand to TruncateHoodieTableCommand
      case TruncateTable(ResolvedTable(_, id, HoodieV1Table(_), _)) =>
        TruncateHoodieTableCommand(id.asTableIdentifier, None)

      case TruncatePartition(ResolvedTable(_, id, HoodieV1Table(_), _), partitionSpec: UnresolvedPartitionSpec) =>
        TruncateHoodieTableCommand(id.asTableIdentifier, Some(partitionSpec.spec))

      case DropPartitions(ResolvedTable(_, id, HoodieV1Table(_), _), specs, ifExists, purge) =>
        AlterHoodieTableDropPartitionCommand(
          id.asTableIdentifier,
          specs.seq.map(f => f.asInstanceOf[UnresolvedPartitionSpec]).map(s => s.spec),
          ifExists,
          purge,
          retainData = true
        )

      case _ => plan
    }
  }
}
