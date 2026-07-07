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

package org.apache.spark.sql.hudi.command.index

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{CreateIndex, DropIndex, HoodieShowIndexes, RefreshIndex}
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

/**
 * Coverage for the Hudi-specific surface of the extended SQL parser: index DDL
 * (CREATE / DROP / SHOW / REFRESH INDEX) and Hudi column types (BLOB, VECTOR).
 *
 * Index DDL is asserted on the parsed logical plan directly, since those plan
 * classes are Hudi-owned and stable across Spark versions. Column types are
 * asserted through the catalog schema, which stays version-agnostic even though
 * the CreateTable plan fields differ between Spark 3.5 and 4.x.
 */
class TestExtendedSqlParserCoverage extends HoodieSparkSqlTestBase {

  private def parse(sql: String) = spark.sessionState.sqlParser.parsePlan(sql)

  private def tableName(plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan): Seq[String] =
    plan.children.head.asInstanceOf[UnresolvedRelation].multipartIdentifier

  test("Test parse CREATE INDEX into a CreateIndex plan") {
    val tableName = generateTableName
    spark.sql(
      s"""
         |create table $tableName (id int, name string) using hudi
         |tblproperties(primaryKey = 'id')
         |""".stripMargin)

    val plan = parse(s"create index idx_name on $tableName using bloom (name)")
    assertResult(classOf[CreateIndex].getName)(plan.getClass.getName)
    val createIndex = plan.asInstanceOf[CreateIndex]
    assertResult("idx_name")(createIndex.indexName)
    assertResult("bloom")(createIndex.indexType)
    assert(this.tableName(createIndex).last.equalsIgnoreCase(tableName))
  }

  test("Test parse DROP INDEX into a DropIndex plan") {
    val tableName = generateTableName
    spark.sql(
      s"""
         |create table $tableName (id int, name string) using hudi
         |tblproperties(primaryKey = 'id')
         |""".stripMargin)

    val plan = parse(s"drop index idx_name on $tableName")
    assertResult(classOf[DropIndex].getName)(plan.getClass.getName)
    val dropIndex = plan.asInstanceOf[DropIndex]
    assertResult("idx_name")(dropIndex.indexName)
    assert(this.tableName(dropIndex).last.equalsIgnoreCase(tableName))
  }

  test("Test parse SHOW INDEXES into a HoodieShowIndexes plan") {
    val tableName = generateTableName
    spark.sql(
      s"""
         |create table $tableName (id int, name string) using hudi
         |tblproperties(primaryKey = 'id')
         |""".stripMargin)

    val plan = parse(s"show indexes from $tableName")
    assertResult(classOf[HoodieShowIndexes].getName)(plan.getClass.getName)
    assert(this.tableName(plan).last.equalsIgnoreCase(tableName))
  }

  test("Test parse REFRESH INDEX into a RefreshIndex plan") {
    val tableName = generateTableName
    spark.sql(
      s"""
         |create table $tableName (id int, name string) using hudi
         |tblproperties(primaryKey = 'id')
         |""".stripMargin)

    val plan = parse(s"refresh index idx_name on $tableName")
    assertResult(classOf[RefreshIndex].getName)(plan.getClass.getName)
    val refreshIndex = plan.asInstanceOf[RefreshIndex]
    assertResult("idx_name")(refreshIndex.indexName)
    assert(this.tableName(refreshIndex).last.equalsIgnoreCase(tableName))
  }

  test("Test parse a BLOB column type into the catalog schema") {
    val tableName = generateTableName
    spark.sql(
      s"""
         |create table $tableName (id int, b blob) using hudi
         |tblproperties(primaryKey = 'id')
         |""".stripMargin)

    val schema = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName)).schema
    assert(schema.exists(_.name == "b"))
  }

  test("Test parse a VECTOR column type into the catalog schema") {
    val tableName = generateTableName
    spark.sql(
      s"""
         |create table $tableName (id int, v vector(3)) using hudi
         |tblproperties(primaryKey = 'id')
         |""".stripMargin)

    val schema = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName)).schema
    assert(schema.exists(_.name == "v"))
  }
}
