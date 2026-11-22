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

package org.apache.spark.sql.hudi.dml.others

import org.apache.hudi.DataSourceWriteOptions.MERGE_INTO_CARDINALITY_CHECK
import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.HoodieMultipleMatchesException

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

/**
 * Spark SQL MERGE INTO statements where more than one source row matches the same target row.
 */
class TestMergeIntoCardinalityCheck extends HoodieSparkSqlTestBase {

  private val cardinalityError =
    "MERGE statement matched a single row from the target table with multiple rows of the source table"

  // (table type, update-as-delete-insert write mode, primary key, ordering field)
  Seq(
    ("cow", false, true, false),
    ("cow", false, true, true),
    ("mor", false, true, false),
    ("mor", false, true, true),
    ("mor", true, true, false),
    ("cow", false, false, false),
    ("mor", false, false, false)).foreach { case (tableType, updatesAsDeletesAndInserts, keyed, ordered) =>
    test(s"Test MERGE INTO with more than one source row for a target row ($tableType, "
      + s"update-as-delete-insert $updatesAsDeletesAndInserts, primary key $keyed, ordering field $ordered)") {
      withTempDir { tmp =>
        val tableName = generateTableName
        withSQLConf(HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS.key() -> updatesAsDeletesAndInserts.toString) {
          val keyProperties = (if (keyed) Seq("primaryKey = 'id'") else Seq.empty) ++
            (if (ordered) Seq("preCombineField = 'ts'") else Seq("'hoodie.record.merge.mode' = 'COMMIT_TIME_ORDERING'"))
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  ts int
               |) using hudi
               | location '${tmp.getCanonicalPath}/$tableName'
               | tblproperties (
               |  type = '$tableType',
               |  ${keyProperties.mkString(",\n  ")}
               | )
             """.stripMargin)
          spark.sql(s"insert into $tableName values (1, 'a1', 1000), (6, 'a6', 1000)")

          // Rows 1 and 2 of the source match target row 1
          val source = "select 1 as id, 'b1' as name, 1001 as ts union all select 1, 'c1', 1002 union all select 2, 'a2', 1000"
          assertCardinalityViolation(
            s"""
               |merge into $tableName t using ($source) s on t.id = s.id
               |when matched then update set *
               |when not matched then insert *
             """.stripMargin)
          // A matched clause that does not apply to the target row does not lift the check. A merge-on-read
          // table without a record key refuses a target field in a matched condition, so it takes the source field
          val matchedCondition = if (tableType == "mor" && !keyed && !updatesAsDeletesAndInserts) "s.id = 6" else "t.id = 6"
          assertCardinalityViolation(
            s"""
               |merge into $tableName t using ($source) s on t.id = s.id
               |when matched and $matchedCondition then update set *
               |when not matched then insert *
             """.stripMargin)
          checkAnswer(s"select id, name, ts from $tableName order by id")(Seq(1, "a1", 1000), Seq(6, "a6", 1000))

          if (keyed) {
            // Not-matched clauses only, or a single unconditional delete, have no cardinality check
            spark.sql(s"merge into $tableName t using ($source) s on t.id = s.id when not matched then insert *")
            checkAnswer(s"select id, name, ts from $tableName order by id")(
              Seq(1, "a1", 1000), Seq(2, "a2", 1000), Seq(6, "a6", 1000))
            spark.sql(s"merge into $tableName t using ($source) s on t.id = s.id when matched then delete")
            checkAnswer(s"select id, name, ts from $tableName order by id")(Seq(6, "a6", 1000))
          }

          // Without the check, the source rows are combined into one update
          withSQLConf(MERGE_INTO_CARDINALITY_CHECK.key -> "false") {
            spark.sql(
              s"""
                 |merge into $tableName t
                 |using (select 6 as id, 'b6' as name, 1001 as ts union all select 6, 'b6', 1001) s on t.id = s.id
                 |when matched then update set *
               """.stripMargin)
          }
          checkAnswer(s"select id, name, ts from $tableName where id = 6")(Seq(6, "b6", 1001))
        }
      }
    }
  }

  /**
   * Asserts that the statement fails the way Spark fails a MERGE INTO on its own tables: with a
   * MERGE_CARDINALITY_VIOLATION error from Spark 3.5 on, as the cause of the exception before Spark 4.
   */
  private def assertCardinalityViolation(sql: String): Unit = {
    val e = intercept[Throwable](spark.sql(sql))
    if (HoodieSparkUtils.gteqSpark3_5) {
      val violation = if (HoodieSparkUtils.gteqSpark4_0) e else e.getCause
      assert(violation.isInstanceOf[SparkRuntimeException], e)
      assertResult("MERGE_CARDINALITY_VIOLATION")(violation.asInstanceOf[SparkRuntimeException].getErrorClass)
      assert(violation.getMessage.contains(cardinalityError + "."), violation.getMessage)
      assert(violation.getSuppressed.exists(_.isInstanceOf[HoodieMultipleMatchesException]), e)
    } else {
      assert(e.isInstanceOf[HoodieMultipleMatchesException], e)
    }
    assert(e.getMessage.contains(cardinalityError), e.getMessage)
  }
}
