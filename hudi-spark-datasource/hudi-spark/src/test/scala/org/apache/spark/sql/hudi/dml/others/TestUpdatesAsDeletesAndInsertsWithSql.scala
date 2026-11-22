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

package org.apache.spark.sql.hudi.dml.others

import org.apache.hudi.DataSourceWriteOptions.{ENABLE_MERGE_INTO_PARTIAL_UPDATES, SPARK_SQL_OPTIMIZED_WRITES}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.table.TableSchemaResolver
import org.apache.hudi.common.table.log.HoodieLogFormat
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.common.util.ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.core.io.storage.HoodieIOFactory
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.table.action.commit.BaseWriteHelper

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase.getMetaClientAndFileSystemView
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Spark SQL DML on merge-on-read tables with [[HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS]]
 * enabled: every update and delete must reach the log as a positional delete.
 */
class TestUpdatesAsDeletesAndInsertsWithSql extends HoodieSparkSqlTestBase {

  Seq("SIMPLE", "GLOBAL_SIMPLE", "RECORD_INDEX").foreach { indexType =>
    test(s"Test MERGE INTO writing updates as deletes and inserts with $indexType index") {
      withTable(indexType) { (tableName, tablePath) =>
        // Every source row matches and there is no not-matched clause
        spark.sql(
          s"""
             |merge into $tableName t
             |using (select 1 as id, 'a1m' as name, 11.0 as price, 'p1' as dt
             |  union all select 2, 'a2m', 21.0, 'p1' union all select 3, 'a3m', 31.0, 'p1') s
             |on t.id = s.id
             |when matched then update set *
           """.stripMargin)
        checkAnswer(s"select id, name, price, dt from $tableName order by id")(
          Seq(1, "a1m", 11.0, "p1"), Seq(2, "a2m", 21.0, "p1"), Seq(3, "a3m", 31.0, "p1"),
          Seq(4, "a4", 40.0, "p2"), Seq(5, "a5", 50.0, "p2"))
        assertEquals(Seq("1", "2", "3"), positionalDeletes(tablePath))

        spark.sql(
          s"""
             |merge into $tableName t
             |using (select 1 as id, 'x' as name, 0.0 as price, 'p1' as dt, 'del' as op
             |  union all select 2, 'a2u', 22.0, 'p1', 'upd'
             |  union all select 4, 'a4u', 44.0, 'p2', 'none'
             |  union all select 6, 'a6', 60.0, 'p2', 'ins'
             |  union all select 7, 'a7', 70.0, 'p2', 'none') s
             |on t.id = s.id
             |when matched and s.op = 'del' then delete
             |when matched and s.op = 'upd' then update set id = s.id, name = s.name, price = s.price, dt = s.dt
             |when not matched and s.op = 'ins' then insert (id, name, price, dt) values (s.id, s.name, s.price, s.dt)
           """.stripMargin)
        checkAnswer(s"select id, name, price, dt from $tableName order by id")(
          Seq(2, "a2u", 22.0, "p1"), Seq(3, "a3m", 31.0, "p1"), Seq(4, "a4", 40.0, "p2"),
          Seq(5, "a5", 50.0, "p2"), Seq(6, "a6", 60.0, "p2"))
        assertEquals(Seq("1", "1", "2", "2", "3"), positionalDeletes(tablePath))
      }
    }
  }

  test("Test MERGE INTO without an update clause writing updates as deletes and inserts") {
    withTable { (tableName, tablePath) =>
      // Delete only: id 2 fails the condition and id 9 has no match
      spark.sql(
        s"""
           |merge into $tableName t
           |using (select 1 as id, 'p1' as dt, 'del' as op union all select 2, 'p1', 'none' union all select 9, 'p1', 'del') s
           |on t.id = s.id
           |when matched and s.op = 'del' then delete
         """.stripMargin)
      checkAnswer(s"select id, name, price, dt from $tableName order by id")(
        Seq(2, "a2", 20.0, "p1"), Seq(3, "a3", 30.0, "p1"), Seq(4, "a4", 40.0, "p2"), Seq(5, "a5", 50.0, "p2"))
      assertEquals(Seq("1"), positionalDeletes(tablePath))

      // Insert only: the matched id 2 is neither inserted again nor deleted
      spark.sql(
        s"""
           |merge into $tableName t
           |using (select 2 as id, 'a2m' as name, 21.0 as price, 'p1' as dt union all select 6, 'a6', 60.0, 'p2') s
           |on t.id = s.id
           |when not matched then insert *
         """.stripMargin)
      checkAnswer(s"select id, name, price, dt from $tableName order by id")(
        Seq(2, "a2", 20.0, "p1"), Seq(3, "a3", 30.0, "p1"), Seq(4, "a4", 40.0, "p2"), Seq(5, "a5", 50.0, "p2"),
        Seq(6, "a6", 60.0, "p2"))
      assertEquals(Seq("1"), positionalDeletes(tablePath))

      // Delete and insert
      spark.sql(
        s"""
           |merge into $tableName t
           |using (select 4 as id, 'a4m' as name, 41.0 as price, 'p2' as dt union all select 7, 'a7', 70.0, 'p1') s
           |on t.id = s.id
           |when matched then delete
           |when not matched then insert *
         """.stripMargin)
      checkAnswer(s"select id, name, price, dt from $tableName order by id")(
        Seq(2, "a2", 20.0, "p1"), Seq(3, "a3", 30.0, "p1"), Seq(5, "a5", 50.0, "p2"), Seq(6, "a6", 60.0, "p2"),
        Seq(7, "a7", 70.0, "p1"))
      assertEquals(Seq("1", "4"), positionalDeletes(tablePath))
    }
  }

  /**
   * A MERGE INTO whose matched clauses need the target record, which the split of an update reads: the statement
   * with its SQL confs, then the rows and the keys of the positional deletes it leaves.
   */
  private case class TargetRecordCase(name: String,
                                      statement: String => String,
                                      rows: Seq[Seq[Any]],
                                      deletedKeys: Seq[String],
                                      conf: Seq[(String, String)] = Seq.empty)

  private val targetConditionCases = Seq(
    // id 2 fails both conditions and id 6 has no match
    TargetRecordCase("update and delete conditions on target fields with an insert",
      tableName =>
        s"""
           |merge into $tableName t
           |using (select 1 as id, 'a1m' as name, 11.0 as price, 'p1' as dt union all select 2, 'a2m', 21.0, 'p1'
           |  union all select 3, 'a3m', 31.0, 'p1' union all select 5, 'a5m', 51.0, 'p2' union all select 6, 'a6', 60.0, 'p2') s
           |on t.id = s.id
           |when matched and t.price < 20 then update set *
           |when matched and t.price >= 30 then delete
           |when not matched then insert *
         """.stripMargin,
      Seq(Seq(1, "a1m", 11.0, "p1"), Seq(2, "a2", 20.0, "p1"), Seq(4, "a4", 40.0, "p2"), Seq(6, "a6", 60.0, "p2")),
      Seq("1", "3", "5")),
    // id 2 fails the condition and id 9 has no match
    TargetRecordCase("a delete condition on a target field without an update clause",
      tableName =>
        s"""
           |merge into $tableName t
           |using (select 2 as id, 'p1' as dt union all select 3, 'p1' union all select 9, 'p1') s
           |on t.id = s.id
           |when matched and t.price > 25 then delete
         """.stripMargin,
      Seq(Seq(1, "a1", 10.0, "p1"), Seq(2, "a2", 20.0, "p1"), Seq(4, "a4", 40.0, "p2"), Seq(5, "a5", 50.0, "p2")),
      Seq("3")))

  private val targetAssignmentCases = Seq(
    TargetRecordCase("an update that leaves out columns",
      tableName =>
        s"""
           |merge into $tableName t
           |using (select 1 as id, 12.0 as price, 'p1' as dt union all select 4, 42.0, 'p2') s
           |on t.id = s.id
           |when matched then update set price = s.price
         """.stripMargin,
      Seq(Seq(1, "a1", 12.0, "p1"), Seq(2, "a2", 20.0, "p1"), Seq(3, "a3", 30.0, "p1"), Seq(4, "a4", 42.0, "p2"),
        Seq(5, "a5", 50.0, "p2")),
      Seq("1", "4")),
    TargetRecordCase("an update that leaves out columns with partial updates off",
      tableName =>
        s"""
           |merge into $tableName t
           |using (select 2 as id, 'a2m' as name, 'p1' as dt union all select 5, 'a5m', 'p2') s
           |on t.id = s.id
           |when matched then update set id = s.id, name = s.name
         """.stripMargin,
      Seq(Seq(1, "a1", 10.0, "p1"), Seq(2, "a2m", 20.0, "p1"), Seq(3, "a3", 30.0, "p1"), Seq(4, "a4", 40.0, "p2"),
        Seq(5, "a5m", 50.0, "p2")),
      Seq("2", "5"),
      Seq(ENABLE_MERGE_INTO_PARTIAL_UPDATES.key -> "false")),
    TargetRecordCase("an update that assigns from target fields",
      tableName =>
        s"""
           |merge into $tableName t
           |using (select 3 as id, 1.5 as price, 'p1' as dt) s
           |on t.id = s.id
           |when matched then update set price = t.price + s.price, name = concat(t.name, '-', t.dt)
         """.stripMargin,
      Seq(Seq(1, "a1", 10.0, "p1"), Seq(2, "a2", 20.0, "p1"), Seq(3, "a3-p1", 31.5, "p1"), Seq(4, "a4", 40.0, "p2"),
        Seq(5, "a5", 50.0, "p2")),
      Seq("3")))

  // A global index can merge a source record with the record it matches while it tags, which leaves a delete to the split
  Seq(("SIMPLE", true, targetConditionCases ++ targetAssignmentCases),
    ("SIMPLE", false, targetConditionCases ++ targetAssignmentCases.take(1)),
    ("GLOBAL_SIMPLE", true, targetConditionCases.take(1)),
    ("RECORD_INDEX", true, targetConditionCases.take(1))).foreach { case (indexType, partitioned, cases) =>
    cases.foreach { mergeCase =>
      test(s"Test MERGE INTO with ${mergeCase.name} writing updates as deletes and inserts with $indexType index"
        + s" on a ${if (partitioned) "partitioned" else "non-partitioned"} table") {
        withTable(indexType, partitioned) { (tableName, tablePath) =>
          withSQLConf(mergeCase.conf: _*) {
            spark.sql(mergeCase.statement(tableName))
          }
          checkAnswer(s"select id, name, price, dt from $tableName order by id")(mergeCase.rows: _*)
          assertEquals(mergeCase.deletedKeys, positionalDeletes(tablePath, partitioned))

          withSQLConf(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key -> "1") {
            spark.sql(s"call run_compaction(op => 'run', table => '$tableName')")
          }
          assertEquals(1, HoodieTestUtils.createMetaClient(HoodieTestUtils.getDefaultStorageConf, tablePath)
            .getActiveTimeline.getCommitTimeline.filterCompletedInstants.countInstants)
          checkAnswer(s"select id, name, price, dt from $tableName order by id")(mergeCase.rows: _*)
        }
      }
    }
  }

  test("Test MERGE INTO of a deleted record writing updates as deletes and inserts") {
    withTable { (tableName, tablePath) =>
      spark.sql(s"delete from $tableName where id = 1")
      // The index still locates the deleted copy of id 1 in its base file, which is no match
      spark.sql(
        s"""
           |merge into $tableName t
           |using (select 1 as id, 'a1m' as name, 11.0 as price, 'p1' as dt) s
           |on t.id = s.id
           |when matched then update set *
         """.stripMargin)
      checkAnswer(s"select id, name, price, dt from $tableName order by id")(
        Seq(2, "a2", 20.0, "p1"), Seq(3, "a3", 30.0, "p1"), Seq(4, "a4", 40.0, "p2"), Seq(5, "a5", 50.0, "p2"))
      assertEquals(Seq("1"), positionalDeletes(tablePath))

      spark.sql(
        s"""
           |merge into $tableName t
           |using (select 1 as id, 'a1n' as name, 12.0 as price, 'p1' as dt) s
           |on t.id = s.id
           |when matched then update set id = s.id, name = 'matched', price = s.price, dt = s.dt
           |when not matched then insert *
         """.stripMargin)
      checkAnswer(s"select id, name, price, dt from $tableName order by id")(
        Seq(1, "a1n", 12.0, "p1"), Seq(2, "a2", 20.0, "p1"), Seq(3, "a3", 30.0, "p1"), Seq(4, "a4", 40.0, "p2"),
        Seq(5, "a5", 50.0, "p2"))
      assertEquals(Seq("1"), positionalDeletes(tablePath))
    }
  }

  Seq(None, Some("false")).foreach { optimizedWrites =>
    test(s"Test UPDATE and DELETE writing updates as deletes and inserts with optimized writes ${optimizedWrites.getOrElse("default")}") {
      withSQLConf(optimizedWrites.map(SPARK_SQL_OPTIMIZED_WRITES.key() -> _).toSeq: _*) {
        withTable { (tableName, tablePath) =>
          spark.sql(s"update $tableName set price = price * 2 where id in (1, 4)")
          checkAnswer(s"select id, name, price, dt from $tableName order by id")(
            Seq(1, "a1", 20.0, "p1"), Seq(2, "a2", 20.0, "p1"), Seq(3, "a3", 30.0, "p1"),
            Seq(4, "a4", 80.0, "p2"), Seq(5, "a5", 50.0, "p2"))
          assertEquals(Seq("1", "4"), positionalDeletes(tablePath))

          // The index also locates the tombstoned copy of key 1 in its old file group and deletes it again
          spark.sql(s"delete from $tableName where id in (1, 3)")
          checkAnswer(s"select id, name, price, dt from $tableName order by id")(
            Seq(2, "a2", 20.0, "p1"), Seq(4, "a4", 80.0, "p2"), Seq(5, "a5", 50.0, "p2"))
          assertEquals(Seq("1", "1", "1", "3", "4"), positionalDeletes(tablePath))
        }
      }
    }
  }

  test("Test UPDATE, MERGE INTO and DELETE writing updates as deletes and inserts with the record index across a compaction") {
    val rows = Seq(Seq(2, "a2m", 21.0, "p1"), Seq(4, "a4", 80.0, "p2"), Seq(5, "a5", 50.0, "p2"))
    withTable("RECORD_INDEX") { (tableName, tablePath) =>
      // Inline compaction stays off until the explicit one, so the log files keep every positional delete
      withSQLConf(HoodieCompactionConfig.INLINE_COMPACT.key -> "false") {
        spark.sql(s"update $tableName set price = price * 2 where id in (1, 4)")
        assertEquals(Seq("1", "4"), positionalDeletes(tablePath))

        // The record index locates only the current copy of id 1, so its tombstoned copy is not deleted again
        spark.sql(s"update $tableName set name = 'a1u' where id = 1")
        checkAnswer(s"select id, name, price, dt from $tableName order by id")(
          Seq(1, "a1u", 20.0, "p1"), Seq(2, "a2", 20.0, "p1"), Seq(3, "a3", 30.0, "p1"),
          Seq(4, "a4", 80.0, "p2"), Seq(5, "a5", 50.0, "p2"))
        assertEquals(Seq("1", "1", "4"), positionalDeletes(tablePath))

        spark.sql(
          s"""
             |merge into $tableName t
             |using (select 1 as id, 'x' as name, 0.0 as price, 'p1' as dt, 'none' as op
             |  union all select 2, 'a2m', 21.0, 'p1', 'upd'
             |  union all select 3, 'x', 0.0, 'p1', 'del'
             |  union all select 6, 'a6', 60.0, 'p2', 'ins') s
             |on t.id = s.id
             |when matched and s.op = 'del' then delete
             |when matched and s.op = 'upd' then update set id = s.id, name = s.name, price = s.price, dt = s.dt
             |when not matched and s.op = 'ins' then insert (id, name, price, dt) values (s.id, s.name, s.price, s.dt)
           """.stripMargin)
        spark.sql(s"delete from $tableName where id in (1, 6)")
        checkAnswer(s"select id, name, price, dt from $tableName order by id")(rows: _*)
        assertEquals(Seq("1", "1", "1", "2", "3", "4", "6"), positionalDeletes(tablePath))
      }

      withSQLConf(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key -> "1") {
        spark.sql(s"call run_compaction(op => 'run', table => '$tableName')")
      }
      checkAnswer(s"select id, name, price, dt from $tableName order by id")(rows: _*)
      assertEquals(Seq.empty, positionalDeletes(tablePath))

      // The compacted file groups keep their ids, so the record index still locates every key
      spark.sql(s"update $tableName set price = 99.0 where id = 2")
      spark.sql(s"delete from $tableName where id = 4")
      checkAnswer(s"select id, name, price, dt from $tableName order by id")(
        Seq(2, "a2m", 99.0, "p1"), Seq(5, "a5", 50.0, "p2"))
      assertEquals(Seq("2", "4"), positionalDeletes(tablePath))
    }
  }

  test("Test UPDATE, DELETE and MERGE INTO refused on a table without a primary key writing updates as deletes and inserts") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  dt string
           |) using hudi
           | partitioned by (dt)
           | location '$tablePath'
           | tblproperties (
           |  type = 'mor',
           |  'hoodie.record.merge.mode' = 'COMMIT_TIME_ORDERING'
           | )
         """.stripMargin)
      withSQLConf(HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS.key() -> "true") {
        // An insert locates no existing row, so the write mode lets it through
        spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 'p1'), (2, 'a2', 20.0, 'p1')")
        val instants = HoodieTestUtils.createMetaClient(HoodieTestUtils.getDefaultStorageConf, tablePath)
          .getActiveTimeline.getInstants.asScala.toList
        Seq(
          s"update $tableName set price = 11.0 where id = 1",
          s"delete from $tableName where id = 2",
          s"""merge into $tableName t
             |using (select 1 as id, 'a1m' as name, 12.0 as price, 'p1' as dt) s
             |on t.id = s.id
             |when matched then update set *""".stripMargin
        ).foreach { statement =>
          checkExceptionContain(statement)(BaseWriteHelper.keylessTableNotSupported().getMessage)
        }
        // Each statement is refused before it starts a commit
        assertEquals(instants, HoodieTestUtils.createMetaClient(HoodieTestUtils.getDefaultStorageConf, tablePath)
          .getActiveTimeline.getInstants.asScala.toList)
      }
      checkAnswer(s"select id, name, price, dt from $tableName order by id")(
        Seq(1, "a1", 10.0, "p1"), Seq(2, "a2", 20.0, "p1"))
    }
  }

  /**
   * Creates a merge-on-read table with the write mode enabled, holding ids 1 to 3 with dt p1 and ids 4 and 5
   * with dt p2, which are its partitions unless it is not partitioned.
   */
  private def withTable(f: (String, String) => Unit): Unit = withTable("SIMPLE")(f)

  private def withTable(indexType: String)(f: (String, String) => Unit): Unit = withTable(indexType, partitioned = true)(f)

  private def withTable(indexType: String, partitioned: Boolean)(f: (String, String) => Unit): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      withSQLConf(HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS.key() -> "true", "hoodie.index.type" -> indexType,
        HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key -> String.valueOf(indexType == "RECORD_INDEX")) {
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  dt string
             |) using hudi
             | ${if (partitioned) "partitioned by (dt)" else ""}
             | location '$tablePath'
             | tblproperties (
             |  type = 'mor',
             |  primaryKey = 'id',
             |  'hoodie.record.merge.mode' = 'COMMIT_TIME_ORDERING'
             | )
           """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 'p1'), (2, 'a2', 20.0, 'p1'), (3, 'a3', 30.0, 'p1'),"
          + " (4, 'a4', 40.0, 'p2'), (5, 'a5', 50.0, 'p2')")
        f(tableName, tablePath)
      }
    }
  }

  /**
   * Asserts that every log block of the latest file slices is a delete block whose positions point at
   * the rows of the deleted keys in the slice's base file, and returns the sorted deleted keys.
   */
  private def positionalDeletes(tablePath: String, partitioned: Boolean = true): Seq[String] = {
    val (metaClient, fsView) = getMetaClientAndFileSystemView(tablePath)
    val schema = new TableSchemaResolver(metaClient).getTableSchema
    val deletedKeys = ArrayBuffer[String]()
    try {
      (if (partitioned) Seq("dt=p1", "dt=p2") else Seq("")).flatMap(fsView.getLatestFileSlices(_).iterator().asScala).filter(_.hasLogFiles).foreach { slice =>
        assertTrue(slice.getBaseFile.isPresent, s"a file group with logs must have a base file: $slice")
        val baseFileKeys = ArrayBuffer[String]()
        val fileReader = HoodieIOFactory.getIOFactory(metaClient.getStorage)
          .getReaderFactory(HoodieRecordType.AVRO)
          .getFileReader(DEFAULT_HUDI_CONFIG_FOR_READER, new StoragePath(slice.getBaseFile.get.getPath))
        try {
          fileReader.getRecordKeyIterator.asScala.foreach(baseFileKeys += _)
        } finally {
          fileReader.close()
        }
        slice.getLogFiles.iterator().asScala.foreach { logFile =>
          val logReader = HoodieLogFormat.newReader(metaClient, logFile, schema)
          try {
            while (logReader.hasNext) {
              val block = logReader.next()
              assertTrue(block.isInstanceOf[HoodieDeleteBlock],
                s"expected only delete blocks but got ${block.getClass.getSimpleName} in ${logFile.getFileName}")
              val deleteBlock = block.asInstanceOf[HoodieDeleteBlock]
              val deleteRecords = deleteBlock.getRecordsToDelete
              val positions = deleteBlock.getRecordPositionList.asScala
              assertEquals(deleteRecords.length, positions.size, "every delete record must carry a position")
              positions.zipWithIndex.foreach { case (position, i) =>
                assertEquals(deleteRecords(i).getRecordKey, baseFileKeys(position.intValue()),
                  "the base file row at a delete position must hold the deleted record key")
              }
              assertEquals(slice.getBaseFile.get.getCommitTime, deleteBlock.getBaseFileInstantTimeOfPositions,
                "positions must reference the file slice's base file")
              deleteRecords.foreach(deletedKeys += _.getRecordKey)
            }
          } finally {
            logReader.close()
          }
        }
      }
    } finally {
      fsView.close()
    }
    deletedKeys.sorted.toList
  }
}
