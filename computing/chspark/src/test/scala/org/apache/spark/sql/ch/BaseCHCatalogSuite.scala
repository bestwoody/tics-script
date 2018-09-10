package org.apache.spark.sql.ch

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchTableException}
import org.apache.spark.sql.SparkSession

abstract class BaseCHCatalogSuite extends SparkFunSuite {
  var extended: SparkSession
  val testDb: String
  val testT: String

  def verifyShowDatabases(expected: Array[String])

  def verifyShowTables(db: String,
                       expected: Array[String],
                       otherDb: String,
                       otherExpected: Array[String])

  def verifyDescTable(table: String, expected: Array[Array[String]])

  def init(): Unit = {
    extended.sql("create flash database if not exists default")

    extended.sql(s"drop table if exists default.$testT")
    // Drop twice to make sure for both legacy and CH catalog.
    extended.sql(s"drop database if exists $testDb")
    extended.sql(s"drop database if exists $testDb")
  }

  def cleanUp(): Unit = {
    extended.sql(s"drop table if exists default.$testT")
    extended.sql(s"drop table if exists $testDb.$testT")
    // Drop twice to make sure for both legacy and CH catalog.
    extended.sql(s"drop database if exists $testDb")
    extended.sql(s"drop database if exists $testDb")
  }

  def runDatabaseTest(): Unit = {
    verifyShowDatabases(Array("default"))
    extended.sql("use default")
    assertThrows[NoSuchDatabaseException](extended.sql(s"use $testDb"))
    extended.sql(s"create flash database $testDb")
    verifyShowDatabases(Array("default", testDb))
    extended.sql(s"use $testDb")
    extended.sql(s"drop database $testDb")
    verifyShowDatabases(Array("default"))
    assertThrows[NoSuchDatabaseException](extended.sql(s"use $testDb"))
    extended.sql(s"create flash database $testDb")
    extended.sql(s"use $testDb")
    extended.sql(s"use default")
  }

  def runTableTest(): Unit = {
    extended.sql(
      s"create table if not exists $testT(i int primary key, s String not null, d decimal(20, 10)) using MMT(128)"
    )
    verifyShowTables("default", Array(testT), testDb, Array())
    verifyDescTable(
      testT,
      Array(
        Array("i", "int", null),
        Array("s", "string", null),
        Array("d", "decimal(20,10)", null),
        Array("Engine", "MutableMergeTree", ""),
        Array("PK", "i", "")
      )
    )
    verifyShowTables("default", Array(testT), testDb, Array())
    extended.sql(
      s"create table $testDb.$testT(i int primary key, s String not null, d decimal(20, 10)) using mmt"
    )
    extended.sql(s"use $testDb")
    verifyShowTables(testDb, Array(testT), "default", Array(testT))
    extended.sql(s"drop table $testT")
    verifyShowTables(testDb, Array(), "default", Array(testT))
    assertThrows[NoSuchTableException](extended.sql(s"drop table $testDb.$testT"))
    extended.sql(
      s"create table $testT(i int primary key, s String not null, d decimal(20, 10)) using MMT"
    )
    verifyShowTables(testDb, Array(testT), "default", Array(testT))
    extended.sql(s"drop table default.$testT")
    extended.sql(s"use default")
    verifyShowTables("default", Array(), testDb, Array(testT))
    assertThrows[NoSuchTableException](extended.sql(s"drop table $testT"))
    extended.sql(s"use $testDb")
    extended.sql(
      s"create table default.$testT(i int primary key, s String not null, d decimal(20, 10)) using mmt(8192)"
    )
    verifyShowTables(testDb, Array(testT), "default", Array(testT))
  }

  def runInsertTest(): Unit = {
    extended.sql(s"use default")
    extended.sql(s"insert into $testT values(0, '', null)")
    extended.sql(s"insert into $testDb.$testT values(1, 'abc', 12.34)")
    extended.sql(s"use $testDb")
    extended.sql(s"insert into default.$testT values(2, 'def', -43.21)")
    extended.sql(s"insert into $testT values(3, '', 0.0)")
  }

  def runQueryTest(): Unit = {
    def verifyTable(db: String, otherDb: String, table: String, expected: Array[Int]) = {
      var r: Array[Int] = null
      extended.sql(s"use $db")
      r = extended.sql(s"select i from $table order by i").collect().map(_.getInt(0))
      assert(r.deep == expected.deep)
      r = extended.sql(s"select i from $db.$table order by i").collect().map(_.getInt(0))
      assert(r.deep == expected.deep)
      extended.sql(s"use $otherDb")
      r = extended.sql(s"select i from $db.$table order by i").collect().map(_.getInt(0))
      assert(r.deep == expected.deep)
    }

    verifyTable("default", testDb, testT, Array(0, 2))
    verifyTable(testDb, "default", testT, Array(1, 3))
  }
}
