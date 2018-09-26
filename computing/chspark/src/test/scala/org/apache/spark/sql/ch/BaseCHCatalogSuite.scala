package org.apache.spark.sql.ch

import java.util.Locale

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}

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
        Array("Engine", "MutableMergeTree(128)", ""),
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
    assertThrows[AnalysisException](extended.sql(s"drop table $testDb.$testT"))
    extended.sql(
      s"create table $testT(i int primary key, s String not null, d decimal(20, 10)) using MMT"
    )
    verifyShowTables(testDb, Array(testT), "default", Array(testT))
    extended.sql(s"drop table default.$testT")
    extended.sql(s"use default")
    verifyShowTables("default", Array(), testDb, Array(testT))
    assertThrows[AnalysisException](extended.sql(s"drop table $testT"))
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

  def runWithAsTest(): Unit = {
    def verifyWithAs(db: String, otherDb: String, table: String, expected: Array[Int]) = {
      var r: Array[Int] = null
      extended.sql(s"use $db")
      r = extended
        .sql(s"with w as (select * from $table) select i from w order by i")
        .collect()
        .map(_.getInt(0))
      assert(r.deep == expected.deep)
      extended.sql(s"use $otherDb")
      r = extended
        .sql(s"with w as (select * from $db.$table) select i from w order by i")
        .collect()
        .map(_.getInt(0))
      assert(r.deep == expected.deep)
      extended.sql(s"use $db")
      r = extended
        .sql(s"with w as (select * from $otherDb.$table) select i from $table order by i")
        .collect()
        .map(_.getInt(0))
      assert(r.deep == expected.deep)
    }

    verifyWithAs("default", testDb, testT, Array(0, 2))
    verifyWithAs(testDb, "default", testT, Array(1, 3))
  }

  def runTempViewTest(): Unit = {
    val globalTempDB =
      extended.conf.get(StaticSQLConf.GLOBAL_TEMP_DATABASE).toLowerCase(Locale.ROOT)

    assertThrows[AnalysisException](extended.sql(s"create flash database $globalTempDB"))
    assertThrows[AnalysisException](extended.sql(s"create database $globalTempDB"))
    assertThrows[AnalysisException](extended.sql(s"drop database $globalTempDB"))
    assertThrows[AnalysisException](extended.sql(s"use $globalTempDB"))

    def verifyTempView(db: String, otherDb: String) = {
      extended.sql(s"use $db")
      extended.sql(s"select 1 as vc").createTempView(testT)
      verifyShowTables(db, Array(testT, testT), otherDb, Array(testT, testT))
      extended.sql(s"select vc from $testT")
      extended.sql(s"drop table $testT")
      verifyShowTables(db, Array(testT), otherDb, Array(testT))

      extended.sql(s"use $otherDb")
      extended.sql(s"create temporary view $testT as select 1 as vc")
      verifyShowTables(db, Array(testT, testT), otherDb, Array(testT, testT))
      extended.sql(s"select vc from $testT")
      extended.sql(s"drop view $testT")
      verifyShowTables(db, Array(testT), otherDb, Array(testT))

      extended.sql(s"create global temporary view $testT as select 1 as gvc")
      verifyShowTables(db, Array(testT), otherDb, Array(testT))
      assert(
        extended.sql(s"show tables from $globalTempDB").collect().map(_.getString(1)).deep == Array(
          testT
        ).deep
      )
      extended.sql(s"select gvc from $globalTempDB.$testT")
      extended.sql(s"drop table $globalTempDB.$testT")
      verifyShowTables(db, Array(testT), otherDb, Array(testT))
      assert(extended.sql(s"show tables from $globalTempDB").collect().isEmpty)

      extended.sql(s"create temporary view $testT as select 1 as vc")
      extended.sql(s"select 1 as gvc").createGlobalTempView(testT)

      extended.sql(s"use $db")
      extended.sql(s"select vc from $testT")
      extended.sql(s"select gvc from $globalTempDB.$testT")
      extended.sql(s"select i from $db.$testT")
      extended.sql(s"select i from $otherDb.$testT")

      extended.sql(s"use $otherDb")
      extended.sql(s"select vc from $testT")
      extended.sql(s"select gvc from $globalTempDB.$testT")
      extended.sql(s"select i from $db.$testT")
      extended.sql(s"select i from $otherDb.$testT")

      extended.sql(s"drop table $testT")
      extended.sql(s"drop table $globalTempDB.$testT")
    }

    verifyTempView("default", testDb)

    def verifyTempView2(db: String, otherDb: String) = {
      def verifyDf(df1: DataFrame, df2: DataFrame) =
        assert(df1.except(df2).count() == 0 && df2.except(df1).count() == 0)
      val df = extended.sql(s"select i from $db.$testT")
      val otherDf = extended.sql(s"select i from $otherDb.$testT")

      extended.sql(s"use $db")
      extended.sql(s"create temporary view v as select * from $testT")
      var dfV = extended.sql(s"select i from v")
      verifyDf(df, dfV)
      extended.sql(s"use $otherDb")
      dfV = extended.sql(s"select i from v")
      verifyDf(df, dfV)
      extended.sql(s"drop view v")

      extended.sql(s"create temporary view v as select * from $db.$testT")
      dfV = extended.sql(s"select i from v")
      verifyDf(df, dfV)
      dfV = extended.sql(s"select i from v")
      verifyDf(df, dfV)
      extended.sql(s"drop view v")

      extended.sql(s"use $otherDb")
      extended.sql(s"create temporary view v as select * from $testT")
      var otherDfV = extended.sql(s"select i from v")
      verifyDf(otherDf, otherDfV)
      extended.sql(s"use $db")
      otherDfV = extended.sql(s"select i from v")
      verifyDf(otherDf, otherDfV)
      extended.sql(s"drop view v")

      extended.sql(s"create temporary view v as select * from $otherDb.$testT")
      otherDfV = extended.sql(s"select i from v")
      verifyDf(otherDf, otherDfV)
      dfV = extended.sql(s"select i from v")
      verifyDf(otherDf, otherDfV)
      extended.sql(s"drop view v")
    }

    verifyTempView2("default", testDb)

    extended.sql(s"create temporary view $testT as select 1 as vc")
    extended.sql(s"create global temporary view $testT as select 1 as gvc")

    extended.sql(s"use default")
    extended.sql(s"drop table if exists default.$testT")
    verifyShowTables("default", Array(testT), testDb, Array(testT, testT))
    extended.sql(s"select vc from $testT")
    extended.sql(s"select gvc from $globalTempDB.$testT")

    extended.sql(s"use $testDb")
    extended.sql(s"drop table if exists $testDb.$testT")
    verifyShowTables(testDb, Array(testT), "default", Array(testT))
    extended.sql(s"select vc from $testT")
    extended.sql(s"select gvc from $globalTempDB.$testT")
  }
}
