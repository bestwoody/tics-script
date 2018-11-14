package org.apache.spark.sql.ch

import java.util.Locale

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.execution.columnar.{InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}

abstract class BaseCHCatalogSuite extends SparkFunSuite {
  var extended: SparkSession
  val testDb: String
  val testT: String
  val testDbWhatever: String = "whateverwhatever"
  val testTWhatever: String = "whateverwhatever"

  def verifyShowDatabases(expected: Array[String])

  def verifyShowTables(db: String,
                       expected: Array[String],
                       otherDb: String,
                       otherExpected: Array[String])

  def verifyDescTable(table: String, expected: Array[Array[String]])

  def init(): Unit = {
    extended.sql("create flash database if not exists default")

    extended.sql(s"drop table if exists default.$testT")
    extended.sql(s"drop table if exists default.$testTWhatever")

    extended.sql(s"drop database if exists $testDb")
    extended.sql(s"drop database if exists $testDbWhatever cascade")
  }

  def cleanUp(): Unit = {
    extended.sql(s"drop table if exists default.$testT")
    extended.sql(s"drop table if exists default.$testTWhatever")

    extended.sql(s"drop database if exists $testDb")
    extended.sql(s"drop database if exists $testDbWhatever cascade")
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

  def runExplainTest(): Unit = {
    extended.sql(s"use $testDb")
    assert(
      extended
        .sql(s"explain select * from ${testT}123")
        .collect()
        .head
        .getString(0)
        .contains("NoSuchTableException")
    )
    assert(
      !extended
        .sql(s"explain select * from $testT")
        .collect()
        .head
        .getString(0)
        .contains("NoSuchTableException")
    )
  }

  def runInsertTest(): Unit = {
    extended.sql(s"use default")
    extended.sql(s"insert into $testT values(0, '', null)")
    extended.sql(s"insert into $testDb.$testT values(1, 'abc', 12.34)")
    extended.sql(s"use $testDb")
    extended.sql(s"insert into default.$testT values(2, 'def', -43.21)")
    extended.sql(s"insert into $testT values(3, '', 0.0)")
  }

  def runCacheTest(): Unit = {
    def verifyCacheQuery(view: String, tableIdentifier: TableIdentifier, expected: Array[Int]) = {
      val df = extended.sql(s"select * from $view order by i")
      df.explain()
      val plan = df.queryExecution.sparkPlan
      assert(plan.find {
        case InMemoryTableScanExec(_, _, InMemoryRelation(_, _, _, _, _, tableName))
            if tableName.get == s"`$view`" =>
          true
        case _ => false
      }.isDefined)
      assert(df.collect().map(_.getInt(0)).deep == expected.deep)
    }

    extended.sql(s"use $testDb")
    var defaultExpected =
      extended.sql(s"select * from default.$testT order by i").collect().map(_.getInt(0))
    var testExpected =
      extended.sql(s"select * from $testDb.$testT order by i").collect().map(_.getInt(0))
    extended.sql(s"cache table cached_default as select * from default.$testT")
    verifyCacheQuery(s"cached_default", TableIdentifier(testT, Some("default")), defaultExpected)
    extended.sql(s"cache table cached_$testDb as select * from $testT")
    verifyCacheQuery(s"cached_$testDb", TableIdentifier(testT, Some(testDb)), testExpected)
    extended.sql(s"drop table cached_default")
    extended.sql(s"drop table cached_$testDb")

    def verifyCacheTable(tableIdentifier: TableIdentifier, query: String, expected: Array[Int]) = {
      val df = extended.sql(query)
      df.explain()
      val plan = df.queryExecution.sparkPlan
      assert(plan.find {
        case InMemoryTableScanExec(_, _, InMemoryRelation(_, _, _, _, _, tableName))
            if tableName.get == tableIdentifier.quotedString =>
          true
        case _ => false
      }.isDefined)
      assert(df.collect().map(_.getInt(0)).deep == expected.deep)
    }

    extended.sql(s"use default")
    defaultExpected =
      extended.sql(s"select * from default.$testT order by i").collect().map(_.getInt(0))
    testExpected =
      extended.sql(s"select * from $testDb.$testT order by i").collect().map(_.getInt(0))
    extended.sql(s"cache table $testT")
    extended.sql(s"cache table $testDb.$testT")
    verifyCacheTable(
      TableIdentifier(testT, Some("default")),
      s"select * from default.$testT order by i",
      defaultExpected
    )
    extended.sql(s"use $testDb")
    verifyCacheTable(
      TableIdentifier(testT, Some(testDb)),
      s"select * from $testT order by i",
      testExpected
    )

    // Testing FLASH-44.
    extended.sql(s"show databases")
    extended.sql(s"show tables")

    def verifyUncacheTable(query: String) = {
      val df = extended.sql(query)
      df.explain()
      val plan = df.queryExecution.sparkPlan
      assert(plan.find {
        case InMemoryTableScanExec(_, _, InMemoryRelation(_, _, _, _, _, _)) =>
          true
        case _ => false
      }.isEmpty)
    }

    extended.sql(s"use default")
    extended.sql(s"uncache table $testDb.$testT")
    extended.sql(s"use $testDb")
    verifyUncacheTable(s"select * from $testT")
    extended.sql(s"use default")
    extended.sql(s"uncache table $testT")
    extended.sql(s"use $testDb")
    verifyUncacheTable(s"select * from default.$testT")
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

  def runCTASTest(): Unit = {
    val ctas = "ctas"
    extended.sql(s"drop table if exists $testDb.$ctas")
    extended.sql(s"create table $testDb.$ctas using LOG as select * from $testDb.$testT")
    val df1 = extended.sql(s"select * from $testDb.$ctas")
    val df2 = extended.sql(s"select * from $testDb.$testT")
    assert(df1.except(df2).count() == 0 && df2.except(df1).count() == 0)
    extended.sql(s"drop table if exists $testDb.$ctas")
  }

  def runTruncateTest(): Unit = {
    extended.sql(s"use default")

    assertThrows[AnalysisException](extended.sql(s"truncate table $testTWhatever"))
    assertThrows[AnalysisException](extended.sql(s"truncate table $testDbWhatever.$testT"))

    extended.sql(s"drop table if exists $testDb.ctas_default")
    extended.sql(s"drop table if exists default.ctas_$testDb")

    extended.sql(s"create table $testDb.ctas_default using log as select * from $testT")
    extended.sql(s"create table ctas_$testDb using log as select * from $testDb.$testT")

    extended.sql(s"truncate table $testT")
    extended.sql(s"truncate table $testDb.$testT")

    assert(extended.sql(s"select * from default.$testT").count() == 0)
    assert(extended.sql(s"select * from $testDb.$testT").count() == 0)

    extended.sql(s"insert into table $testT select * from $testDb.ctas_default")
    extended.sql(s"insert into table $testDb.$testT select * from ctas_$testDb")

    extended.sql(s"drop table $testDb.ctas_default")
    extended.sql(s"drop table default.ctas_$testDb")

    assert(extended.sql(s"select * from default.$testT").count() > 0)
    assert(extended.sql(s"select * from $testDb.$testT").count() > 0)
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
