package org.apache.spark.sql.ch

import java.util.Locale

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{DatabaseAlreadyExistsException, NoSuchDatabaseException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog.{CHSessionCatalog, SessionCatalog}
import org.apache.spark.sql.execution.columnar.{InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.internal.StaticSQLConf

abstract class BaseLegacyCatalogSuite extends SparkFunSuite {
  var extended: SparkSession

  lazy val chCatalog: CHSessionCatalog = extended.sessionState.planner.extraPlanningStrategies.head
    .asInstanceOf[CHApplyTimestampStrategy]
    .getOrCreateCHContext(extended)
    .chConcreteCatalog
  lazy val legacyCatalog: SessionCatalog = extended.sqlContext.sessionState.catalog

  val testLegacyDb: String
  val testCHDb: String
  val testDbFlash: String = "flash"
  val testDbWhatever: String = "whateverwhatever"
  val testTWhatever: String = "whateverwhatever"
  val testT: String

  def verifyShowDatabases(expected: Array[String])

  def verifyShowTables(db: String,
                       expected: Array[String],
                       otherDb: String,
                       otherExpected: Array[String])

  def verifyShowCreateTable(table: String, isCHTable: Boolean) = {
    val stmt1 = extended.sql(s"show create table $table").head.getString(0)
    assert(stmt1.contains("PRIMARY KEY") == isCHTable)
    extended.sql(s"drop table $table")
    extended.sql(stmt1)
    val stmt2 = extended.sql(s"show create table $table").head.getString(0)
    var last = stmt1.indexOf("DdlTime")
    if (last == -1) last = stmt1.length
    assert(stmt1.substring(0, last) == stmt2.substring(0, last))
  }

  def verifyDescLegacyTable(table: String, expected: Array[Array[String]])

  def verifyDescCHTable(table: String, expected: Array[Array[String]])

  def init(): Unit = {
    extended.sparkContext.setLogLevel("ERROR")

    extended.sql(s"create database if not exists default")
    extended.sql(s"create flash database if not exists default")

    extended.sql(s"drop table if exists default.$testT")
    extended.sql(s"drop table if exists default.$testTWhatever")

    extended.sql(s"drop database if exists $testLegacyDb cascade")
    extended.sql(s"drop database if exists $testCHDb cascade")
    extended.sql(s"drop database if exists $testDbFlash cascade")
    extended.sql(s"drop database if exists $testDbWhatever cascade")
  }

  def cleanUp(): Unit = {
    extended.sql(s"drop table if exists default.$testT")
    extended.sql(s"drop table if exists default.$testTWhatever")

    extended.sql(s"drop database if exists $testLegacyDb cascade")
    extended.sql(s"drop database if exists $testCHDb cascade")
    extended.sql(s"drop database if exists $testDbFlash cascade")
    extended.sql(s"drop database if exists $testDbWhatever cascade")
  }

  def runQuotedNameTest(): Unit = {
    val db = "quoted-db"
    val t = "quoted-t"
    val c = "i am"
    extended.sql(s"drop database if exists `$db`")
    extended.sql(s"create flash database `$db`")
    verifyShowDatabases(Array("default", db))
    extended.sql(s"use `$db`")
    extended.sql(s"create table `$t`(`$c` int primary key) using mmt(128)")
    verifyDescCHTable(
      s"`$t`",
      Array(
        Array(c, "int", null),
        Array("Engine", "MutableMergeTree(128)", ""),
        Array("PK", c, "")
      )
    )
    verifyShowCreateTable(s"`$t`", true)
    extended.sql(s"insert into `$t` values(0)")
    assert(extended.sql(s"select `$c` from `$t`").head.getInt(0) == 0)
    verifyShowTables(s"`db`", Array(t), "default", Array())
    extended.sql(s"use default")
    extended.sql(s"drop table `$db`.`$t`")
    extended.sql(s"drop database `$db`")
  }

  def runValidateCatalogTest(): Unit = {
    extended.sql(s"create flash database $testCHDb")
    assertThrows[AnalysisException](
      extended.sql(s"create table $testT(i int primary key) using mmt")
    )
    assertThrows[AnalysisException](extended.sql(s"create table from tidb $testT using mmt"))
    extended.sql(s"use $testCHDb")
    assertThrows[AnalysisException](extended.sql(s"create table $testT(i int)"))
    extended.sql(s"drop database $testCHDb")
  }

  def runDatabaseTest(): Unit = {
    verifyShowDatabases(Array("default"))
    assertThrows[NoSuchDatabaseException](extended.sql(s"show tables in $testLegacyDb"))
    assertThrows[NoSuchDatabaseException](extended.sql(s"show tables in $testCHDb"))
    extended.sql(s"create database $testLegacyDb")
    verifyShowDatabases(Array("default", testLegacyDb))
    extended.sql(s"show tables in $testLegacyDb")
    assert(legacyCatalog.databaseExists(testLegacyDb))
    assert(!chCatalog.databaseExists(testLegacyDb))
    extended.sql(s"drop database if exists $testDbFlash")
    assertThrows[NoSuchDatabaseException](extended.sql(s"use $testDbFlash"))
    extended.sql(s"create database $testDbFlash")
    verifyShowDatabases(Array("default", testDbFlash, testLegacyDb))
    extended.sql(s"use $testDbFlash")
    assert(legacyCatalog.getCurrentDatabase.equals(testDbFlash))
    extended.sql(s"drop database $testDbFlash")
    extended.sql(s"create flash database $testLegacyDb")
    verifyShowDatabases(Array("default", testLegacyDb))
    assert(chCatalog.databaseExists(testLegacyDb))
    extended.sql(s"drop database $testLegacyDb")
    assert(!legacyCatalog.databaseExists(testLegacyDb))
    assert(chCatalog.databaseExists(testLegacyDb))
    extended.sql(s"drop database $testLegacyDb")
    assert(!legacyCatalog.databaseExists(testLegacyDb))
    assert(!chCatalog.databaseExists(testLegacyDb))
    extended.sql(s"create database $testLegacyDb")
    extended.sql(s"use $testLegacyDb")
    verifyShowDatabases(Array("default", testLegacyDb))
    extended.sql(s"create flash database $testCHDb")
    verifyShowDatabases(Array("default", testLegacyDb, testCHDb))
    assertThrows[DatabaseAlreadyExistsException](extended.sql(s"create flash database $testCHDb"))
    extended.sql(s"show tables in $testCHDb")
    verifyShowDatabases(Array("default", testLegacyDb, testCHDb))
    extended.sql(s"use $testCHDb")
    assert(legacyCatalog.databaseExists(testLegacyDb))
    assert(chCatalog.databaseExists(testCHDb))
    assert(legacyCatalog.getCurrentDatabase.equals(testLegacyDb))
    assert(chCatalog.getCurrentDatabase.equals(testCHDb))
    extended.sql(s"use default")
  }

  def runTableTest(): Unit = {
    assertThrows[NoSuchTableException](extended.sql(s"desc $testT"))
    assertThrows[NoSuchDatabaseException](extended.sql(s"desc $testDbWhatever.$testT"))
    assertThrows[AnalysisException](
      extended.sql(
        s"create table $testT(i int primary key, s String not null, d decimal(20, 10)) using MMT(128)"
      )
    )
    assertThrows[NoSuchTableException](extended.sql(s"show create table $testT"))
    assertThrows[NoSuchTableException](extended.sql(s"desc $testT"))
    verifyShowTables("default", Array(), testCHDb, Array())
    extended.sql(
      s"create table $testT(i int, s String, d decimal(20, 10))"
    )
    verifyShowCreateTable(s"$testT", false)
    verifyShowCreateTable(s"default.$testT", false)
    verifyShowTables("default", Array(testT), testLegacyDb, Array())
    verifyShowTables("default", Array(testT), testCHDb, Array())
    assert(legacyCatalog.tableExists(TableIdentifier(testT, None)))
    extended.sql(s"use $testLegacyDb")
    verifyShowTables(testLegacyDb, Array(), "default", Array(testT))
    verifyShowTables(testLegacyDb, Array(), testCHDb, Array())
    extended.sql(
      s"create table $testCHDb.$testT(i int primary key, s String not null, d decimal(20, 10)) using MMT(32, 128)"
    )
    verifyShowCreateTable(s"$testCHDb.$testT", true)
    extended.sql(s"use $testCHDb")
    verifyShowTables(testCHDb, Array(testT), "default", Array(testT))
    verifyShowTables(testCHDb, Array(testT), testLegacyDb, Array())
    assert(chCatalog.tableExists(TableIdentifier(testT, None)))
    assertThrows[TableAlreadyExistsException](
      extended.sql(
        s"create table $testT(i int primary key, s String not null, d decimal(20, 10)) using MMT(128)"
      )
    )
    verifyShowCreateTable(s"$testT", true)
    extended.sql(s"drop table default.$testT")
    assert(!legacyCatalog.tableExists(TableIdentifier(testT, Some("default"))))
    verifyShowTables(testCHDb, Array(testT), "default", Array())
    verifyShowTables(testCHDb, Array(testT), testLegacyDb, Array())
    extended.sql(
      s"create table default.$testT(i int, s String, d decimal(20, 10))"
    )
    extended.sql(s"drop table $testT")
    assert(legacyCatalog.tableExists(TableIdentifier(testT, Some("default"))))
    assert(!chCatalog.tableExists(TableIdentifier(testT, None)))
    verifyShowTables(testCHDb, Array(), "default", Array(testT))
    verifyShowTables(testCHDb, Array(), testLegacyDb, Array())
    extended.sql(
      s"create table if not exists default.$testT(i int, s String, d decimal(20, 10))"
    )

    extended.sql(
      s"create table $testT(i int primary key, s String not null, d decimal(20, 10)) using MMT(128)"
    )
    assert(legacyCatalog.tableExists(TableIdentifier(testT, Some("default"))))
    assert(chCatalog.tableExists(TableIdentifier(testT, Some(testCHDb))))
    verifyShowTables(testCHDb, Array(testT), "default", Array(testT))
    verifyShowTables(testCHDb, Array(testT), testLegacyDb, Array())
    extended.sql(
      s"create table if not exists $testCHDb.$testT(i int primary key, s String not null, d decimal(20, 10)) using MMT(128)"
    )

    extended.sql(s"use $testLegacyDb")
    extended.sql(
      s"create table $testT(i int, s String, d decimal(20, 10))"
    )
    assertThrows[TableAlreadyExistsException](
      extended.sql(
        s"create table $testLegacyDb.$testT(i int, s String, d decimal(20, 10))"
      )
    )

    extended.sql("use default")
    verifyDescLegacyTable(
      testT,
      Array(
        Array("i", "int", null),
        Array("s", "string", null),
        Array("d", "decimal(20,10)", null)
      )
    )
    verifyDescCHTable(
      s"$testCHDb.$testT",
      Array(
        Array("i", "int", null),
        Array("s", "string", null),
        Array("d", "decimal(20,10)", null),
        Array("Engine", "MutableMergeTree(128)", ""),
        Array("PK", "i", "")
      )
    )
  }

  def runInsertTest(): Unit = {
    extended.sql(s"use default")
    assertThrows[AnalysisException](
      extended.sql(s"insert into $testTWhatever values(0, 'default', null)")
    )
    extended.sql(s"insert into $testT values(0, 'default', null)")
    extended.sql(s"insert into $testLegacyDb.$testT values(1, 'legacy', 12.34)")
    extended.sql(s"use $testCHDb")
    extended.sql(s"insert into $testT values(2, 'CH', 0.0)")
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

    extended.sql(s"use $testCHDb")
    var legacyExpected =
      extended.sql(s"select * from $testLegacyDb.$testT order by i").collect().map(_.getInt(0))
    var chExpected =
      extended.sql(s"select * from $testCHDb.$testT order by i").collect().map(_.getInt(0))
    extended.sql(s"cache table cached_$testLegacyDb as select * from $testLegacyDb.$testT")
    verifyCacheQuery(
      s"cached_$testLegacyDb",
      TableIdentifier(testT, Some(testLegacyDb)),
      legacyExpected
    )
    extended.sql(s"cache table cached_$testCHDb as select * from $testT")
    verifyCacheQuery(s"cached_$testCHDb", TableIdentifier(testT, Some(testCHDb)), chExpected)
    extended.sql(s"drop table cached_$testLegacyDb")
    extended.sql(s"drop table cached_$testCHDb")

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

    extended.sql(s"use $testLegacyDb")
    legacyExpected =
      extended.sql(s"select * from $testLegacyDb.$testT order by i").collect().map(_.getInt(0))
    chExpected =
      extended.sql(s"select * from $testCHDb.$testT order by i").collect().map(_.getInt(0))
    extended.sql(s"cache table $testT")
    extended.sql(s"cache table $testCHDb.$testT")
    verifyCacheTable(
      TableIdentifier(testT, Some(testLegacyDb)),
      s"select * from $testLegacyDb.$testT order by i",
      legacyExpected
    )
    extended.sql(s"use $testCHDb")
    verifyCacheTable(
      TableIdentifier(testT, Some(testCHDb)),
      s"select * from $testT order by i",
      chExpected
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

    extended.sql(s"use $testLegacyDb")
    extended.sql(s"uncache table $testCHDb.$testT")
    extended.sql(s"use $testCHDb")
    verifyUncacheTable(s"select * from $testT")
    extended.sql(s"use $testLegacyDb")
    extended.sql(s"uncache table $testT")
    extended.sql(s"use $testCHDb")
    verifyUncacheTable(s"select * from $testLegacyDb.$testT")
  }

  def runTruncateTest(): Unit = {
    extended.sql(s"use $testLegacyDb")

    assertThrows[AnalysisException](extended.sql(s"truncate table $testTWhatever"))
    assertThrows[AnalysisException](extended.sql(s"truncate table $testDbWhatever.$testT"))

    extended.sql(s"drop table if exists $testCHDb.ctas_$testLegacyDb")
    extended.sql(s"drop table if exists $testLegacyDb.ctas_$testCHDb")

    extended.sql(s"create table $testCHDb.ctas_$testLegacyDb using log as select * from $testT")
    extended.sql(s"create table ctas_$testCHDb as select * from $testCHDb.$testT")

    extended.sql(s"truncate table $testT")
    extended.sql(s"truncate table $testCHDb.$testT")

    assert(extended.sql(s"select * from $testLegacyDb.$testT").count() == 0)
    assert(extended.sql(s"select * from $testCHDb.$testT").count() == 0)

    extended.sql(s"insert into table $testT select * from $testCHDb.ctas_$testLegacyDb")
    extended.sql(s"insert into table $testCHDb.$testT select * from ctas_$testCHDb")

    extended.sql(s"drop table $testCHDb.ctas_$testLegacyDb")
    extended.sql(s"drop table $testLegacyDb.ctas_$testCHDb")

    assert(extended.sql(s"select * from $testLegacyDb.$testT").count() > 0)
    assert(extended.sql(s"select * from $testCHDb.$testT").count() > 0)
  }

  def runQueryTest(): Unit = {
    def verifySingleTable(db: String, table: String, expected: Array[Int]) = {
      var r: Array[Int] = null
      extended.sql(s"use default")
      r = extended.sql(s"select i from $db.$table").collect().map(_.getInt(0))
      assert(r.deep == expected.deep)
      extended.sql(s"use $testLegacyDb")
      r = extended.sql(s"select i from $db.$table").collect().map(_.getInt(0))
      assert(r.deep == expected.deep)
      extended.sql(s"use $testLegacyDb")
      r = extended.sql(s"select i from $db.$table").collect().map(_.getInt(0))
      assert(r.deep == expected.deep)
      extended.sql(s"use $db")
      r = extended.sql(s"select i from $table").collect().map(_.getInt(0))
      assert(r.deep == expected.deep)
    }

    def verifyJoinTable(db1: String,
                        table1: String,
                        db2: String,
                        table2: String,
                        expected: Array[(Int, Int)]) = {
      var r: Array[(Int, Int)] = null
      extended.sql(s"use default")
      r = extended
        .sql(s"select t1.i, t2.i from $db1.$table1 as t1 cross join $db2.$table2 as t2")
        .collect()
        .map(row => (row.getInt(0), row.getInt(1)))
      assert(r.deep == expected.deep)
      extended.sql(s"use $testLegacyDb")
      r = extended
        .sql(s"select t1.i, t2.i from $db1.$table1 as t1 cross join $db2.$table2 as t2")
        .collect()
        .map(row => (row.getInt(0), row.getInt(1)))
      assert(r.deep == expected.deep)
      extended.sql(s"use $testLegacyDb")
      r = extended
        .sql(s"select t1.i, t2.i from $db1.$table1 as t1 cross join $db2.$table2 as t2")
        .collect()
        .map(row => (row.getInt(0), row.getInt(1)))
      assert(r.deep == expected.deep)
      extended.sql(s"use $db1")
      r = extended
        .sql(s"select t1.i, t2.i from $table1 as t1 cross join $db2.$table2 as t2")
        .collect()
        .map(row => (row.getInt(0), row.getInt(1)))
      assert(r.deep == expected.deep)
      extended.sql(s"use $db2")
      r = extended
        .sql(s"select t1.i, t2.i from $db1.$table1 as t1 cross join $table2 as t2")
        .collect()
        .map(row => (row.getInt(0), row.getInt(1)))
      assert(r.deep == expected.deep)
    }

    assertThrows[AnalysisException](extended.sql(s"select * from $testTWhatever"))

    verifySingleTable("default", testT, Array(0))
    verifySingleTable(testLegacyDb, testT, Array(1))
    verifySingleTable(testCHDb, testT, Array(2))

    verifyJoinTable("default", testT, testLegacyDb, testT, Array((0, 1)))
    verifyJoinTable(testLegacyDb, testT, testCHDb, testT, Array((1, 2)))
    verifyJoinTable(testCHDb, testT, "default", testT, Array((2, 0)))
  }

  def runCTASTest(): Unit = {
    def verify(t1: String, t2: String) = {
      val df1 = extended.sql(s"select * from $t1")
      val df2 = extended.sql(s"select * from $t2")
      assert(df1.except(df2).count() == 0 && df2.except(df1).count() == 0)
    }
    val ctas = "ctas"

    extended.sql(s"drop table if exists $testCHDb.$ctas")
    extended.sql(s"create table $testCHDb.$ctas using LOG as select * from $testCHDb.$testT")
    verify(s"$testCHDb.$ctas", s"$testCHDb.$testT")
    extended.sql(s"drop table if exists $testCHDb.$ctas")

    extended.sql(s"use $testLegacyDb")
    assertThrows[AnalysisException](
      extended.sql(s"create table $ctas using LOG as select * from $testCHDb.$testT")
    )
    assertThrows[AnalysisException](
      extended.sql(s"create table $testCHDb.$ctas as select * from $testT")
    )
    extended.sql(s"create table $ctas as select * from $testCHDb.$testT")
    verify(s"$testLegacyDb.$ctas", s"$testCHDb.$testT")
    extended.sql(s"create table $testCHDb.$ctas using LOG as select * from $testT")
    verify(s"$testCHDb.$ctas", s"$testLegacyDb.$testT")
    extended.sql(s"drop table if exists $testLegacyDb.$ctas")
    extended.sql(s"drop table if exists $testCHDb.$ctas")
    assertThrows[AnalysisException](
      extended.sql(s"create table $testCHDb.$ctas as select * from $testCHDb.$testT")
    )
    assertThrows[AnalysisException](
      extended.sql(s"create table $ctas using LOG as select * from $testT")
    )
    extended.sql(s"create table $ctas as select * from $testT")
    verify(s"$testLegacyDb.$ctas", s"$testLegacyDb.$testT")
    extended.sql(s"create table $testCHDb.$ctas using LOG as select * from $testCHDb.$testT")
    verify(s"$testCHDb.$ctas", s"$testCHDb.$testT")
    extended.sql(s"drop table if exists $testLegacyDb.$ctas")
    extended.sql(s"drop table if exists $testCHDb.$ctas")

    extended.sql(s"use $testCHDb")
    assertThrows[AnalysisException](
      extended.sql(s"create table $ctas as select * from $testLegacyDb.$testT")
    )
    assertThrows[AnalysisException](
      extended.sql(s"create table $testLegacyDb.$ctas using LOG as select * from $testT")
    )
    extended.sql(s"create table $ctas using LOG as select * from $testLegacyDb.$testT")
    verify(s"$testCHDb.$ctas", s"$testLegacyDb.$testT")
    extended.sql(s"create table $testLegacyDb.$ctas as select * from $testT")
    verify(s"$testLegacyDb.$ctas", s"$testCHDb.$testT")
    extended.sql(s"drop table if exists $testLegacyDb.$ctas")
    extended.sql(s"drop table if exists $testCHDb.$ctas")
    assertThrows[AnalysisException](
      extended
        .sql(s"create table $testLegacyDb.$ctas using LOG as select * from $testLegacyDb.$testT")
    )
    assertThrows[AnalysisException](extended.sql(s"create table $ctas as select * from $testT"))
    extended.sql(s"create table $ctas using LOG as select * from $testT")
    verify(s"$testCHDb.$ctas", s"$testCHDb.$testT")
    extended.sql(s"create table $testLegacyDb.$ctas as select * from $testLegacyDb.$testT")
    verify(s"$testLegacyDb.$ctas", s"$testLegacyDb.$testT")
    extended.sql(s"drop table if exists $testLegacyDb.$ctas")
    extended.sql(s"drop table if exists $testCHDb.$ctas")
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

    verifyTempView("default", testLegacyDb)
    verifyTempView("default", testCHDb)
    verifyTempView(testLegacyDb, testCHDb)

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

    verifyTempView2("default", testLegacyDb)
    verifyTempView2("default", testCHDb)
    verifyTempView2(testLegacyDb, testCHDb)

    extended.sql(s"create temporary view $testT as select 1 as vc")
    extended.sql(s"create global temporary view $testT as select 1 as gvc")

    extended.sql(s"use default")
    extended.sql(s"drop table if exists default.$testT")
    verifyShowTables("default", Array(testT), testLegacyDb, Array(testT, testT))
    verifyShowTables("default", Array(testT), testCHDb, Array(testT, testT))
    extended.sql(s"select vc from $testT")
    extended.sql(s"select gvc from $globalTempDB.$testT")

    extended.sql(s"use $testLegacyDb")
    extended.sql(s"drop table if exists $testLegacyDb.$testT")
    verifyShowTables(testLegacyDb, Array(testT), "default", Array(testT))
    verifyShowTables(testLegacyDb, Array(testT), testCHDb, Array(testT, testT))
    extended.sql(s"select vc from $testT")
    extended.sql(s"select gvc from $globalTempDB.$testT")

    extended.sql(s"use $testCHDb")
    extended.sql(s"drop table if exists $testCHDb.$testT")
    verifyShowTables(testCHDb, Array(testT), "default", Array(testT))
    verifyShowTables(testCHDb, Array(testT), testLegacyDb, Array(testT))
    extended.sql(s"select vc from $testT")
    extended.sql(s"select gvc from $globalTempDB.$testT")
  }
}
