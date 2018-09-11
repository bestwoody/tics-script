package org.apache.spark.sql.ch

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{DatabaseAlreadyExistsException, NoSuchDatabaseException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog.{CHSessionCatalog, SessionCatalog}

abstract class BaseLegacyCatalogSuite extends SparkFunSuite {
  var extended: SparkSession

  lazy val chCatalog: CHSessionCatalog = extended.sessionState.planner.extraPlanningStrategies.head
    .asInstanceOf[CHStrategy]
    .getOrCreateCHContext(extended)
    .chConcreteCatalog
  lazy val legacyCatalog: SessionCatalog = extended.sqlContext.sessionState.catalog

  val testLegacyDb: String
  val testCHDb: String
  val testDbFlash: String = "flash"
  val testDbWhatever: String = "whateverwhatever"
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
    extended.sql(s"create database if not exists default")
    extended.sql(s"create flash database if not exists default")
    extended.sql(s"drop table if exists default.$testT")
    extended.sql(s"drop database if exists $testLegacyDb cascade")
    extended.sql(s"drop database if exists $testCHDb cascade")
    extended.sql(s"drop database if exists $testDbFlash cascade")
    extended.sql(s"drop database if exists $testDbWhatever cascade")
  }

  def cleanUp(): Unit = {
    extended.sql(s"drop table if exists default.$testT")
    extended.sql(s"drop table if exists $testLegacyDb.$testT")
    extended.sql(s"drop table if exists $testCHDb.$testT")
    extended.sql(s"drop database if exists $testLegacyDb cascade")
    extended.sql(s"drop database if exists $testCHDb cascade")
    extended.sql(s"drop database if exists $testDbFlash cascade")
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
        Array("Engine", "MutableMergeTree", ""),
        Array("PK", "i", "")
      )
    )
  }

  def runInsertTest(): Unit = {
    extended.sql(s"use default")
    extended.sql(s"insert into $testT values(0, 'default', null)")
    extended.sql(s"insert into $testLegacyDb.$testT values(1, 'legacy', 12.34)")
    extended.sql(s"use $testCHDb")
    extended.sql(s"insert into $testT values(2, 'CH', 0.0)")
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

    verifySingleTable("default", testT, Array(0))
    verifySingleTable(testLegacyDb, testT, Array(1))
    verifySingleTable(testCHDb, testT, Array(2))

    verifyJoinTable("default", testT, testLegacyDb, testT, Array((0, 1)))
    verifyJoinTable(testLegacyDb, testT, testCHDb, testT, Array((1, 2)))
    verifyJoinTable(testCHDb, testT, "default", testT, Array((2, 0)))
  }
}
