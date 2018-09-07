/*
 *
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.sql

import java.sql.Statement
import java.util.Locale

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

import scala.collection.mutable.ArrayBuffer

class BaseClickHouseSuite extends QueryTest with SharedSQLContext {

  protected var clickHouseStmt: Statement = _

  protected var tableNames: Seq[String] = _

  protected def querySpark(query: String): List[List[Any]] = {
    val df = sql(query)
    val schema = df.schema.fields

    dfData(df, schema)
  }

  def queryClickHouse(query: String): List[List[Any]] = {
    val resultSet = clickHouseStmt.executeQuery(query)
    val rsMetaData = resultSet.getMetaData
    val retSet = ArrayBuffer.empty[List[Any]]
    val retSchema = ArrayBuffer.empty[String]
    for (i <- 1 to rsMetaData.getColumnCount) {
      retSchema += rsMetaData.getColumnTypeName(i)
    }
    while (resultSet.next()) {
      val row = ArrayBuffer.empty[Any]

      for (i <- 1 to rsMetaData.getColumnCount) {
        row += toOutput(resultSet.getObject(i), retSchema(i - 1))
      }
      retSet += row.toList
    }
    retSet.toList
  }

  def createOrReplaceTempView(dbName: String, viewName: String, postfix: String = "_j"): Unit =
    spark.read
      .format("jdbc")
      .option(JDBCOptions.JDBC_URL, jdbcUrl + s"/$dbName")
      .option(JDBCOptions.JDBC_TABLE_NAME, s"`$viewName`")
      .option(JDBCOptions.JDBC_DRIVER_CLASS, "ru.yandex.clickhouse.ClickHouseDriver")
      .load()
      .createOrReplaceTempView(s"`$viewName$postfix`")

  protected def loadTestData(databases: Seq[String] = Seq(testDBName)): Unit =
    try {
      tableNames = Seq.empty[String]
      for (dbName <- databases) {
        clickHouseConn.setCatalog(dbName)
        ch.mapCHDatabase(dbName)
        val tableDF = spark.read
          .format("jdbc")
          .option(JDBCOptions.JDBC_URL, jdbcUrl + "/system")
          .option(JDBCOptions.JDBC_TABLE_NAME, "tables")
          .option(JDBCOptions.JDBC_DRIVER_CLASS, "ru.yandex.clickhouse.ClickHouseDriver")
          .load()
          .filter(s"database = '$dbName'")
          .select("name")
        val tables = tableDF.collect().map((row: Row) => row.get(0).toString)
        tables.foreach(createOrReplaceTempView(dbName, _))
        tableNames ++= tables
      }
      logger.info("reload test data complete")
    } catch {
      case e: Exception =>
        println(e.getStackTrace.mkString(","))
        logger.warn("reload test data failed", e)
    } finally {
      tableNames = tableNames.sorted.reverse
    }

  protected case class TestTables(dbName: String, tables: String*)

  def loadTestDataFromTestTables(testTables: TestTables): Unit = {
    val dbName = testTables.dbName
    clickHouseConn.setCatalog(dbName)
    ch.mapCHDatabase(dbName)
    for (tableName <- testTables.tables) {
      createOrReplaceTempView(dbName, tableName)
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    setLogLevel("WARN")
    loadTestData()
    initializeTimeZone()
  }

  def initializeTimeZone(): Unit = {
    clickHouseConn.setCatalog(testDBName)
    clickHouseStmt = clickHouseConn.createStatement()
  }

  def refreshConnections(testTables: TestTables): Unit = {
    super.refreshConnections()
    loadTestDataFromTestTables(testTables)
    initializeTimeZone()
  }

  override def refreshConnections(): Unit = {
    super.refreshConnections()
    loadTestData()
    initializeTimeZone()
  }

  def setLogLevel(level: String): Unit =
    spark.sparkContext.setLogLevel(level)

  private def replaceJDBCTableName(qSpark: String, skipJDBC: Boolean): String = {
    var qJDBC: String = null
    if (!skipJDBC) {
      qJDBC = qSpark + " "
      for (tableName <- tableNames) {
        // tableNames is guaranteed to be in reverse order, so Seq[t, t2, lt]
        // will never be possible, and the following operation holds correct.
        // e.g., for input Seq[t2, t, lt]
        // e.g., select * from t, t2, lt -> select * from t_j, t2_j, lt_j
        qJDBC = qJDBC.replaceAllLiterally(" " + tableName + " ", " " + tableName + "_j ")
        qJDBC = qJDBC.replaceAllLiterally(" " + tableName + ",", " " + tableName + "_j,")
      }
    }
    qJDBC
  }

  def judge(qSpark: String, skipped: Boolean = false): Unit =
    assert(execDBTSAndJudge(qSpark, skipped))

  def convertSparkSQLToCHSQL(qSpark: String): String =
    qSpark
      .toLowerCase(Locale.ENGLISH)
      .replace(" date(", " toDate(")
      .replace(" date)", " Nullable(Date))")
      .replace(" first(", " any(")
      .replace(" ifnull(", " ifNull(")

  def execDBTSAndJudge(qSpark: String, skipped: Boolean = false): Boolean =
    try {
      if (skipped) {
        logger.warn(s"Test is skipped. [With Spark SQL: $qSpark]")
        true
      } else {
        val qClickHouse: String = convertSparkSQLToCHSQL(qSpark)
        compResult(querySpark(qSpark), queryClickHouse(qClickHouse), qSpark.contains(" order by "))
      }
    } catch {
      case e: Throwable => fail(e)
    }

  def explainSpark(str: String, skipped: Boolean = false): Unit =
    try {
      if (skipped) {
        logger.warn(s"Test is skipped. [With Spark SQL: $str]")
      } else {
        spark.sql(str).explain()
      }
    } catch {
      case e: Throwable => fail(e)
    }

  def explainAndTest(str: String, skipped: Boolean = false): Unit =
    try {
      explainSpark(str)
      judge(str, skipped)
    } catch {
      case e: Throwable => fail(e)
    }

  def explainAndRunTest(qSpark: String,
                        qJDBC: String = null,
                        skipped: Boolean = false,
                        rSpark: List[List[Any]] = null,
                        rJDBC: List[List[Any]] = null,
                        rClickHouse: List[List[Any]] = null,
                        skipJDBC: Boolean = false,
                        skipClickHouse: Boolean = false): Unit =
    try {
      explainSpark(qSpark)
      if (qJDBC == null) {
        runTestWithSkip(qSpark, skipped, rSpark, rJDBC, rClickHouse, skipJDBC, skipClickHouse)
      } else {
        runTestWithoutReplaceTableName(
          qSpark,
          qJDBC,
          skipped,
          rSpark,
          rJDBC,
          rClickHouse,
          skipJDBC,
          skipClickHouse
        )
      }
    } catch {
      case e: Throwable => fail(e)
    }

  def runTest(qSpark: String,
              qJDBC: String = null,
              rSpark: List[List[Any]] = null,
              rJDBC: List[List[Any]] = null,
              rClickHouse: List[List[Any]] = null,
              skipJDBC: Boolean = false,
              skipClickHouse: Boolean = false): Unit =
    runTestWithoutReplaceTableName(
      qSpark,
      if (qJDBC == null) replaceJDBCTableName(qSpark, skipJDBC)
      else replaceJDBCTableName(qJDBC, skipJDBC),
      qSpark.contains("[skip]"),
      rSpark,
      rJDBC,
      rClickHouse,
      skipJDBC,
      skipClickHouse
    )

  def runTestWithSkip(qSpark: String,
                      skipped: Boolean = false,
                      rSpark: List[List[Any]] = null,
                      rJDBC: List[List[Any]] = null,
                      rClickHouse: List[List[Any]] = null,
                      skipJDBC: Boolean = false,
                      skipClickHouse: Boolean = false): Unit =
    runTestWithoutReplaceTableName(
      qSpark,
      replaceJDBCTableName(qSpark, skipJDBC),
      skipped,
      rSpark,
      rJDBC,
      rClickHouse,
      skipJDBC,
      skipClickHouse
    )

  /** Run test with sql `qSpark` for CHSpark and ClickHouse, `qJDBC` for Spark-JDBC. Throw fail exception when
   *    - CHSpark query throws exception
   *    - Both ClickHouse and Spark-JDBC queries fails to execute
   *    - Both ClickHouse and Spark-JDBC results differ from CHSpark result
   *
   *  rSpark, rJDBC and rClickHouse are used when we want to guarantee a fixed result which might change due to
   *    - Current incorrectness/instability in used version(s)
   *    - Format differences for partial data types
   *
   * @param qSpark    query for CHSpark and ClickHouse
   * @param qJDBC     query for Spark-JDBC
   * @param rSpark    pre-calculated CHSpark result
   * @param rJDBC     pre-calculated Spark-JDBC result
   * @param rClickHouse     pre-calculated ClickHouse result
   * @param skipJDBC  whether not to run test for Spark-JDBC
   * @param skipClickHouse  whether not to run test for ClickHouse
   */
  def runTestWithoutReplaceTableName(qSpark: String,
                                     qJDBC: String,
                                     skipped: Boolean = false,
                                     rSpark: List[List[Any]] = null,
                                     rJDBC: List[List[Any]] = null,
                                     rClickHouse: List[List[Any]] = null,
                                     skipJDBC: Boolean = false,
                                     skipClickHouse: Boolean = false): Unit = {
    if (skipped) {
      logger.warn(s"Test is skipped. [With Spark SQL: $qSpark]")
      return
    }

    var r1: List[List[Any]] = rSpark
    var r2: List[List[Any]] = rJDBC
    var r3: List[List[Any]] = rClickHouse

    val isOrdered = qSpark.toLowerCase.contains(" order by ") && !qSpark.contains("/*non-order*/")
    val isLimited = qSpark.toLowerCase.contains(" limit ")
    val hasNullOrder = qSpark.toLowerCase.contains(" nulls first") || qSpark.toLowerCase.contains(
      " nulls last"
    )

    if (isOrdered && isLimited && !hasNullOrder) {
      fail(new IllegalArgumentException("Test sql does not contain nulls order when using limit"))
    }

    if (r1 == null) {
      try {
        r1 = querySpark(qSpark)
        printR1(r1)
      } catch {
        case e: Throwable => fail(e)
      }
    }

    if (skipJDBC && skipClickHouse) {
      // If JDBC and ClickHouse tests are both skipped, the correctness of test is not guaranteed.
      // However the result might still be useful when we only want to test if the query fails in CHSpark.
      logger.warn(
        s"Unknown correctness of test result: Skipped in both JDBC and ClickHouse. [With Spark SQL: $qSpark]"
      )
      return
    }

    if (!skipJDBC && r2 == null) {
      try {
        r2 = querySpark(qJDBC)
        printR2(r2)
      } catch {
        case e: Throwable =>
          printMsg(e.getMessage)
          logger.warn(s"Spark with JDBC failed when executing:$qJDBC", e) // JDBC failed
      }
    } else {
      if (r2 == null) {
        r2 = List(List("skipped"))
      }
      printR2(r2)
    }

    val comp12 = compResult(r1, r2, isOrdered)

    val qClickHouse: String = convertSparkSQLToCHSQL(qSpark)

    if (skipJDBC || !comp12) {
      if (!skipClickHouse && r3 == null) {
        try {
          r3 = queryClickHouse(qClickHouse)
          printR3(r3)
        } catch {
          case e: Throwable =>
            printMsg(e.getMessage)
            logger.warn(s"ClickHouse failed when executing:$qClickHouse", e) // ClickHouse failed
        }
      } else {
        if (r3 == null) {
          r3 = List(List("skipped"))
        }
        printR3(r3)
      }
      val comp13 = compResult(r1, r3, isOrdered)
      if (skipClickHouse || !comp13) {
        if (truncateOutput) {
          fail(
            s"""Failed with
               |CHSpark:\t\t${StringUtils.substring(listToString(r1), 0, 200)}
               |Spark With JDBC:${StringUtils.substring(listToString(r2), 0, 200)}
               |ClickHouse:\t\t${StringUtils.substring(listToString(r3), 0, 200)}""".stripMargin
          )
        } else {
          fail(
            s"""Failed with
               |CHSpark:\t\t${listToString(r1)}
               |Spark With JDBC:${listToString(r2)}
               |ClickHouse:\t\t${listToString(r3)}""".stripMargin
          )
        }
      }
    }
  }

  private def printR1(result: List[List[Any]]) = printList("r1: ", result)

  private def printR2(result: List[List[Any]]) = printList("r2: ", result)

  private def printR3(result: List[List[Any]]) = printList("r3: ", result)

  private def printMsg(result: String): Unit =
    if (showTestOutput) {
      println(result)
    }

  private def printList(prefix: String, result: List[List[Any]]): Unit =
    if (showTestOutput) {
      println(prefix + " " + listToString(result))
    }

  private def listToString(result: List[List[Any]]): String =
    if (result == null) s"[len: null] = null"
    else s"[len: ${result.length}] = ${result.map(mapStringList).mkString(",")}"

  private def mapStringList(result: List[Any]): String =
    if (result == null) "null" else "List(" + result.map(mapString).mkString(",") + ")"

  private def mapString(result: Any): String =
    if (result == null) "null"
    else
      result match {
        case _: Array[Byte] =>
          var str = "["
          for (s <- result.asInstanceOf[Array[Byte]]) {
            str += " " + s.toString
          }
          str += " ]"
          str
        case _ =>
          result.toString
      }
}
