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

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

import scala.collection.mutable.ArrayBuffer

class BaseClickHouseSuite extends QueryTest with SharedSQLContext {

  protected var clickHouseStmt: Statement = _

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
      .option(JDBCOptions.JDBC_URL, jdbcUrl)
      .option(JDBCOptions.JDBC_TABLE_NAME, s"`$dbName`.`$viewName`")
      .option(JDBCOptions.JDBC_DRIVER_CLASS, "ru.yandex.clickhouse.ClickHouseDriver")
      .load()
      .createOrReplaceTempView(s"`$viewName$postfix`")

  def loadTestDataFromTestTables(testTables: TestTables = defaultTestTables): Unit = {
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
    loadTestDataFromTestTables()
    initializeTimeZone()
  }

  def initializeTimeZone(): Unit =
    clickHouseStmt = clickHouseConn.createStatement()

  case class TestTables(dbName: String, tables: String*)

  private val defaultTestTables: TestTables =
    TestTables(dbName = "default", "full_data_type_table")

  def refreshConnections(testTables: TestTables): Unit = {
    super.refreshConnections()
    loadTestDataFromTestTables(testTables)
    initializeTimeZone()
  }

  override def refreshConnections(): Unit = {
    super.refreshConnections()
    loadTestDataFromTestTables()
    initializeTimeZone()
  }

  def setLogLevel(level: String): Unit =
    spark.sparkContext.setLogLevel(level)

  def replaceJDBCTableName(qSpark: String, skipJDBC: Boolean): String = {
    var qJDBC: String = null
    if (!skipJDBC) {
      if (qSpark.contains("full_data_type_table_idx")) {
        qJDBC = qSpark.replace("full_data_type_table_idx", "full_data_type_table_idx_j")
      } else if (qSpark.contains("full_data_type_table")) {
        qJDBC = qSpark.replace("full_data_type_table", "full_data_type_table_j")
      } else {
        qJDBC = qSpark
      }
    }
    qJDBC
  }

  def judge(qSpark: String, skipped: Boolean = false): Unit =
    assert(execDBTSAndJudge(qSpark, skipped))

  def convertSparkSQLToCHSQL(qSpark: String): String =
    qSpark.replace(" date(", " toDate(").replace(" date)", " Date)")

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
          println(e.getMessage)
          logger.warn(s"Spark with JDBC failed when executing:$qJDBC", e) // JDBC failed
      }
    } else {
      if (r2 == null) {
        r2 = List(List("skipped"))
      }
      printR2(r2)
    }

    val isOrdered = qSpark.contains(" order by ") && !qSpark.contains("/*non-order*/")

    val comp12 = compResult(r1, r2, isOrdered)

    val qClickHouse: String = convertSparkSQLToCHSQL(qSpark)

    if (skipJDBC || !comp12) {
      if (!skipClickHouse && r3 == null) {
        try {
          r3 = queryClickHouse(qClickHouse)
          printR3(r3)
        } catch {
          case e: Throwable =>
            println(e.getMessage)
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
