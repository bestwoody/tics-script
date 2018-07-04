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

package org.apache.spark.sql.test

import java.sql.{Connection, Statement}
import java.util.{Locale, Properties, TimeZone}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.test.TestConstants._
import org.apache.spark.sql.test.Utils._
import org.apache.spark.sql.{CHContext, CHTestContext, SQLContext, SparkSession}
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.joda.time.DateTimeZone
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.slf4j.Logger
import ru.yandex.clickhouse.ClickHouseDataSource
import ru.yandex.clickhouse.settings.ClickHouseProperties

/**
 * This trait manages basic CHSpark, Spark JDBC, ClickHouse JDBC
 * connection resource and relevant configurations.
 *
 * `clickhouse_config.properties` must be provided in test resources folder
 */
trait SharedSQLContext extends SparkFunSuite with Eventually with BeforeAndAfterAll with Logging {
  protected val logger: Logger = log

  protected def spark: SparkSession = SharedSQLContext.spark

  protected def ch: CHTestContext = SharedSQLContext.ch

  protected def jdbc: SparkSession = SharedSQLContext.jdbc

  protected def clickHouseConn: Connection = SharedSQLContext.clickHouseConn

  protected def sql = spark.sql _

  protected def jdbcUrl: String = SharedSQLContext.jdbcUrl

  protected def tpchDBName: String = SharedSQLContext.tpchDBName

  protected def timeZoneOffset: String = SharedSQLContext.timeZoneOffset

  protected def refreshConnections(): Unit = SharedSQLContext.refreshConnections()

  /**
   * The [[TestSQLContext]] to use for all tests in this suite.
   */
  protected implicit def sqlContext: SQLContext = spark.sqlContext

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    try {
      SharedSQLContext.init()
    } catch {
      case e: Throwable =>
        fail(
          s"Failed to initialize SQLContext:${e.getMessage}, please check your ClickHouse and Spark configuration",
          e
        )
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    try {
      SharedSQLContext.stop()
    } catch {
      case e: Throwable =>
        fail(
          s"Failed to stop SQLContext:${e.getMessage}, please check your ClickHouse and Spark configuration",
          e
        )
    }
  }
}

object SharedSQLContext extends Logging {
  private val timeZoneOffset = "+0:00"
  // Timezone is fixed to Asia/Shanghai for those timezone sensitive tests (timestamp_*, date_*, etc)
  private val timeZone = TimeZone.getTimeZone(s"GMT$timeZoneOffset")
  // JDK time zone
  TimeZone.setDefault(timeZone)
  // Joda time zone
  DateTimeZone.setDefault(DateTimeZone.forTimeZone(timeZone))
  // Add Locale setting
  Locale.setDefault(Locale.CHINA)

  protected val logger: Logger = log
  protected val sparkConf = new SparkConf()
  private var _spark: SparkSession = _
  private var _ch: CHTestContext = _
  private var _clickHouseConf: Properties = _
  private var _clickHouseConnection: Connection = _
  private var _statement: Statement = _
  private var _sparkJDBC: SparkSession = _
  protected var jdbcUrl: String = _
  protected var tpchDBName: String = _

  protected lazy val sql = spark.sql _

  protected implicit def spark: SparkSession = _spark

  protected implicit def ch: CHTestContext = _ch

  protected implicit def jdbc: SparkSession = _sparkJDBC

  protected implicit def clickHouseConn: Connection = _clickHouseConnection

  protected implicit def clickHouseStmt: Statement = _statement

  /**
   * The [[TestSQLContext]] to use for all tests in this suite.
   */
  protected implicit def sqlContext: SQLContext = _spark.sqlContext

  protected var _sparkSession: SparkSession = _

  def refreshConnections(): Unit = {
    stop()
    init(true)
  }

  /**
   * Initialize the [[TestSparkSession]].  Generally, this is just called from
   * beforeAll; however, in test using styles other than FunSuite, there is
   * often code that relies on the session between test group constructs and
   * the actual tests, which may need this session.  It is purely a semantic
   * difference, but semantically, it makes more sense to call
   * 'initializeSession' between a 'describe' and an 'it' call than it does to
   * call 'beforeAll'.
   */
  protected def initializeSession(): Unit = {
    if (_spark == null) {
      _spark = _sparkSession
    }
  }

  private def initializeJDBC(): Unit = {
    if (_sparkJDBC == null) {
      _sparkJDBC = _sparkSession
    }
  }

  protected def initializeCHContext(): Unit = {
    if (_spark != null && _ch == null) {
      _ch = new CHTestContext(_spark)
    }
  }

  private def initializeClickHouse(forceNotLoad: Boolean = false): Unit = {
    if (_clickHouseConnection == null) {

      val jdbcHostname = getOrElse(_clickHouseConf, CLICKHOUSE_ADDRESS, "127.0.0.1")

      val jdbcPort = Integer.parseInt(getOrElse(_clickHouseConf, CLICKHOUSE_PORT, "8123"))

      val loadData = getOrElse(_clickHouseConf, SHOULD_LOAD_DATA, "true").toBoolean

      jdbcUrl = s"jdbc:clickhouse://$jdbcHostname:$jdbcPort"

      val properties = new ClickHouseProperties()

      properties.setConnectionTimeout(100)

      val dataSource = new ClickHouseDataSource(jdbcUrl, properties)

      try {
        _clickHouseConnection = dataSource.getConnection()
        _statement = _clickHouseConnection.createStatement()
      } catch {
        case e: Throwable =>
          _statement.close()
          _clickHouseConnection.close()
          throw e
      }

      if (loadData && !forceNotLoad) {
        logger.warn("Loading CHSparkTestData")
        var queryStringList = Array.empty[String]
        // Load expression test data
        queryStringList = resourceToString(
          s"chspark-test/chspark-test.sql",
          classLoader = Thread.currentThread().getContextClassLoader
        ).split("\n")
        queryStringList.foreach { sql =>
          while (try {
                   _statement.executeUpdate(sql)
                   false
                 } catch {
                   case e: Throwable =>
                     println(e)
                     Thread.sleep(2000)
                     true
                 }) {}
        }
        logger.warn("Loading CHSparkTest.sql successfully.")
      }
    }
  }

  private def initializeConf(): Unit = {
    if (_clickHouseConf == null) {
      val confStream = Thread
        .currentThread()
        .getContextClassLoader
        .getResourceAsStream("clickhouse_config.properties")

      val prop = new Properties()
      if (confStream != null) {
        prop.load(confStream)
      }

      tpchDBName = getOrElse(prop, TPCH_DB_NAME, "default")
      _clickHouseConf = prop
      _sparkSession = new TestSparkSession(sparkConf)
    }
  }

  /**
   * Make sure the [[TestSparkSession]] is initialized before any tests are run.
   */
  def init(forceNotLoad: Boolean = false): Unit = {
    stop()
    initializeConf()
    initializeSession()
    initializeClickHouse(forceNotLoad)
    initializeCHContext()
    initializeJDBC()
  }

  /**
   * Stop the underlying resources, if any.
   */
  def stop(): Unit = {
    if (_spark != null) {
      _spark.sessionState.catalog.reset()
      _spark.stop()
      _spark = null
    }

    if (_sparkJDBC != null) {
      _sparkJDBC.sessionState.catalog.reset()
      _sparkJDBC.stop()
      _sparkJDBC = null
    }

    if (_statement != null) {
      _statement.close()
      _statement = null
    }

    if (_clickHouseConnection != null) {
      _clickHouseConnection.close()
      _clickHouseConnection = null
    }

    if (_ch != null) {
      _ch.sparkSession.sessionState.catalog.reset()
      _ch = null
    }

    if (_clickHouseConf != null) {
      _clickHouseConf = null
    }
  }
}
