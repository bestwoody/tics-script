/*
 *
 * Copyright 2018 PingCAP, Inc.
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

class IssueTestSuite extends BaseClickHouseSuite {
  test("$438 NPE when decimal is NOT NULL") {
    clickHouseStmt.execute("DROP TABLE IF EXISTS test")
    clickHouseStmt.execute(
      """CREATE TABLE test ( d Decimal(65, 20), nd Nullable(Decimal(65, 20)), md Nullable(Decimal(65, 30)), bd Nullable(Decimal(37, 10))) ENGINE = MutableMergeTree(d, 8192)"""
    )
    clickHouseStmt.execute(
      """INSERT INTO TABLE test VALUES (355000000001000000002000000003000000004.000000050000000000030, null, null, 500000000000000000000000000.0000000005)"""
    )
    clickHouseStmt.execute(
      """INSERT INTO TABLE test VALUES (255000000001000000002000000003000000004.00000005, 255000000001000000002000000003000000004.00000005, 255000000001000000002000000003.00000000400000005, 700000000000000000000000000.0000000005)"""
    )
    clickHouseStmt.execute("INSERT INTO TABLE test VALUES (100, 100, 100, null)")
    refreshConnections()

    spark.sql("select * from test").show(false)
    val df = spark.sql("select sum(d), sum(nd), sum(md), sum(bd) from test")
    df.show(false)
    df.printSchema
    runTest("select sum(d), sum(nd), sum(bd) from test", skipJDBC = true)
    // 100.000 under Decimal(65,20) will be converted into
    // Spark Decimal(38,-7), thus it will be truncated into 0E+7.
    // This is now considered a normal behavior.
    runTest(
      "select * from test",
      skipJDBC = true,
      rClickHouse = List(
        List(0E+7, 0E+7, 100.000, null),
        List(
          2.5500000000100000000200000000300E+38,
          2.5500000000100000000200000000300E+38,
          255000000001000000002000000003.000,
          700000000000000000000000000.0000000005
        ),
        List(
          3.5500000000100000000200000000300E+38,
          null,
          null,
          500000000000000000000000000.0000000005
        )
      )
    )
  }

  test("#413 Count distinct has an incorrect plan") {
    runTest("select sum(distinct tp_int8) from full_data_type_table")
    runTest("select count(distinct tp_int8) from full_data_type_table")
    runTest("select avg(distinct tp_int8) from full_data_type_table")
  }

  test("#235 Comparison incorrect when TimeStamp cast to Date in predicates") {
    explainAndRunTest(
      "SELECT cast(tp_datetime as date) cast_datetime, date(tp_datetime) date_datetime, tp_datetime FROM full_data_type_table WHERE date(tp_datetime) > date('2009-01-02')"
    )
  }

  test("#544 New Date/DateTime does not deal with TimeZone") {
    explainAndRunTest(
      "SELECT cast(tp_datetime as date) cast_datetime, date(tp_datetime) date_datetime, tp_datetime FROM full_data_type_table"
    )
  }

  override def afterAll(): Unit =
    try {
      clickHouseStmt.execute("DROP TABLE IF EXISTS test")
    } finally {
      super.afterAll()
    }
}
