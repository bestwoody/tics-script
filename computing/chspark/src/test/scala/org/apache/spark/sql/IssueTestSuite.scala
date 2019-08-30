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
  test("Fix empty project list on oneRowRelation") {
    spark.sql("use chspark_test")
    spark.sql("select 1 as c").createOrReplaceTempView("t")
    explainAndRunTest("select count(*) from t cross join full_data_type_table f on t.c = f.tp_int8")
  }

  test("case when tests") {
    explainAndRunTest(
      "select CASE WHEN tp_int8 < 0 AND tp_uint8 % 2 = 0 THEN tp_string ELSE '' END AS src from full_data_type_table order by id_dt",
      skipJDBC = true
    )
    explainAndRunTest(
      "select CASE WHEN tp_int8 < 0 AND tp_uint8 % 2 = 0 THEN tp_string WHEN tp_int8 > 0 THEN 'positive' ELSE 'undefined' END from full_data_type_table order by id_dt",
      skipJDBC = true
    )
    explainAndRunTest(
      "select CASE WHEN tp_int8 < 0 AND cos(tp_uint8) = 0 THEN tp_string WHEN tp_int8 > 0 THEN 'positive' END from full_data_type_table order by id_dt",
      qClickHouse = Option.apply(
        "select CASE WHEN tp_int8 < 0 AND cos(tp_uint8) = 0 THEN tp_string WHEN tp_int8 > 0 THEN 'positive' ELSE null END from full_data_type_table order by id_dt"
      ),
      skipJDBC = true
    )
    // The following case is treated respectively because CH has different behaviours concerning the following cases:
    //
    // Scenario I : Case when (a = 0) then 'y' else 'n' end  ----- a is null ---->  'n'
    // Scenario II: Case a when 0 then 'y' else 'n' end      ----- a is null ---->  null
    //
    // In Spark, both scenarios return 'n'.
    // However, since we only push down Scenario I, which is also what Spark behaves, the correctness is still guaranteed.
    explainAndRunTest(
      "select CASE tp_boolean WHEN 0 THEN 'ok' ELSE 'wtf' END AS src from full_data_type_table order by tp_boolean nulls first, id_dt limit 5",
      skipJDBC = true,
      rClickHouse = List(List("wtf"), List("ok"), List("ok"), List("ok"), List("ok"))
    )
  }

  test("pushdown ifNull test") {
    explainAndRunTest(
      "select ifNull(tp_int8, tp_int8 + 1) from full_data_type_table order by id_dt",
      skipJDBC = true
    )
    explainAndRunTest(
      "select ifNull(tp_int8, 1) from full_data_type_table order by id_dt",
      skipJDBC = true
    )
  }

  test("complex plan tests") {
    explainAndRunTest(
      "select avg(tp_int8) from full_data_type_table group by tp_uint8",
      skipJDBC = true
    )
    explainAndRunTest(
      "select tp_int8 from full_data_type_table order by id_dt nulls first limit 10",
      skipJDBC = true
    )
    explainAndRunTest(
      "select tp_int8 from full_data_type_table where TP_STRING like 'a%'",
      skipJDBC = true
    )
    explainAndRunTest(
      "select tp_int8, tp_int8 + 1, cos(tp_int8), cos(tp_int32), tp_float32 as a from full_data_type_table where cos(tp_int16) > 0 and tp_float32 + 2 > 0 and tp_float64 < tp_float32 order by id_dt nulls first",
      skipJDBC = true
    )
  }

  ignore("#438 NPE when decimal is NOT NULL") {
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

  test("FLASH-451 Alias mis-resolve") {
    clickHouseStmt.execute("DROP TABLE IF EXISTS flash_451")
    clickHouseStmt.execute(
      """CREATE TABLE flash_451(i1 Int32, i2 Int32, i3 Int32) ENGINE = MutableMergeTree(i1, 8192)"""
    )
    clickHouseStmt.execute(
      """INSERT INTO TABLE flash_451 VALUES(1, 1, 2), (2, 2, 3)"""
    )
    refreshConnections()

    spark.sql("select * from flash_451").show(false)
    runTest(
      "select i1 as i3, i3 as i1 from flash_451 where i3 = 2",
      skipJDBC = true,
      rClickHouse = List(List(1, 2))
    )
    runTest(
      "select i2 as i3, i3 as i2 from flash_451 where i3 = 2",
      skipJDBC = true,
      rClickHouse = List(List(1, 2))
    )
    runTest(
      "select i1 as i3, i3 as i1 from flash_451 where i1 = 2",
      skipJDBC = true,
      rClickHouse = List(List(2, 3))
    )
    runTest(
      "select i2 as i3, i3 as i2 from flash_451 where i2 = 2",
      skipJDBC = true,
      rClickHouse = List(List(2, 3))
    )
  }

  override def afterAll(): Unit =
    try {
      clickHouseStmt.execute("DROP TABLE IF EXISTS test")
      clickHouseStmt.execute("DROP TABLE IF EXISTS flash_451")
    } finally {
      super.afterAll()
    }
}
