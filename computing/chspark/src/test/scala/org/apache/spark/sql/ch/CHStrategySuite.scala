/*
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
 */

package org.apache.spark.sql.ch

import org.apache.spark.sql.CHSharedSQLContext
import org.apache.spark.sql.execution.{CHScanExec, SparkPlan}

class CHStrategySuite extends CHSharedSQLContext {

  private def collectCHScans(sparkPlan: SparkPlan): Seq[CHScanExec] = sparkPlan flatMap {
    case chScan: CHScanExec => chScan :: Nil
    case plan: SparkPlan    => plan.subqueries flatMap collectCHScans
  }

  private def testCHLogicalPlan(query: String, expected: Map[String, String]): Unit = {
    val chScans = collectCHScans(spark.sql(query).queryExecution.executedPlan)
      .map(
        chScan => (chScan.chRelation.tables.head.table, chScan.chLogicalPlan)
      )
      .toMap
    assert(chScans.size == expected.size)
    expected.foreach(e => assert(chScans(e._1).toString == e._2))
  }

  private def testTimestamp(query: String, expected: Int): Unit = {
    val chScans = collectCHScans(spark.sql(query).queryExecution.executedPlan).map(
      chScan => chScan.chRelation.ts
    )
    assert(chScans.size == expected)
    val ts = chScans.head
    assert(ts.nonEmpty)
    chScans.foreach(t => assert(t == ts))
  }

  val multiNodeT = "mt"
  val multiNodeT2 = "mt2"

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("create flash database if not exists strategy_test")
    spark.sql("use strategy_test")
    spark.sql(
      s"create table if not exists $multiNodeT(mt_a int primary key, mt_b int, mt_c string, mt_d decimal(38, 10)) using mmt(128)"
    )
    spark.sql(
      s"create table if not exists $multiNodeT2(mt2_a int primary key, mt2_b int, mt2_c string, mt2_d decimal(38, 10)) using mmt(128)"
    )
  }

  protected override def afterAll(): Unit = {
    spark.sql(s"drop table $multiNodeT")
    spark.sql(s"drop table $multiNodeT2")
    spark.sql("use default")
    spark.sql("drop database strategy_test")
    super.afterAll()
  }

  test("basic plans") {
    testCHLogicalPlan(
      "select mt_a from mt",
      Map((multiNodeT, "CHLogicalPlan(project=[mt_a], filter=[], agg=[], topN=[])"))
    )
    testCHLogicalPlan(
      "select mt_a a from mt",
      Map((multiNodeT, "CHLogicalPlan(project=[mt_a AS `a`], filter=[], agg=[], topN=[])"))
    )
    testCHLogicalPlan(
      "select * from mt",
      Map(
        (multiNodeT, "CHLogicalPlan(project=[mt_a, mt_b, mt_c, mt_d], filter=[], agg=[], topN=[])")
      )
    )
    testCHLogicalPlan(
      "select mt_a from mt where mt_a = 0",
      Map(
        (
          multiNodeT,
          "CHLogicalPlan(project=[mt_a], filter=[(mt_a = 0)], agg=[], topN=[])"
        )
      )
    )
    testCHLogicalPlan(
      "select mt_a from mt where mt_b = 0",
      Map(
        (
          multiNodeT,
          "CHLogicalPlan(project=[mt_a], filter=[(mt_b IS NOT NULL), (mt_b = 0)], agg=[], topN=[])"
        )
      )
    )
    testCHLogicalPlan(
      "select mt_a from mt where mt_a in (1, 2, mt_b)",
      Map(
        (
          multiNodeT,
          "CHLogicalPlan(project=[mt_a], filter=[(mt_a IN (1, 2, mt_b))], agg=[], topN=[])"
        )
      )
    )
    testCHLogicalPlan(
      "select mt_a + 1 as a from mt where cos(mt_b) = 0",
      Map(
        (
          multiNodeT,
          "CHLogicalPlan(project=[(mt_a + 1) AS `a`, mt_b], filter=[], agg=[], topN=[])"
        )
      )
    )
    testCHLogicalPlan(
      "select cast(cos(mt_a) as string) from mt",
      Map(
        (
          multiNodeT,
          "CHLogicalPlan(project=[mt_a], filter=[], agg=[], topN=[])"
        )
      )
    )
    // Testing hack.
    testCHLogicalPlan(
      "select cast(cast(mt_a as String) as date) from mt",
      Map(
        (
          multiNodeT,
          "CHLogicalPlan(project=[CAST(CAST(mt_a AS STRING) AS DATE) AS `CAST(CAST(mt_a AS STRING) AS DATE)`], filter=[], agg=[], topN=[])"
        )
      )
    )
    testCHLogicalPlan(
      "select cast(cast(mt_a as String) as date) a from mt",
      Map(
        (
          multiNodeT,
          "CHLogicalPlan(project=[CAST(CAST(mt_a AS STRING) AS DATE) AS `a`], filter=[], agg=[], topN=[])"
        )
      )
    )
    testCHLogicalPlan(
      "select ifNull(mt_a, mt_b + 1) from mt",
      Map(
        (
          multiNodeT,
          "CHLogicalPlan(project=[coalesce(mt_a, (mt_b + 1)) AS `ifnull(mt.``mt_a``, (mt.``mt_b`` + 1))`], filter=[], agg=[], topN=[])"
        )
      )
    )
  }

  test("filter plans") {
    // Predicate LIKE not pushing down, checking if column mt_b is correctly pushed.
    testCHLogicalPlan(
      "select mt_a from mt where MT_B like '%WHATEVER'",
      Map(
        (
          multiNodeT,
          "CHLogicalPlan(project=[mt_a, MT_B], filter=[(MT_B IS NOT NULL)], agg=[], topN=[])"
        )
      )
    )
  }

  test("multi-node aggregate plans") {
    testCHLogicalPlan(
      "select sum(mt_a) from mt",
      Map(
        (
          multiNodeT,
          "CHLogicalPlan(project=[sum(CAST(mt_a AS BIGINT))], filter=[], agg=[[sum(CAST(mt_a AS BIGINT))]], topN=[])"
        )
      )
    )
    testCHLogicalPlan(
      "select sum(mt_a) AS sum_mt_a from mt",
      Map(
        (
          multiNodeT,
          "CHLogicalPlan(project=[sum(CAST(mt_a AS BIGINT))], filter=[], agg=[[sum(CAST(mt_a AS BIGINT))]], topN=[])"
        )
      )
    )
    testCHLogicalPlan(
      "select sum(mt_a) + sum(mt_b) from mt",
      Map(
        (
          multiNodeT,
          "CHLogicalPlan(project=[sum(CAST(mt_a AS BIGINT)), sum(CAST(mt_b AS BIGINT))], filter=[], agg=[[sum(CAST(mt_a AS BIGINT)), sum(CAST(mt_b AS BIGINT))]], topN=[])"
        )
      )
    )
    testCHLogicalPlan(
      "select sum(mt_a) + sum(mt_b) AS sum_mt_a_mt_b from mt",
      Map(
        (
          multiNodeT,
          "CHLogicalPlan(project=[sum(CAST(mt_a AS BIGINT)), sum(CAST(mt_b AS BIGINT))], filter=[], agg=[[sum(CAST(mt_a AS BIGINT)), sum(CAST(mt_b AS BIGINT))]], topN=[])"
        )
      )
    )
    testCHLogicalPlan(
      "select avg(mt_a) from mt",
      Map(
        (
          multiNodeT,
          "CHLogicalPlan(project=[sum(CAST(mt_a AS BIGINT)), count(CAST(mt_a AS BIGINT))], filter=[], agg=[[sum(CAST(mt_a AS BIGINT)), count(CAST(mt_a AS BIGINT))]], topN=[])"
        )
      )
    )
    testCHLogicalPlan(
      "select avg(mt_a) + avg(mt_b) from mt",
      Map(
        (
          multiNodeT,
          "CHLogicalPlan(project=[sum(CAST(mt_a AS BIGINT)), count(CAST(mt_a AS BIGINT)), sum(CAST(mt_b AS BIGINT)), count(CAST(mt_b AS BIGINT))], filter=[], agg=[[sum(CAST(mt_a AS BIGINT)), count(CAST(mt_a AS BIGINT)), sum(CAST(mt_b AS BIGINT)), count(CAST(mt_b AS BIGINT))]], topN=[])"
        )
      )
    )
    testCHLogicalPlan(
      "select count(mt_a) from mt where cos(mt_b) = 0",
      Map((multiNodeT, "CHLogicalPlan(project=[mt_b], filter=[], agg=[], topN=[])"))
    )
    testCHLogicalPlan(
      "select sum(distinct mt_a) from mt",
      Map((multiNodeT, "CHLogicalPlan(project=[mt_a], filter=[], agg=[], topN=[])"))
    )
    testCHLogicalPlan(
      "select sum(mt_d) from mt",
      Map(
        (multiNodeT, "CHLogicalPlan(project=[sum(mt_d)], filter=[], agg=[[sum(mt_d)]], topN=[])")
      )
    )
    testCHLogicalPlan(
      "select sum(mt_d + 1) from mt",
      Map(
        (
          multiNodeT,
          "CHLogicalPlan(project=[sum((CAST(mt_d AS DECIMAL(38,9)) + 1.000000000))], filter=[], agg=[[sum((CAST(mt_d AS DECIMAL(38,9)) + 1.000000000))]], topN=[])"
        )
      )
    )
  }

  test("top-n plans") {
    testCHLogicalPlan(
      "select mt_a from mt order by mt_a",
      Map((multiNodeT, "CHLogicalPlan(project=[mt_a], filter=[], agg=[], topN=[])"))
    )
    testCHLogicalPlan(
      "select mt_a from mt limit 1",
      Map((multiNodeT, "CHLogicalPlan(project=[mt_a], filter=[], agg=[], topN=[1])"))
    )
    testCHLogicalPlan(
      "select mt_a from mt order by mt_a limit 1",
      Map(
        (
          multiNodeT,
          "CHLogicalPlan(project=[mt_a], filter=[], agg=[], topN=[[mt_a ASC NULLS FIRST], 1])"
        )
      )
    )
    testCHLogicalPlan(
      "select mt_a from mt order by mt_a asc, mt_b desc limit 1",
      Map(
        (
          multiNodeT,
          "CHLogicalPlan(project=[mt_a, mt_b], filter=[], agg=[], topN=[[mt_a ASC NULLS FIRST, mt_b DESC NULLS LAST], 1])"
        )
      )
    )
    testCHLogicalPlan(
      "select mt_a from mt order by (mt_a, mt_b) desc limit 1",
      Map(
        (
          multiNodeT,
          "CHLogicalPlan(project=[mt_a, mt_b], filter=[], agg=[], topN=[[named_struct(mt_a, mt_a, mt_b, mt_b) DESC NULLS LAST], 1])"
        )
      )
    )
  }

  test("subquery timestamps") {
    testTimestamp(
      "select 1 from mt t1 join (select count(*) as c from mt2) t2 on t1.mt_a = t2.c",
      2
    )
    testTimestamp("select 1 from mt where (select count(*) from mt2) > mt_a", 2)
    testTimestamp(
      "select 1 from mt where (select count(*) from mt2 where (select count(*) from mt) > 0) > mt_a",
      3
    )
  }
}
