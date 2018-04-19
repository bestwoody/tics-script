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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.execution.CHScanExec
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StructType

class CHStrategySuite extends SharedSQLContext {
  class TestCHRelation(name: String, singleNode: Boolean, output: Attribute*) extends CHRelation(
    if (singleNode) { Seq(null) } else { Seq.empty },
    0, 0, 0)(sqlContext, null) {
    val localRelation = LocalRelation(output)
    override lazy val schema: StructType = localRelation.schema

    sqlContext.baseRelationToDataFrame(this).createTempView(name)
  }

  private def testQuery(query: String, expected: Map[TestCHRelation, String]) = {
    val plans = spark.sql(query).queryExecution.sparkPlan.collect {
      case chScanExec: CHScanExec => chScanExec
    }.map(chScanExec => chScanExec.chRelation match {
      case testCHRelation: TestCHRelation => (testCHRelation, chScanExec.chLogicalPlan)
    }).toMap
    assert(plans.size == expected.size)
    expected.foreach(e =>
      assert(plans(e._1).toString == e._2)
    )
  }

  var multiNodeT: TestCHRelation = _
  var singleNodeT: TestCHRelation = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    spark.experimental.extraStrategies = new CHStrategy(spark) :: Nil
    multiNodeT = new TestCHRelation("mt", false, 'mt_a.int, 'mt_b.int, 'mt_c.string)
    singleNodeT = new TestCHRelation("st", true, 'st_a.int, 'st_b.int, 'st_c.string)
  }

  protected override def afterAll(): Unit = {
    spark.experimental.extraStrategies = Nil
    super.afterAll()
  }

  test("basic plan") {
    testQuery("select mt_a from mt", Map((
      multiNodeT, "CH plan [Project [mt_a#3], Filter [], Aggregate [], TopN []]")))
    testQuery("select mt_a a from mt", Map((
      multiNodeT, "CH plan [Project [mt_a#3], Filter [], Aggregate [], TopN []]")))
    testQuery("select * from mt", Map((
      multiNodeT, "CH plan [Project [mt_a#3, mt_b#4, mt_c#5], Filter [], Aggregate [], TopN []]")))
    testQuery("select mt_a from mt where mt_a = 0", Map((
      multiNodeT, "CH plan [Project [mt_a#3], Filter [isnotnull(mt_a#3), (mt_a#3 = 0)], Aggregate [], TopN []]")))
    testQuery("select mt_a from mt where mt_b = 0", Map((
      multiNodeT, "CH plan [Project [mt_a#3, mt_b#4], Filter [isnotnull(mt_b#4), (mt_b#4 = 0)], Aggregate [], TopN []]")))
  }

  test("multi-node aggregate plan") {
    testQuery("select sum(mt_a) from mt", Map((
      multiNodeT, "CH plan [Project [sum(cast(mt_a#3 as bigint))], Filter [], Aggregate [[sum(cast(mt_a#3 as bigint))]], TopN []]")))
    testQuery("select sum(mt_a) as sum_mt_a from mt", Map((
      multiNodeT, "CH plan [Project [sum(cast(mt_a#3 as bigint))], Filter [], Aggregate [[sum(cast(mt_a#3 as bigint))]], TopN []]")))
    testQuery("select sum(mt_a) + sum(mt_b) from mt", Map((
      multiNodeT, "CH plan [Project [sum(cast(mt_a#3 as bigint)), sum(cast(mt_b#4 as bigint))], Filter [], Aggregate [[sum(cast(mt_a#3 as bigint)), sum(cast(mt_b#4 as bigint))]], TopN []]")))
    testQuery("select sum(mt_a) + sum(mt_b) as sum_mt_a_mt_b from mt", Map((
      multiNodeT, "CH plan [Project [sum(cast(mt_a#3 as bigint)), sum(cast(mt_b#4 as bigint))], Filter [], Aggregate [[sum(cast(mt_a#3 as bigint)), sum(cast(mt_b#4 as bigint))]], TopN []]")))
    testQuery("select avg(mt_a) from mt", Map((
      multiNodeT, "CH plan [Project [sum(cast(mt_a#3 as bigint)), count(cast(mt_a#3 as bigint))], Filter [], Aggregate [[sum(cast(mt_a#3 as bigint)), count(cast(mt_a#3 as bigint))]], TopN []]")))
    testQuery("select avg(mt_a) + avg(mt_b) from mt", Map((
      multiNodeT, "CH plan [Project [sum(cast(mt_a#3 as bigint)), count(cast(mt_a#3 as bigint)), sum(cast(mt_b#4 as bigint)), count(cast(mt_b#4 as bigint))], Filter [], Aggregate [[sum(cast(mt_a#3 as bigint)), count(cast(mt_a#3 as bigint)), sum(cast(mt_b#4 as bigint)), count(cast(mt_b#4 as bigint))]], TopN []]")))
  }

  test("single-node aggregate plan") {
    testQuery("select sum(st_a) from st", Map((
      singleNodeT, "CH plan [Project [sum(cast(st_a#14 as bigint))], Filter [], Aggregate [[sum(cast(st_a#14 as bigint))]], TopN []]")))
    testQuery("select sum(st_a) as sum_st_a from st", Map((
      singleNodeT, "CH plan [Project [sum(cast(st_a#14 as bigint))], Filter [], Aggregate [[sum(cast(st_a#14 as bigint))]], TopN []]")))
    testQuery("select sum(st_a) + sum(st_b) from st", Map((
      singleNodeT, "CH plan [Project [(sum(cast(st_a#14 as bigint)) + sum(cast(st_b#15 as bigint)))], Filter [], Aggregate [[sum(cast(st_a#14 as bigint)), sum(cast(st_b#15 as bigint))]], TopN []]")))
    testQuery("select sum(st_a) + sum(st_b) as sum_mt_a_mt_b from st", Map((
      singleNodeT, "CH plan [Project [(sum(cast(st_a#14 as bigint)) + sum(cast(st_b#15 as bigint)))], Filter [], Aggregate [[sum(cast(st_a#14 as bigint)), sum(cast(st_b#15 as bigint))]], TopN []]")))
    testQuery("select avg(st_a) as avg_st_a from st", Map((
      singleNodeT, "CH plan [Project [avg(cast(st_a#14 as bigint))], Filter [], Aggregate [[avg(cast(st_a#14 as bigint))]], TopN []]")))
    testQuery("select avg(st_a) + avg(st_b) from st", Map((
      singleNodeT, "CH plan [Project [(avg(cast(st_a#14 as bigint)) + avg(cast(st_b#15 as bigint)))], Filter [], Aggregate [[avg(cast(st_a#14 as bigint)), avg(cast(st_b#15 as bigint))]], TopN []]")))
  }

  test("top-n plan") {
    testQuery("select mt_a from mt order by mt_a", Map((
      multiNodeT, "CH plan [Project [mt_a#3], Filter [], Aggregate [], TopN []]")))
    testQuery("select mt_a from mt limit 1", Map((
      multiNodeT, "CH plan [Project [mt_a#3], Filter [], Aggregate [], TopN [1]]")))
    testQuery("select mt_a from mt order by mt_a limit 1", Map((
      multiNodeT, "CH plan [Project [mt_a#3], Filter [], Aggregate [], TopN [[mt_a#3 ASC NULLS FIRST], 1]]")))
    testQuery("select mt_a from mt order by mt_a asc, mt_b desc limit 1", Map((
      multiNodeT, "CH plan [Project [mt_a#3, mt_b#4], Filter [], Aggregate [], TopN [[mt_a#3 ASC NULLS FIRST, mt_b#4 DESC NULLS LAST], 1]]")))
    testQuery("select mt_a from mt order by (mt_a, mt_b) desc limit 1", Map((
      multiNodeT, "CH plan [Project [mt_a#3, mt_b#4], Filter [], Aggregate [], TopN [[named_struct(mt_a, mt_a#3, mt_b, mt_b#4) DESC NULLS LAST], 1]]")))
  }
}
