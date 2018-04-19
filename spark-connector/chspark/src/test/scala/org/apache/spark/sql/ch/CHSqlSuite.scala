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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, Expression, Literal}
import org.apache.spark.sql.types.DoubleType

class CHSqlSuite extends SparkFunSuite {
  val nullLiteral = Literal(null, null)
  val one = 1.0
  val oneEMINUS4 = 0.0001
  val oneE8 = 100000000.0
  val abc = "abc"
  val a = 'a.int
  val b = 'b.int
  val c = 'c.string
  val t = new CHTableRef(null, 0, "d", "t")

  def testCompileExpression(e: Expression, expected: String) : Unit =
    assert(CHSql.compileExpression(e) == expected)

  def testQuery(table: CHTableRef, chLogicalPlan: CHLogicalPlan, expected: String) : Unit =
    assert(CHSql.query(table, chLogicalPlan) == expected)

  test("null check expressions") {
    testCompileExpression(a.isNull, "a IS NULL")
    testCompileExpression(b.isNotNull, "b IS NOT NULL")
    testCompileExpression(!b.isNotNull, "NOT b IS NOT NULL")
  }

  test("literals expressions") {
    testCompileExpression(nullLiteral, "NULL")
    testCompileExpression(one, "1.0")
    testCompileExpression(oneEMINUS4, "1.0E-4")
    testCompileExpression(oneE8, "1.0E8")
    testCompileExpression(abc, "'abc'")
  }

  test("arithmetic expressions") {
    testCompileExpression(a + b, "(a + b)")
    testCompileExpression(-(a + b), "-(a + b)")
    testCompileExpression(a + b * -a, "(a + (b * -a))")
    testCompileExpression(a / b * a, "((a / b) * a)")
    testCompileExpression(a / b * (a - abs(b)), "((a / b) * (a - ABS(b)))")
    testCompileExpression(a - b * (a / abs(b + a % 100)), "(a - (b * (a / ABS((b + (a % 100))))))")
  }

  test("comparison expressions") {
    testCompileExpression(a === b, "(a = b)")
    testCompileExpression(a > b, "(a > b)")
    testCompileExpression(a >= b, "(a >= b)")
    testCompileExpression(a < b, "(a < b)")
    testCompileExpression(a <= b, "(a <= b)")
  }

  test("logical expressions") {
    testCompileExpression(!(a + b <= b), "NOT ((a + b) <= b)")
    testCompileExpression(a + b <= b && a > b, "(((a + b) <= b) AND (a > b))")
    testCompileExpression(a <= b || a + b > b, "((a <= b) OR ((a + b) > b))")
    testCompileExpression(a || a && !b || c, "((a OR (a AND NOT b)) OR c)")
    testCompileExpression((a || b) && !(b || c), "((a OR b) AND NOT (b OR c))")
  }

  test("aggregate expressions") {
    testCompileExpression(avg(a), "AVG(a)")
    testCompileExpression(count(a), "COUNT(a)")
    testCompileExpression(max(a), "MAX(a)")
    testCompileExpression(min(a), "MIN(a)")
    testCompileExpression(sum(a), "SUM(a)")
  }

  test("cast expressions") {
    testCompileExpression(a.cast(DoubleType), "a")
  }

  test("empty query") {
    testQuery(t, CHLogicalPlan(
      Seq.empty, Seq.empty, Seq.empty, Seq.empty, Seq.empty, None
    ), "SELECT  FROM d.t")
  }

  test("project query") {
    testQuery(t, CHLogicalPlan(
      Seq(a, b, c),
      Seq.empty, Seq.empty, Seq.empty, Seq.empty, None
    ), "SELECT a, b, c FROM d.t")
  }

  test("filter query") {
    testQuery(t, CHLogicalPlan(
      Seq.empty,
      Seq(a, b, nullLiteral.isNotNull),
      Seq.empty, Seq.empty, Seq.empty, None
    ), "SELECT  FROM d.t WHERE ((a AND b) AND NULL IS NOT NULL)")
  }

  test("aggregate query") {
    testQuery(t, CHLogicalPlan(
      Seq(a, b, sum(c)),
      Seq.empty,
      Seq(a, b),
      Seq.empty, Seq.empty, None
    ), "SELECT a, b, SUM(c) FROM d.t GROUP BY a, b")
  }

  test("top-n query") {
    testQuery(t, CHLogicalPlan(
      Seq.empty, Seq.empty, Seq.empty, Seq.empty,
      Seq(a desc, b asc),
      None
    ), "SELECT  FROM d.t ORDER BY a DESC, b ASC")
    testQuery(t, CHLogicalPlan(
      Seq.empty, Seq.empty, Seq.empty, Seq.empty,
      Seq(a asc, CreateNamedStruct(Seq(b.name, b, "col1", a + b, "col2", b + c)) desc, c asc),
      None
    ), "SELECT  FROM d.t ORDER BY a ASC, (b, (a + b), (b + c)) DESC, c ASC")
    testQuery(t, CHLogicalPlan(
      Seq.empty, Seq.empty, Seq.empty, Seq.empty, Seq.empty,
      Option(1)
    ), "SELECT  FROM d.t LIMIT 1")
    testQuery(t, CHLogicalPlan(
      Seq.empty, Seq.empty, Seq.empty, Seq.empty,
      Seq(a desc, b asc),
      Option(1)
    ), "SELECT  FROM d.t ORDER BY a DESC, b ASC LIMIT 1")
  }
}
