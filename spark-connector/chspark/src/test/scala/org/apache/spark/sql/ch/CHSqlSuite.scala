/*
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
 */

package org.apache.spark.sql.ch

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId, Expression, Literal}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}

class CHSqlSuite extends SparkFunSuite {
  val dLiteral1 = Literal(1.0, DoubleType)
  val dLiteral1EMINUS4 = Literal(0.0001, DoubleType)
  val dLiteral1E8 = Literal(100000000.0, DoubleType)
  val sLiteral = Literal("abc", StringType)
  val col1 = AttributeReference("col1", IntegerType)(exprId = ExprId(1))
  val col2 = AttributeReference("col2", IntegerType)(exprId = ExprId(2))

  def testCompileExpression(e: Expression, expected: String) : Unit =
    assert(CHSql.compileExpression(e) == expected)

  test("test literals expressions") {
    testCompileExpression(dLiteral1, "1.0")
    testCompileExpression(dLiteral1EMINUS4, "1.0E-4")
    testCompileExpression(dLiteral1E8, "1.0E8")
    testCompileExpression(sLiteral, "'abc'")
  }

  test("test arithmetic expressions") {
    testCompileExpression(col1 + col2, "(col1 + col2)")
    testCompileExpression(col1 + col2 * col1, "(col1 + (col2 * col1))")
    testCompileExpression(col1 / col2 * col1, "((col1 / col2) * col1)")
    testCompileExpression(col1 / col2 * (col1 + abs(col2)), "((col1 / col2) * (col1 + abs(col2)))")
    testCompileExpression(abs(col1), "abs(col1)")
  }

  test("test aggregate expressions") {
    testCompileExpression(sum(col1), "sum(col1)")
  }

  test("test cast expressions") {
    testCompileExpression(col1.cast(DoubleType), "(col1)")
  }
}
