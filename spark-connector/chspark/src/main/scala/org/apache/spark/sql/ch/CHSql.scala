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

import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Abs, Add, And, AttributeReference, Cast, CreateNamedStruct, Divide, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, Multiply, Not, Or, Remainder, Subtract, UnaryMinus}
import org.apache.spark.sql.types.StringType

/**
 * Compiler that compiles CHLogicalPlan/CHTableRef to CH SQL string.
 */
object CHSql {
  /**
    * Compose a query string based on input table and chLogical.
    * @param table
    * @param chLogicalPlan
    * @param useSelraw
    * @return
    */
  def query(table: CHTableRef, chLogicalPlan: CHLogicalPlan, useSelraw: Boolean = false): String = {
    compileProject(chLogicalPlan.chProject, useSelraw) +
    compileTable(table) +
    compileFilter(chLogicalPlan.chFilter) +
    compileAggregate(chLogicalPlan.chAggregate) +
    compileTopN(chLogicalPlan.chTopN)
  }

  /**
    * Compose a desc table string.
    * @param table
    * @return
    */
  def desc(table: CHTableRef): String = {
    "DESC " + table.absName
  }

  /**
    * Compose a count(*) SQL string.
    * @param table
    * @param useSelraw
    * @return
    */
  def count(table: CHTableRef, useSelraw: Boolean = false): String = {
    (if (useSelraw) "SELRAW" else "SELECT") + " COUNT(*) FROM " + table.absName
  }

  private def compileProject(chProject: CHProject, useSelraw: Boolean): String = {
    (if (useSelraw) "SELRAW " else "SELECT ") + chProject.projectList.map(compileExpression)
      .mkString(", ")
  }

  private def compileTable(table: CHTableRef): String = {
    " FROM " + table.absName
  }

  private def compileFilter(chFilter: CHFilter): String = {
    if (chFilter.predicates.isEmpty) ""
    else " WHERE " + chFilter.predicates.reduceLeftOption(And).map(compileExpression).get
  }

  private def compileAggregate(chAggregate: CHAggregate): String = {
    if (chAggregate.groupingExpressions.isEmpty) ""
    else " GROUP BY " + chAggregate.groupingExpressions.map(compileExpression).mkString(", ")
  }

  private def compileTopN(chTopN: CHTopN): String = {
    (if (chTopN.sortOrders.isEmpty) "" else " ORDER BY " + chTopN.sortOrders.map(so => {
      so.child match {
        case ns@CreateNamedStruct(_) =>
          // Spark will compile order by expression `(a + b, a)` to
          // `named_struct("col1", a + b, "a", a)`.
          // Need to emit the expression list enclosed by ().
          ns.valExprs.map(compileExpression).mkString("(", ", ", ") ") + so.direction.sql
        case _ => compileExpression(so.child) + " " + so.direction.sql
      }}).mkString(", ")) + chTopN.n.map(" LIMIT " + _).getOrElse("")
  }

  def compileExpression(expression: Expression): String = {
    expression match {
      case Literal(value, dataType) =>
        if (dataType == null) {
          "NULL"
        } else {
          dataType match {
            case StringType => "'" + value.toString + "'"
            case _ => value.toString
          }
        }
      case attr: AttributeReference => attr.name
      case Cast(child, dataType) =>
        // TODO: Handle cast
        s"${compileExpression(child)}"
      case IsNotNull(child) => s"${compileExpression(child)} IS NOT NULL"
      case IsNull(child) => s"${compileExpression(child)} IS NULL"
      case UnaryMinus(child) => s"-${compileExpression(child)}"
      case Not(child) => s"NOT ${compileExpression(child)}"
      case Abs(child) => s"ABS(${compileExpression(child)})"
      case Add(left, right) => s"(${compileExpression(left)} + ${compileExpression(right)})"
      case Subtract(left, right) => s"(${compileExpression(left)} - ${compileExpression(right)})"
      case Multiply(left, right) => s"(${compileExpression(left)} * ${compileExpression(right)})"
      case Divide(left, right) => s"(${compileExpression(left)} / ${compileExpression(right)})"
      case Remainder(left, right) => s"(${compileExpression(left)} % ${compileExpression(right)})"
      case GreaterThan(left, right) => s"(${compileExpression(left)} > ${compileExpression(right)})"
      case GreaterThanOrEqual(left, right) => s"(${compileExpression(left)} >= ${compileExpression(right)})"
      case LessThan(left, right) => s"(${compileExpression(left)} < ${compileExpression(right)})"
      case LessThanOrEqual(left, right) => s"(${compileExpression(left)} <= ${compileExpression(right)})"
      case EqualTo(left, right) => s"(${compileExpression(left)} = ${compileExpression(right)})"
      case And(left, right) => s"(${compileExpression(left)} AND ${compileExpression(right)})"
      case Or(left, right) => s"(${compileExpression(left)} OR ${compileExpression(right)})"
      case AggregateExpression(aggregateFunction, _, _, _) => compileExpression(aggregateFunction)
      case Average(child) => s"AVG(${compileExpression(child)})"
      case Count(children) => s"COUNT(${children.map(compileExpression).mkString(", ")})"
      case Max(child) => s"MAX(${compileExpression(child)})"
      case Min(child) => s"MIN(${compileExpression(child)})"
      case Sum(child) => s"SUM(${compileExpression(child)})"
      // TODO: Support more expression types.
      case _ => throw new UnsupportedOperationException(s"Expression ${expression} is not supported by CHSql.")
    }
  }
}
