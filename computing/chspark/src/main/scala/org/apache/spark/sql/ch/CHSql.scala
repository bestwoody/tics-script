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

import com.pingcap.ch.datatypes.{CHType, CHTypeDate, CHTypeDateTime, CHTypeString}
import com.pingcap.ch.datatypes.CHTypeNumber._
import com.pingcap.theflash.TypeMappingJava
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Abs, Add, And, AttributeReference, Cast, CreateNamedStruct, Divide, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, Multiply, Not, Or, Remainder, Subtract, UnaryMinus}
import org.apache.spark.sql.types._

/**
 * Compiler that compiles CHLogicalPlan/CHTableRef to CH SQL string.
 * Note that every identifier, i.e. db name, table name, and column name appearing in the query
 * will be lower-cased and back-quoted.
 */
object CHSql {

  case class Query(private val projection: String,
                   private val table: CHTableRef,
                   private val filter: String,
                   private val aggregation: String,
                   private val topN: String) {
    def buildQuery(partition: String): String = {
      buildQueryInternal(CHSql.compileTable(table, partition))
    }

    def buildQuery(): String = {
      buildQueryInternal(CHSql.compileTable(table))
    }

    private def buildQueryInternal(from: String): String = {
      s"$projection$from$filter$aggregation$topN"
    }

    override def toString: String = buildQuery()
  }

  /**
    * Compose a query string based on given input table and CH logical plan.
    * @param table
    * @param chLogicalPlan
    * @param useSelraw
    * @return
    */
  def query(table: CHTableRef, chLogicalPlan: CHLogicalPlan, useSelraw: Boolean = false): Query = {
    Query(compileProject(chLogicalPlan.chProject, useSelraw),
      table,
      compileFilter(chLogicalPlan.chFilter),
      compileAggregate(chLogicalPlan.chAggregate),
      compileTopN(chLogicalPlan.chTopN))
  }

  /**
    * Query partition list of a table
    * @param table
    * @return
    */
  def partitionList(table: CHTableRef): String = {
    s"SELECT DISTINCT(partition) FROM system.parts WHERE database = '${table.database}' AND table = '${table.table}' AND active = 1"
  }

  /**
    * Compose a desc table string.
    * @param table
    * @return
    */
  def desc(table: CHTableRef): String = {
    "DESC " + getBackQuotedAbsTabName(table)
  }

  /**
    * Compose a count(*) SQL string.
    * @param table
    * @param useSelraw
    * @return
    */
  def count(table: CHTableRef, useSelraw: Boolean = false): String = {
    (if (useSelraw) "SELRAW" else "SELECT") + " COUNT(*) FROM " + getBackQuotedAbsTabName(table)
  }

  private def compileProject(chProject: CHProject, useSelraw: Boolean): String = {
    (if (useSelraw) "SELRAW " else "SELECT ") + chProject.projectList.map(e => compileExpression(e))
      .mkString(", ")
  }

  private def compileTable(table: CHTableRef, partitions: String = null): String = {
    if (partitions == null || partitions.isEmpty) {
      s" FROM ${getBackQuotedAbsTabName(table)}"
    } else {
      s" FROM ${getBackQuotedAbsTabName(table)} PARTITION $partitions"
    }
  }

  private def compileFilter(chFilter: CHFilter): String = {
    if (chFilter.predicates.isEmpty) ""
    else " WHERE " + chFilter.predicates.reduceLeftOption(And).map(e => compileExpression(e)).get
  }

  private def compileAggregate(chAggregate: CHAggregate): String = {
    if (chAggregate.groupingExpressions.isEmpty) ""
    else " GROUP BY " + chAggregate.groupingExpressions.map(e => compileExpression(e)).mkString(", ")
  }

  private def compileTopN(chTopN: CHTopN): String = {
    (if (chTopN.sortOrders.isEmpty) "" else " ORDER BY " + chTopN.sortOrders.map(so => {
      so.child match {
        case ns@CreateNamedStruct(_) =>
          // Spark will compile order by expression `(a + b, a)` to
          // `named_struct("col1", a + b, "a", a)`.
          // Need to emit the expression list enclosed by ().
          ns.valExprs.map(e => compileExpression(e)).mkString("(", ", ", ") ") + so.direction.sql
        case _ => compileExpression(so.child) + " " + so.direction.sql
      }}).mkString(", ")) + chTopN.n.map(" LIMIT " + _).getOrElse("")
  }

  def compileExpression(expression: Expression, isDistinct: Boolean = false): String = {
    expression match {
      case Literal(value, dataType) =>
        if (dataType == null || value == null) {
          "NULL"
        } else {
          dataType match {
            case StringType => "'" + escapeString(value.toString) + "'"
            case BooleanType =>
              val isTrue = if (value.toString.equalsIgnoreCase("TRUE")) 1 else 0
              s"CAST($isTrue AS UInt8)"
            case _ => value.toString
          }
        }
      case attr: AttributeReference => s"`${attr.name.toLowerCase()}`"
      case Cast(child, dataType) =>
        try {
          val dataTypeName = TypeMappingJava.sparkTypeToCHType(dataType, child.nullable).name()
          s"CAST(${compileExpression(child)} AS $dataTypeName)"
        } catch {
          // Unsupported target type, downgrading to not casting.
          case _: UnsupportedOperationException => s"${compileExpression(child)}"
        }
      case IsNotNull(child) => s"${compileExpression(child)} IS NOT NULL"
      case IsNull(child) => s"${compileExpression(child)} IS NULL"
      case UnaryMinus(child) => s"(-${compileExpression(child)})"
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
      case In(value, list) => s"${compileExpression(value)} IN (${list.map(e => compileExpression(e)).mkString(", ")})"
      case AggregateExpression(aggregateFunction, _, _isDistinct, _) => compileExpression(aggregateFunction, _isDistinct)
      case Average(child) => s"AVG(${compileExpression(child)})"
      case Count(children) => s"COUNT(${if(isDistinct) "distinct " else ""}${children.map(e => compileExpression(e)).mkString(", ")})"
      case Max(child) => s"MAX(${compileExpression(child)})"
      case Min(child) => s"MIN(${compileExpression(child)})"
      case Sum(child) => s"SUM(${compileExpression(child)})"
      // TODO: Support more expression types.
      case _ => throw new UnsupportedOperationException(s"Expression $expression is not supported by CHSql.")
    }
  }

  /**
    * Escape a string to CH string literal.
    * See: http://clickhouse-docs.readthedocs.io/en/latest/query_language/syntax.html
    * @param value
    * @return
    */
  private def escapeString(value: String) =
    value.flatMap {
      case '\\' => "\\\\"
      case '\'' => "\\'"
      case '\b' => "\\b"
      case '\f' => "\\f"
      case '\r' => "\\r"
      case '\n' => "\\n"
      case '\t' => "\\t"
      case '\0' => "\\0"
      case c => s"$c"
    }

  /**
    * Get the back-quoted absolute name of a table.
    * @param table
    * @return
    */
  private def getBackQuotedAbsTabName(table: CHTableRef): String =
    if (table.database.isEmpty) s"`${table.table}`" else s"`${table.database}`.`${table.table}`"
}
