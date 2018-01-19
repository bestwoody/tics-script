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

import org.apache.spark.sql.catalyst.expressions.Expression

class CHSqlAggFunc(val function: String, val column: String) extends Serializable {
  override def toString: String = {
    function + "(" + column + ")"
  }
}

object CHSqlAggFunc {
  def apply(function: String, exp: Expression*): CHSqlAggFunc =
    new CHSqlAggFunc(function, exp.map(CHUtil.expToCHString).mkString(","))
}

class CHSqlAgg(val groupByColumns: Seq[String], val functions: Seq[CHSqlAggFunc]) extends Serializable {
  override def toString: String = {
    s"aggFunc=${functions.mkString(",")},groupByCols=${groupByColumns.mkString(",")}"
  }
}

class CHSqlOrderByCol(val orderByColName: String, val direction: String, val namedStructure: Boolean = false) extends Serializable {
  override def toString: String = {
    s"orderByCol=$orderByColName,direction=$direction"
  }
}

object CHSqlOrderByCol {
  def apply(orderByColName: String, direction: String, namedStructure: Boolean = false): CHSqlOrderByCol =
    new CHSqlOrderByCol(orderByColName, direction, namedStructure)
}

class CHSqlTopN(val orderByColumns: Seq[CHSqlOrderByCol], val limit: String) extends Serializable {
  override def toString: String = {
    orderByColumns.mkString(";") + s";limit=$limit"
  }
}

object CHSql {
  def desc(table: String): String = {
    "DESC " + table
  }

  def count(table: String): String = {
    "SELECT COUNT(*) FROM " + table
  }

  def scan(table: String): String = {
    scan(table, null, null)
  }

  def scan(table: String, columns: Seq[String]): String = {
    scan(table, columns, null)
  }

  def scan(table: String, filter: String): String = {
    scan(table, null, filter)
  }

  def scan(table: String, columns: Seq[String], filter: String): String = {
    "SELECT " + columnsStr(columns) + " FROM " + table + filterStr(filter)
  }

  def scan(table: String, columns: Seq[String], filter: String, aggregation: CHSqlAgg): String = {
    if (aggregation == null) {
      scan(table, columns, filter)
    } else {
      // TODO: Check Set(columns) == Set(agg columns)
      "SELECT " + columnsStr(columns) + " FROM " + table + filterStr(filter) +
        groupByColumnsStr(aggregation.groupByColumns)
    }
  }

  def scan(table: String, columns: Seq[String], filter: String, aggregation: CHSqlAgg, topN: CHSqlTopN): String = {
    var sql = scan(table, columns, filter, aggregation)
    if (topN != null) {
      val orderByColumns = topN.orderByColumns
      val limit = topN.limit

      if (orderByColumns != null && orderByColumns.nonEmpty) {
        if (orderByColumns.head.namedStructure && orderByColumns.lengthCompare(1) == 0) {
          sql += " ORDER BY (" + orderByColumns.head.orderByColName + ") " + orderByColumns.head.direction
        } else {
          sql += " ORDER BY " + orderByColumns.map(order => order.orderByColName + " " + order.direction).mkString(", ")
        }
      }

      if (limit != null && limit.nonEmpty) {
        sql += " LIMIT " + limit
      }
    }

    sql
  }

  private def filterStr(filter: String): String = {
    filter match {
      case null => ""
      case _ => " WHERE " + filter
    }
  }

  private def columnsStr(columns: Seq[String]): String = {
    columns match {
      case null => "*"
      case _ => columns.mkString(", ")
    }
  }

  private def groupByColumnsStr(columns: Seq[String]): String = {
    if (columns == null || columns.isEmpty) "" else " GROUP BY " + columns.mkString(", ")
  }
}
