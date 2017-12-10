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


object CHSql {
  def desc(table: String): String = {
    "DESC " + table
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
    val filterStr = filter match {
      case null => ""
      case _ => " WHERE " + filter
    }
    val columnsStr = columns match {
      case null => "*"
      case _ => columns.mkString(", ")
    }
    "SELECT " + columnsStr + " FROM " + table + filterStr
  }
}
