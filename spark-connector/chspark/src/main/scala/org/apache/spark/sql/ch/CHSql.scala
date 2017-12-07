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
  def mappedTableName(database: String, table: String) {
    table
  }

  def tableRef(database: String, table: String): String = {
    if (database == null || database.isEmpty) {
      table
    } else {
      database + "." + table
    }
  }

  def desc(database: String, table: String): String = {
    "DESC " + tableRef(database, table)
  }

  def allScan(database: String, table: String): String = {
    "SELECT * FROM " + tableRef(database, table)
  }
}
