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


class CHTableRef(val host: String, val port: Int, val database: String, val table: String) extends Serializable {
  val mappedName = table

  val absName = {
    if (database == null || database.isEmpty) {
      table
    } else {
      database + "." + table
    }
  }

  override def toString: String = {
    s"{host=$host, port=$port, db=$database, table=$table}"
  }
}
