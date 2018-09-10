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

import com.pingcap.common.{Cluster, Node}

class CHTableRef(val node: Node, _database: String, _table: String) extends Serializable {
  // CH database name and table name are all in lower-case so normalize them immediately.
  val database: String = Option(_database).getOrElse("").toLowerCase()
  val table: String = _table.toLowerCase()

  val mappedName: String = table

  override def toString: String =
    s"{host=${node.host}, port=${node.port}, db=$database, table=$table}"
}

object CHTableRef {
  def ofNode(node: Node, db: String, table: String): CHTableRef =
    new CHTableRef(node, db, table)

  def ofCluster(cluster: Cluster, db: String, table: String): Array[CHTableRef] =
    cluster.nodes.map(ofNode(_, db, table))
}
