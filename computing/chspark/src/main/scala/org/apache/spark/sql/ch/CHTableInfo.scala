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

import org.apache.spark.sql.ch.hack.CHStructType

import org.apache.spark.sql.types.StructType

import scala.collection.mutable

class TableInfo(var schema: StructType, var rowWidth: Int, var rowCount: Long)
    extends Serializable {}

class CHTableInfo(val table: CHTableRef, val useSelraw: Boolean) extends Serializable {
  private var info: TableInfo = new TableInfo(null, -1, -1)
  private val TIDB_ROWID = "_tidb_rowid"

  def getSchema: StructType = {
    info.schema
  }

  def getRowWidth: Long = {
    info.rowWidth
  }

  def getRowCount: Long = {
    info.rowCount
  }

  def getInfo: TableInfo = {
    info
  }

  def fetchSchema(): Unit = {
    val fields = CHUtil.getFields(table)
    info.schema = new CHStructType(
      if (useSelraw) {
        fields
      } else {
        // Exclude implicit TIDB_ROWID column if not using selRaw.
        fields.filterNot(_.name == TIDB_ROWID)
      }
    )
    // TODO: Calculate row width
  }

  def fetchRows(): Unit = {
    info.rowCount = CHUtil.getRowCount(table, useSelraw)
  }

  // TODO: Parallel fetch
  def fetchInfo(): Unit = {
    fetchSchema()
    fetchRows()
  }

  // TODO: Async fetch
  fetchInfo()
}

object CHTableInfos {
  val instances: mutable.Map[CHTableRef, CHTableInfo] = mutable.Map()

  // TODO: Background refresh
  def getInfo(table: CHTableRef, useSelraw: Boolean): CHTableInfo = this.synchronized {
    if (!instances.contains(table)) {
      instances += (table -> new CHTableInfo(table, useSelraw))
    }
    instances(table)
  }

  // TODO: Parallel fetch
  // TODO: Data tiling in different tables should be considered
  def getInfo(clusterTable: Seq[CHTableRef], useSelraw: Boolean): TableInfo = {
    var info: TableInfo = null
    clusterTable.foreach(table => {
      val curr = getInfo(table, useSelraw).getInfo
      if (info == null) {
        info = new TableInfo(curr.schema, curr.rowWidth, curr.rowCount)
      } else {
        if (info.schema != curr.schema) {
          throw new Exception("Cluster table schema not the same: " + table)
        }
        info.rowCount += curr.rowCount
      }
    })
    info
  }
}
