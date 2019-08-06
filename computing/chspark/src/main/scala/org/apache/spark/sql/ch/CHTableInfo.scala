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

import com.pingcap.tispark.statistics.StatisticsManager
import org.apache.spark.sql.CHContext
import org.apache.spark.sql.types.StructType

class TableInfo(@transient val chContext: CHContext,
                val schema: StructType,
                val rowCount: Long,
                val engine: CHEngine)
    extends Serializable {}

class CHTableInfo(@transient val chContext: CHContext,
                  val table: CHTableRef,
                  private var useSelraw: Boolean)
    extends Serializable {
  val engine: CHEngine = {
    val stmt = CHUtil.getShowCreateTable(table)
    val _engine = CHEngine.fromCreateStatement(stmt)
    if (_engine.name != CHEngine.MutableMergeTree && _engine.name != CHEngine.TxnMergeTree) {
      useSelraw = false
    }
    _engine
  }

  val schema: StructType = {
    val fields = engine.mapFields(CHUtil.getFields(table))
    new StructType(
      if (useSelraw) {
        fields
      } else {
        // Exclude implicit TIDB_ROWID column if not using selRaw.
        fields.filterNot(_.name == CHEngine.TIDB_ROWID)
      }
    )
  }
}

// TODO: This is the metadata of a CH table, and should be cached by utilizing Spark's catalog
// like how hive metadata is cached.
object CHTableInfos {
  def getInfo(chContext: CHContext, table: CHTableRef, useSelraw: Boolean): CHTableInfo =
    new CHTableInfo(chContext, table, useSelraw)

  // TODO: Parallel fetch
  // TODO: Data tiling in different tables should be considered
  def getInfo(chContext: CHContext,
              clusterTable: Seq[CHTableRef],
              useSelraw: Boolean): TableInfo = {
    val tiContext = chContext.tiContext
    val nullFreeClusterTable = clusterTable.filter(_ != null)

    var chTableInfo = new CHTableInfo(chContext, nullFreeClusterTable.head, useSelraw)
    var maxSchemaVer = chTableInfo.engine.getSchemaVersion()
    nullFreeClusterTable
      .map(new CHTableInfo(chContext, _, useSelraw))
      .foreach(tableInfo => {
        tableInfo.engine.getSchemaVersion() match {
          case Some(schemaVer) =>
            // If has schema version, get the schema with the latest schema version to be golden, the rest will auto-sync to latest when processing this query.
            if (schemaVer > maxSchemaVer.get) {
              maxSchemaVer = tableInfo.engine.getSchemaVersion()
              chTableInfo = tableInfo
            }
          case None =>
            // If has no schema version, check schema equality.
            if (chTableInfo.schema != tableInfo.schema)
              throw new Exception("Table info inconsistent among TiFlash nodes")
        }
      })

    val tiTableOpt = tiContext.meta.getTable(chTableInfo.table._database, chTableInfo.table._table)
    var sizeInBytes = Long.MaxValue
    if (tiTableOpt.nonEmpty) {
      if (tiContext.autoLoad) {
        StatisticsManager.loadStatisticsInfo(tiTableOpt.get)
      }
      sizeInBytes = StatisticsManager.estimateTableSize(tiTableOpt.get)
    }

    new TableInfo(chContext, chTableInfo.schema, sizeInBytes, chTableInfo.engine)
  }
}
