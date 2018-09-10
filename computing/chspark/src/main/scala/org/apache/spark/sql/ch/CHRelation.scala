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

import com.pingcap.common.Cluster
import com.pingcap.theflash.SparkCHClientInsert
import org.apache.spark.sql.ch.CHUtil.{Partitioner, PrimaryKey}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation}
import org.apache.spark.sql.types.StructType

class CHRelation(
  val tables: Array[CHTableRef],
  val partitionsPerSplit: Int
)(@transient val sqlContext: SQLContext)
    extends BaseRelation
    with InsertableRelation {

  private lazy val tableInfo: TableInfo = {
    val useSelraw =
      sqlContext.conf.getConfString(CHConfigConst.ENABLE_SELRAW, "false").toBoolean
    CHTableInfos.getInfo(tables, useSelraw)
  }

  def useSelraw: Boolean =
    if (tableInfo.engine == CHEngine.MutableMergeTree) {
      sqlContext.conf.getConfString(CHConfigConst.ENABLE_SELRAW, "false").toBoolean
    } else {
      false
    }

  if (tables.size != tables.toSet.size)
    throw new Exception("Duplicated tables: " + tables.toString)

  override lazy val schema: StructType = {
    tableInfo.schema
  }

  lazy val primaryKeys: Seq[PrimaryKey] = {
    CHTableInfos.getInfo(tables, useSelraw).primaryKeys
  }

  override def sizeInBytes: Long = {
    // TODO consider rowWidth
    val size = tableInfo.rowCount * 64 // Assuming each row is 64 bytes in width
    size
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val table = tables.head
    // TODO: sampling.
    val clientBatchSize = sqlContext.conf
      .getConfString(
        CHConfigConst.CLIENT_BATCH_SIZE,
        SparkCHClientInsert.CLIENT_BATCH_INSERT_COUNT.toString
      )
      .toInt
    val storageBatchRows = sqlContext.conf
      .getConfString(
        CHConfigConst.STORAGE_BATCH_ROWS,
        SparkCHClientInsert.STORAGE_BATCH_INSERT_COUNT_ROWS.toString
      )
      .toLong
    val storageBatchBytes = sqlContext.conf
      .getConfString(
        CHConfigConst.STORAGE_BATCH_BYTES,
        SparkCHClientInsert.STORAGE_BATCH_INSERT_COUNT_BYTES.toString
      )
      .toLong
    Partitioner.fromPrimaryKeys(primaryKeys) match {
      case Partitioner(Partitioner.Hash, keyIndex) =>
        CHUtil.insertDataHash(
          data,
          table.database,
          table.table,
          keyIndex,
          false,
          clientBatchSize,
          storageBatchRows,
          storageBatchBytes,
          Cluster.ofCHTableRefs(tables)
        )
      case Partitioner(Partitioner.Random, _) =>
        CHUtil.insertDataRandom(
          data,
          table.database,
          table.table,
          false,
          clientBatchSize,
          storageBatchRows,
          storageBatchBytes,
          Cluster.ofCHTableRefs(tables)
        )
    }
  }
}
