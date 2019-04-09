/*
 * Copyright 2018 PingCAP, Inc.
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

package org.apache.spark.sql.execution.datasources

import com.pingcap.theflash.SparkCHClientSelect

import scala.collection.mutable.ListBuffer
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{CHContext, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.ch._
import com.pingcap.theflash.codegen.CHColumnBatch
import com.pingcap.tispark.TiSessionCache
import com.pingcap.tikv.meta.TiTimestamp
import org.apache.spark.sql.ch.CHSql.Query
import org.apache.spark.util.{TaskCompletionListener, TaskFailureListener}

class CHScanRDD(@transient private val chContext: CHContext,
                @transient private val sparkSession: SparkSession,
                @transient val output: Seq[Attribute],
                val tableQueryPairs: Seq[(CHTableRef, Query)],
                private val partitionPerSplit: Int,
                val ts: Option[TiTimestamp] = None,
                val isTMT: Boolean)
    extends RDD[InternalRow](sparkSession.sparkContext, Nil) {

  private val tiConf = chContext.tiContext.tiSession.getConf

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    var client: SparkCHClientSelect = null
    if (isTMT) {
      val part = split.asInstanceOf[CHRegionPartition]
      val table = part.regions.table
      val query = part.query

      logInfo(s"Query sent to CH: $query")

      client = new SparkCHClientSelect(
        query,
        table.node.host,
        table.node.port,
        TiSessionCache.getSession(tiConf),
        ts.orNull,
        part.regions.region
      )
    } else {
      val part = split.asInstanceOf[CHPartition]
      val table = part.table
      val query = part.query

      logInfo(s"Query sent to CH: $query")

      client = new SparkCHClientSelect(
        query,
        table.node.host,
        table.node.port,
        TiSessionCache.getSession(tiConf),
        ts.orNull,
        null
      )
    }

    context.addTaskFailureListener(new TaskFailureListener {
      override def onTaskFailure(context: TaskContext, error: Throwable): Unit = client.close()
    })
    context.addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = client.close()
    })

    new Iterator[CHColumnBatch] {
      override def hasNext: Boolean = client.hasNext
      override def next(): CHColumnBatch = client.next()
    }.asInstanceOf[Iterator[InternalRow]]
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] =
    if (isTMT) {
      split.asInstanceOf[CHRegionPartition].regions.table.node.host :: Nil
    } else {
      split.asInstanceOf[CHPartition].table.node.host :: Nil
    }

  override protected def getPartitions: Array[Partition] =
    if (isTMT) {
      val result = new ListBuffer[CHRegionPartition]
      var index = 0
      val curParts = ListBuffer.empty[String]
      var table: CHTableRef = null
      val p = tableQueryPairs(0)
      table = p._1
      val tableInfo = new CHTableInfo(chContext, table, false)
      val regions =
        CHUtil
          .getRegionPartitionList(tableInfo, TiSessionCache.getSession(tiConf), partitionPerSplit)
      for (region <- regions) {
        result.append(new CHRegionPartition(index, region, p._2.buildQuery()))
        index += 1
      }
      result.toArray
    } else {
      val result = new ListBuffer[CHPartition]
      var index = 0
      val curParts = ListBuffer.empty[String]
      var table: CHTableRef = null
      for (p <- tableQueryPairs) {
        table = p._1
        val partitionList = CHUtil.getPartitionList(table).map(part => s"'$part'")
        if (partitionList.isEmpty) {
          result.append(new CHPartition(index, p._1, p._2.buildQuery()))
          index += 1
        } else {
          for (part <- partitionList) {
            curParts += part
            if (curParts.length >= partitionPerSplit) {
              result.append(
                new CHPartition(index, p._1, p._2.buildQuery(s"(${curParts.mkString(",")})"))
              )
              curParts.clear()
              index += 1
            }
          }
          if (curParts.nonEmpty) {
            result.append(
              new CHPartition(index, p._1, p._2.buildQuery(s"(${curParts.mkString(",")})"))
            )
            curParts.clear()
            index += 1
          }
        }
      }
      result.toArray
    }
}
