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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.ch._
import com.pingcap.theflash.codegene.CHColumnBatch
import org.apache.spark.sql.ch.CHSql.Query
import org.apache.spark.util.{TaskCompletionListener, TaskFailureListener}

class CHScanRDD(
  @transient private val sparkSession: SparkSession,
  @transient val output: Seq[Attribute],
  val tableQueryPairs: Seq[(CHTableRef, Query)],
  private val partitionPerSplit: Int,
  private val singleNode: Boolean) extends RDD[InternalRow](sparkSession.sparkContext, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    if (context.attemptNumber > 0){
      throw new IllegalStateException("We don't support partition retry right now! partition: " + split.index + ", attemptNumber: " + context.attemptNumber())
    }

    val part = split.asInstanceOf[CHPartition]
    val table = part.table
    val query = part.query

    logInfo(s"Query sent to CH: $query")

    val client = new SparkCHClientSelect(query, table.host, table.port)

    context.addTaskFailureListener(new TaskFailureListener {
      override def onTaskFailure(context: TaskContext, error: Throwable) = client.close()
    })
    context.addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext) = client.close()
    })

    new Iterator[CHColumnBatch] {
      override def hasNext: Boolean = client.hasNext
      override def next(): CHColumnBatch = client.next()
    }.asInstanceOf[Iterator[InternalRow]]
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] =
    split.asInstanceOf[CHPartition].table.host :: Nil

  override protected def getPartitions: Array[Partition] = {
    val result = new ListBuffer[CHPartition]
    var index = 0

    if (singleNode) {
      if (tableQueryPairs.length != 1) {
        throw new Exception("more than one node encountered in single node mode");
      }
      val table = tableQueryPairs.head._1
      val query = tableQueryPairs.head._2
      result.append(new CHPartition(index, table, query.buildQuery()))
    } else {
      val curParts = ListBuffer.empty[String]
      var table: CHTableRef = null
      for (p <- tableQueryPairs) {
        table = p._1
        val partitionList = CHUtil.getPartitionList(table).map(part => s"'$part'")
        for (part <- partitionList) {
          curParts += part
          if (curParts.length >= partitionPerSplit) {
            result.append(new CHPartition(index, p._1, p._2.buildQuery(s"(${curParts.mkString(",")})")))
            curParts.clear()
            index += 1
          }
        }
        if (curParts.length != 0) {
          result.append(new CHPartition(index, p._1, p._2.buildQuery(s"(${curParts.mkString(",")})")))
          curParts.clear()
          index += 1
        }
      }
    }

    result.toArray
  }
}
