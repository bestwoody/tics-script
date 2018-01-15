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

import scala.actors.threadpool.BlockingQueue
import scala.actors.threadpool.LinkedBlockingQueue
import scala.collection.mutable.ListBuffer

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.VectorSchemaRoot;


class CHRDD(
  @transient private val sparkSession: SparkSession,
  val tables: Seq[CHTableRef],
  private val requiredColumns: Seq[String],
  private val filterString: String,
  private val aggregation: CHSqlAgg,
  private val topN: CHSqlTopN,
  private val partitionCount: Int,
  private val decoderCount: Int,
  private val encoderCount: Int) extends RDD[Row](sparkSession.sparkContext, Nil) {

  @throws[Exception]
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = new Iterator[Row] {

    val part = split.asInstanceOf[CHPartition]
    val table = part.table
    val qid = part.qid
    val sql = CHSql.scan(table.absName, requiredColumns, filterString, aggregation, topN)

    val resp = new CHExecutorParall(qid, sql, table.host, table.port, table.absName,
      decoderCount, encoderCount, partitionCount, part.clientIndex)

    val queue: BlockingQueue[Iterator[Row]] = new LinkedBlockingQueue[Iterator[Row]](3)

    private val EOF = new Iterator[Row] {
      override def hasNext: Boolean = false
      override def next(): Row = null
    }

    private def getBlock(): Iterator[Row] = {
      val block = resp.next
      if (block != null) block.encoded else EOF
    }

    private val asyncCodec = new Thread {
      override def run {
        var it: Iterator[Row] = null
        try {
          while (it != EOF) {
            it = getBlock
            queue.put(it)
          }
        } catch {
          case _: InterruptedException => {}
          case e: Any => throw e
        }
      }
    }
    asyncCodec.start

    var blockIter: Iterator[Row] = queue.take

    override def hasNext: Boolean = {
      if (blockIter == EOF) {
        false
      } else {
        if (!blockIter.hasNext) {
          blockIter.asInstanceOf[CHRows].close
          blockIter = queue.take
          if (blockIter == EOF) {
            resp.close
            false
          } else {
            blockIter.hasNext
          }
        } else {
          true
        }
      }
    }

    override def next(): Row = blockIter.next
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] =
    split.asInstanceOf[CHPartition].table.host :: Nil

  override protected def getPartitions: Array[Partition] = {
    val qid = CHUtil.genQueryId
    val result = new ListBuffer[CHPartition]
    var index = 0
    tables.foreach(table => {
      for (i <- 0 until partitionCount) {
        result.append(new CHPartition(index, table, qid, i))
        index += 1
      }
    })
    result.toArray
  }
}
