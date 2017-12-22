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
  private val partitionCount: Int) extends RDD[Row](sparkSession.sparkContext, Nil) {

  @throws[Exception]
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = new Iterator[Row] {

    val table = split.asInstanceOf[CHPartition].table
    val sql = CHSql.scan(table.absName, requiredColumns, filterString)
    val resp = CHExecutorPool.get(sql, table.host, table.port, table.absName, 4)

    private def getBlock(): Iterator[Row] = {
      val block = resp.executor.next
      if (block != null) {
        block.encoded
      } else {
        null
      }
    }

    var blockIter: Iterator[Row] = getBlock

    override def hasNext: Boolean = {
      if (blockIter == null) {
        false
      } else {
        if (!blockIter.hasNext) {
          blockIter.asInstanceOf[CHRows].close
          blockIter = getBlock
          if (blockIter == null) {
            CHExecutorPool.close(resp)
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

  // TODO: All paritions may not assign to a same Spark node, so we need a better session module, like:
  // <Spark nodes>-<share handle of a query> --- <CH sessions, each session respond to a handle>
  override protected def getPreferredLocations(split: Partition): Seq[String] =
    split.asInstanceOf[CHPartition].table.host :: Nil

  override protected def getPartitions: Array[Partition] = {
    // TODO: Read cluster info from CH masterH
    val result = new ListBuffer[CHPartition]
    var index = 0
    tables.foreach(table => {
      for (i <- 0 until partitionCount) {
        result.append(new CHPartition(index, table))
        index += 1
      }
    })
    result.toArray
  }
}
