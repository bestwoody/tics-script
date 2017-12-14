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
  private val filterString: String) extends RDD[Row](sparkSession.sparkContext, Nil) {

  @throws[Exception]
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = new Iterator[Row] {
    // TODO: Error handling (Exception)

    val table = tables(split.asInstanceOf[CHPartition].index)
    val sql = CHSql.scan(table.absName, requiredColumns, filterString)
    val resp = new CHParallel(sql, table.host, table.port, 4)

    val schema: Schema = resp.getSchema

    private def getBlock(): Iterator[Row] = {
      if (resp.hasNext) {
        val block = resp.next
        if (block != null) {
          ArrowConverter.toRows(schema, table.absName, block)
        } else {
          null
        }
      } else {
        null
      }
    }

    var blockIter: Iterator[Row] = getBlock

    override def hasNext: Boolean = {
      blockIter != null && resp.hasNext
    }

    // TODO: Async convert
    // TODO: Parallel convert
    override def next(): Row = {
      val result = blockIter.next
      if (!blockIter.hasNext) {
        blockIter.asInstanceOf[CHRows].close
        blockIter = getBlock
      }
      result
    }
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] =
    tables(split.asInstanceOf[CHPartition].index).host :: Nil

  override protected def getPartitions: Array[Partition] = {
    // TODO: Read cluster info from CH masterH
    val result = new ListBuffer[CHPartition]
    var index = 0
    tables.foreach(table => {
      result.append(new CHPartition(index))
      index += 1
    })
    result.toArray
  }
}
