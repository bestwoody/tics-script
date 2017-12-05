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

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.VectorSchemaRoot;


class CHRDD(@transient private val sparkSession: SparkSession, @transient private val tableInfo: CHRelation)
  extends RDD[Row](sparkSession.sparkContext, Nil) {

  @throws[Exception]
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = new Iterator[Row] {
    // TODO: Predicate push down
    // TODO: Error handling (Exception)

    // TODO: Share ArrowDecoder
    val resp = new CHResponse("select * from " + tableInfo.table, tableInfo.host, tableInfo.port, null)

    val schema: Schema = resp.getSchema()
    var blockIter: Iterator[Row] = null

    override def hasNext: Boolean = {
      if (blockIter != null && !blockIter.hasNext) {
        blockIter = null
      }
      (blockIter != null || resp.hasNext)
    }

    // TODO: Async convert
    override def next(): Row = blockIter match {
      case null => {
        blockIter = ArrowConverter.toRows(schema, resp.next)
        // TODO: Empty check
        blockIter.next
      }
      case _ => blockIter.next
    }
  }

  override protected def getPartitions: Array[Partition] = {
    // TODO: Read cluster info from CH masterH
    Array(new CHPartition(0))
  }
}
