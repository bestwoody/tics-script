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
import java.io.IOException

class CHRDD(@transient private val sparkSession: SparkSession)
  extends RDD[Row](sparkSession.sparkContext, Nil) {

  @throws[IOException]
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = new Iterator[Row] {
    // TODO: Read data from CH
    val iterator = Iterator("x")
    override def hasNext: Boolean = iterator.hasNext
    override def next(): Row = Row.fromSeq(Seq(iterator.next))
  }

  override protected def getPartitions: Array[Partition] = {
    // TODO: Read cluster info from CH masterH
    Array(new SimplePartition(0))
  }
}
