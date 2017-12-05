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

import java.io.IOException

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import org.apache.arrow.vector.VectorSchemaRoot;


class CHRDD(@transient private val sparkSession: SparkSession)
  extends RDD[Row](sparkSession.sparkContext, Nil) {

  @throws[IOException]
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    // TODO: Read data from CH
    val block: VectorSchemaRoot = null
    ArrowConverter.toRows(block)
  }

  override protected def getPartitions: Array[Partition] = {
    // TODO: Read cluster info from CH masterH
    Array(new CHPartition(0))
  }
}
