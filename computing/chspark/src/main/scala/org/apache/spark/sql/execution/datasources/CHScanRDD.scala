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

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{CHContext, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.ch._
import com.pingcap.theflash.codegen.CHColumnBatch
import com.pingcap.tispark.TiSessionCache
import org.apache.spark.util.{TaskCompletionListener, TaskFailureListener}

class CHScanRDD(@transient private val chContext: CHContext,
                @transient private val sparkSession: SparkSession,
                @transient private val chRelation: CHRelation,
                @transient private val chLogicalPlan: CHLogicalPlan)
    extends RDD[InternalRow](sparkSession.sparkContext, Nil) {

  private val tiConf = chContext.tiContext.tiSession.getConf

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val part = split.asInstanceOf[CHPartition]

    logInfo(s"Physical plan sent to CH ${part.chPhysicalPlan}")

    val client: SparkCHClientSelect = part.chPhysicalPlan.createCHClient(tiConf)

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
    split.asInstanceOf[CHPartition].chPhysicalPlan.table.node.host :: Nil

  override protected def getPartitions: Array[Partition] =
    chRelation.tableInfo.engine
      .getPhysicalPlans(chRelation, chLogicalPlan, TiSessionCache.getSession(tiConf))
      .zipWithIndex
      .map(p => CHPartition(p._2, p._1))
}
