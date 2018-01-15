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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{LeafExecNode, RDDConversions}


case class CHPlan(output: Seq[Attribute],
  @transient private val sparkSession: SparkSession,
  @transient private val tables: Seq[CHTableRef],
  @transient private val requiredColumns: Seq[String],
  @transient private val filterString: String,
  @transient private val aggregation: CHSqlAgg,
  @transient private val topN: CHSqlTopN,
  @transient private val partitions: Int,
  @transient private val decoders: Int,
  @transient private val encoders: Int) extends LeafExecNode {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows")
  )

  override val nodeName: String = "CHScanRDDExec"

  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val types = schema.fields.map(_.dataType)
    val rdd = new CHRDD(sparkSession, tables, requiredColumns, filterString,
      aggregation, topN, partitions, decoders, encoders)

    val result = RDDConversions.rowToRowRdd(rdd, types)

    // TODO: Can use async?
    result.mapPartitionsWithIndexInternal { (partition, iter) =>
      val proj = UnsafeProjection.create(schema)
      proj.initialize(partition)
      iter.map {
        numOutputRows += 1
        r => proj(r)
      }
    }
  }

  override def verboseString: String = {
    s"$nodeName(tables={${tables.mkString(",")}}, requiredCols=${requiredColumns.mkString(",")}, filter=$filterString, " +
      s"agg={$aggregation}, topN=$topN, partitions=$partitions, decoders=$decoders, encoders=$encoders)"
  }

  override def simpleString: String = verboseString
}
