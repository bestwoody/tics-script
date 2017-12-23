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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.execution.RDDConversions
import org.apache.spark.sql.execution.SparkPlan

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection


case class CHPlan(output: Seq[Attribute],
  @transient private val sparkSession: SparkSession,
  @transient private val tables: Seq[CHTableRef],
  @transient private val requiredColumns: Seq[String],
  @transient private val filterString: String,
  @transient private val partitions: Int,
  @transient private val decoders: Int) extends SparkPlan {

  override protected def doExecute(): RDD[InternalRow] = {
    val types = schema.fields.map(_.dataType)
    // TODO: Read decoderCount/patitionCount from config
    val rdd = new CHRDD(sparkSession, tables, requiredColumns, filterString, partitions, decoders)
    val result = RDDConversions.rowToRowRdd(rdd, types)
    result.mapPartitionsWithIndexInternal { (partition, iter) =>
      val proj = UnsafeProjection.create(schema)
      proj.initialize(partition)
      iter.map { r => proj(r) }
    }
  }

  override def children: Seq[SparkPlan] = Nil
}
