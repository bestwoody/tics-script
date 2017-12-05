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

import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType, StructType}


case class CHPlan(output: Seq[Attribute], sparkSession: SparkSession, tableInfo: CHRelation) extends SparkPlan {
  override protected def doExecute(): RDD[InternalRow] = {
    // TODO: get types info from schema or from tableInfo
    val types = Seq(StringType)
    val result = RDDConversions.rowToRowRdd(new CHRDD(sparkSession, tableInfo), types)
    result.mapPartitionsWithIndexInternal { (partition, iter) =>
      val proj = UnsafeProjection.create(schema)
      proj.initialize(partition)
      iter.map { r => proj(r) }
    }
  }
  override def children: Seq[SparkPlan] = Nil
}
