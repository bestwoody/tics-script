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

package org.apache.spark.sql.ch.mock

import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.execution.RDDConversions
import org.apache.spark.sql.execution.SparkPlan

import org.apache.spark.sql.sources.BaseRelation

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection

import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType}


class MockSimpleRelation(tableName: String)(@transient val sqlContext: SQLContext) extends BaseRelation {
  override lazy val schema: StructType = {
    val fields = new Array[StructField](1)
    val name="col1"
    val metadata = new MetadataBuilder().putString("name", name).build()
    // val ft = FloatType
    // val ft = StringType
    // val ft = IntegerType
    val ft = DoubleType
    fields(0) = StructField(name, ft, nullable = true, metadata)
    new StructType(fields)
  }
}

case class MockSimplePlan(output: Seq[Attribute], sparkSession: SparkSession) extends SparkPlan {
  override protected def doExecute(): RDD[InternalRow] = {
    // TODO: Get type info from schema
    // val ft = FloatType
    // val ft = StringType
    // val ft = IntegerType
    val ft = DoubleType
    val result = RDDConversions.rowToRowRdd(new MockSimpleRDD(sparkSession), Seq(ft))
    result.mapPartitionsWithIndexInternal { (partition, iter) =>
      val proj = UnsafeProjection.create(schema)
      proj.initialize(partition)
      iter.map { r => proj(r) }
    }
  }
  override def children: Seq[SparkPlan] = Nil
}

class MockSimpleRDD(@transient private val sparkSession: SparkSession)
  extends RDD[Row](sparkSession.sparkContext, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = new Iterator[Row] {
    // val iterator = Iterator(1.1, 2.2, 3.3)
    // val iterator = Iterator("aaa", "bbb", "ccc")
    // val iterator = Iterator(3, 6, 9)
    val iterator = Iterator(11.11, 22.22, 33.33)
    override def hasNext: Boolean = iterator.hasNext
    override def next(): Row = Row.fromSeq(Seq(iterator.next))
  }

  override protected def getPartitions: Array[Partition] = {
    Array(new SimplePartition(0))
  }
}