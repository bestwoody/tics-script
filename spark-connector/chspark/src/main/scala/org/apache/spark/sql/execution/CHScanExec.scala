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

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, UnsafeProjection}

import org.apache.spark.sql.execution.datasources.CHScanRDD

import org.apache.spark.sql.ch.{CHRDD, CHSqlAgg, CHSqlTopN, CHTableRef}

case class CHScanExec(
  output: Seq[Attribute],
  chScanRDD: CHScanRDD,
  @transient private val sparkSession: SparkSession,
  @transient private val tables: Seq[CHTableRef],
  @transient private val requiredColumns: Seq[String],
  @transient private val filterString: String,
  @transient private val aggregation: CHSqlAgg,
  @transient private val topN: CHSqlTopN,
  @transient private val partitions: Int,
  @transient private val decoders: Int,
  @transient private val encoders: Int,
  private val enableCodeGen: Boolean = true)
  extends LeafExecNode with ArrowBatchScan {

  private lazy val types = schema.fields.map(_.dataType)
  // Used for non-CodeGen based data pipeline
  private lazy val result = RDDConversions.rowToRowRdd(rdd, types)
  private lazy val rdd = new CHRDD(sparkSession, tables, requiredColumns, filterString,
    aggregation, topN, partitions, decoders, encoders)

  override def inputRDDs(): Seq[RDD[InternalRow]] = if (enableCodeGen) {
    chScanRDD :: Nil
  } else {
    result :: Nil
  }

  override protected def doProduce(ctx: CodegenContext): String = {
    if (enableCodeGen) {
      super.doProduce(ctx)
    } else {
      val numOutputRows = metricTerm(ctx, "numOutputRows")
      // PhysicalRDD always just has one input
      val input = ctx.freshName("input")
      ctx.addMutableState("scala.collection.Iterator", input, s"$input = inputs[0];")
      val exprRows = output.zipWithIndex.map{ case (a, i) =>
        BoundReference(i, a.dataType, a.nullable)
      }
      val row = ctx.freshName("row")
      ctx.INPUT_ROW = row
      ctx.currentVars = null
      val columnsRowInput = exprRows.map(_.genCode(ctx))
      s"""
         |while ($input.hasNext()) {
         |  InternalRow $row = (InternalRow) $input.next();
         |  $numOutputRows.add(1);
         |  ${consume(ctx, columnsRowInput).trim}
         |  if (shouldStop()) return;
         |}
     """.stripMargin
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    if (enableCodeGen) {
      WholeStageCodegenExec(this).execute()
    } else {
      val numOutputRows = longMetric("numOutputRows")
      result.mapPartitionsWithIndexInternal { (partition, iter) =>
        val proj = UnsafeProjection.create(schema)
        proj.initialize(partition)
        iter.map { r =>
          numOutputRows += 1
          proj(r)
        }
      }
    }
  }
}
