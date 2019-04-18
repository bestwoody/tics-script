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
import org.apache.spark.sql.{CHContext, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.ch.{CHLogicalPlan, CHRelation}
import org.apache.spark.sql.execution.datasources.CHScanRDD

case class CHScanExec(output: Seq[Attribute],
                      @transient chContext: CHContext,
                      @transient sparkSession: SparkSession,
                      @transient chRelation: CHRelation,
                      @transient chLogicalPlan: CHLogicalPlan)
    extends LeafExecNode
    with CHBatchScan {

  override def inputRDDs(): Seq[RDD[InternalRow]] =
    new CHScanRDD(
      chContext,
      sparkSession,
      chRelation,
      chLogicalPlan
    ) :: Nil

  override protected def doExecute(): RDD[InternalRow] =
    WholeStageCodegenExec(this)(codegenStageId = 0).execute()
}
