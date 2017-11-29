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

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.expressions.Attribute

import org.apache.spark.sql.execution.RDDConversions
import org.apache.spark.sql.types.StringType

import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.unsafe.types.UTF8String


class CHStrategy(sparkSession: SparkSession) extends Strategy with Logging {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    //plan.collectFirst {
    //  case LogicalRelation(relation: CHRelation, _, _) => CHPlan(Nil, sparkSession) :: Nil
    //}.toSeq.flatten

    plan match {
      case LogicalRelation(relation: CHRelation, _, _) => CHPlan(Nil, sparkSession) :: Nil
      case _ => Nil
    }
  }
}

case class CHPlan(output: Seq[Attribute], sparkSession: SparkSession) extends SparkPlan {
  override protected def doExecute(): RDD[InternalRow] = {
    RDDConversions.rowToRowRdd(new CHRDD(sparkSession), Seq(StringType))
  }
  override def children: Seq[SparkPlan] = Nil
}
