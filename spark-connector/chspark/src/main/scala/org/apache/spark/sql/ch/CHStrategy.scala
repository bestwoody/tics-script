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
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.execution.{RDDConversions, SparkPlan}
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.execution.{RDDConversions, SparkPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType}


class CHStrategy(sparkSession: SparkSession) extends Strategy with Logging {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    //plan.collectFirst {
    //  case LogicalRelation(relation: CHRelation, _, _) => CHPlan(Nil, sparkSession) :: Nil
    //}.toSeq.flatten

    plan match {
      case rel@LogicalRelation(relation: CHRelation, output: Option[Seq[Attribute]], _) => {
        output match {
          case Some(v) => CHPlan(rel.output, sparkSession) :: Nil
          case Some(v) => CHIntPlan(rel.output, sparkSession) :: Nil
          case _ => CHPlan(rel.output, sparkSession) :: Nil
        }
      }
      case _ => Nil
    }
  }
}

case class CHPlan(output: Seq[Attribute], sparkSession: SparkSession) extends SparkPlan {
  override protected def doExecute(): RDD[InternalRow] = {
    val result = RDDConversions.rowToRowRdd(new CHRDD(sparkSession), Seq(DoubleType))
//    val result = RDDConversions.rowToRowRdd(new CHRDD(sparkSession), Seq(FloatType))
//    val result = RDDConversions.rowToRowRdd(new CHRDD(sparkSession), Seq(StringType))
//    val result = RDDConversions.rowToRowRdd(new CHRDD(sparkSession), Seq(IntegerType))
    result.mapPartitionsWithIndexInternal { (partition, iter) =>
      val proj = UnsafeProjection.create(schema)
      proj.initialize(partition)
      iter.map {
        r => {
          proj(r)
        }
      }
    }
  }
  override def children: Seq[SparkPlan] = Nil
}

case class CHIntPlan(output: Seq[Attribute], sparkSession: SparkSession) extends SparkPlan {
  override protected def doExecute(): RDD[InternalRow] = {
    val result = RDDConversions.rowToRowRdd(new CHRDD(sparkSession), Seq(IntegerType))
    result.mapPartitionsWithIndexInternal { (partition, iter) =>
      val proj = UnsafeProjection.create(schema)
      proj.initialize(partition)
      iter.map {
        r => {
          proj(r)
        }
      }
    }
  }
  override def children: Seq[SparkPlan] = Nil
}