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
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.expressions.Attribute

import org.apache.spark.sql.ch.mock.MockSimpleRelation
import org.apache.spark.sql.ch.mock.MockSimplePlan
import org.apache.spark.sql.ch.mock.MockArrowRelation
import org.apache.spark.sql.ch.mock.MockArrowPlan
import org.apache.spark.sql.ch.mock.TypesTestRelation
import org.apache.spark.sql.ch.mock.TypesTestPlan


class CHStrategy(sparkSession: SparkSession) extends Strategy with Logging {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case rel@LogicalRelation(relation: MockSimpleRelation, output: Option[Seq[Attribute]], _) =>
        MockSimplePlan(rel.output, sparkSession) :: Nil
      case rel@LogicalRelation(relation: MockArrowRelation, output: Option[Seq[Attribute]], _) =>
        MockArrowPlan(rel.output, sparkSession) :: Nil
      case rel@LogicalRelation(relation: TypesTestRelation, output: Option[Seq[Attribute]], _) =>
        TypesTestPlan(rel.output, sparkSession) :: Nil
      case rel@LogicalRelation(relation: CHRelation, output: Option[Seq[Attribute]], _) =>
        CHPlan(rel.output, sparkSession, relation.table) :: Nil
      case _ => Nil
    }
  }
}
