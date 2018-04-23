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

package org.apache.spark.sql.ch

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.util

/**
 * IR for a CH query plan, mostly represented in Spark data structures.
 * This IR is hopefully backend independent, thus can be further compiled into
 * various physical representations.
 */
class CHLogicalPlan(
  val chProject: CHProject,
  val chFilter: CHFilter,
  val chAggregate: CHAggregate,
  val chTopN: CHTopN) {
  override def toString: String = s"CH plan [$chProject, $chFilter, $chAggregate, $chTopN]"
}

object CHLogicalPlan {
  def apply(
    projectList: Seq[Expression],
    filterPredicates: Seq[Expression],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    sortOrders: Seq[SortOrder],
    limit: Option[Int]): CHLogicalPlan = {
    if (!projectList.forall(CHUtil.isSupportedExpression)
      || !filterPredicates.forall(CHUtil.isSupportedExpression)
      || !groupingExpressions.forall(CHUtil.isSupportedExpression)
      || !aggregateExpressions.forall(ae => CHUtil.isSupportedAggregate(ae.aggregateFunction))) {
      throw new UnsupportedOperationException
    }
    new CHLogicalPlan(
      new CHProject(projectList), new CHFilter(filterPredicates),
      new CHAggregate(groupingExpressions, aggregateExpressions), new CHTopN(sortOrders, limit))
  }
}

class CHProject(val projectList: Seq[Expression]) {
  override def toString: String = projectList.map(util.toPrettySQL).mkString("Project [", ", ", "]")
}

class CHFilter(val predicates: Seq[Expression]) {
  override def toString: String = predicates.map(util.toPrettySQL).mkString("Filter [", ", ", "]")
}

class CHAggregate(val groupingExpressions: Seq[NamedExpression],
  val aggregateExpressions: Seq[AggregateExpression]) {
  override def toString: String = Seq(
    if (groupingExpressions.isEmpty) "" else groupingExpressions.map(util.toPrettySQL)
      .mkString("[", ", ", "]"),
    if (aggregateExpressions.isEmpty) "" else aggregateExpressions.map(util.toPrettySQL)
      .mkString("[", ", ", "]"))
    .filter(_.nonEmpty)
    .mkString("Aggregate [", ", ", "]")
}

class CHTopN(val sortOrders: Seq[SortOrder], val n: Option[Int]) {
  override def toString: String = Seq(
    if (sortOrders.isEmpty) "" else sortOrders.map(util.toPrettySQL).mkString("[", ", ", "]"),
    n.getOrElse("").toString)
    .filter(_.nonEmpty)
    .mkString("TopN [", ", ", "]")
}
