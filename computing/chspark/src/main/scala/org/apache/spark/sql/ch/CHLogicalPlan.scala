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
case class CHLogicalPlan(chProject: CHProject,
                         chFilter: CHFilter,
                         chAggregate: CHAggregate,
                         chTopN: CHTopN) {
  override def toString: String = s"CHLogicalPlan($chProject, $chFilter, $chAggregate, $chTopN)"

  def transformExpressions(rule: PartialFunction[Expression, Expression]): CHLogicalPlan =
    CHLogicalPlan(
      CHProject(chProject.projectList.map(_.transform(rule))),
      CHFilter(chFilter.predicates.map(_.transform(rule))),
      CHAggregate(
        chAggregate.groupingExpressions.map(_.transform(rule).asInstanceOf[NamedExpression]),
        chAggregate.aggregateExpressions.map(_.transform(rule).asInstanceOf[AggregateExpression])
      ),
      CHTopN(chTopN.sortOrders.map(_.transform(rule).asInstanceOf[SortOrder]), chTopN.n)
    )
}

object CHLogicalPlan {
  def apply(projectList: Seq[Expression],
            filterPredicates: Seq[Expression],
            groupingExpressions: Seq[NamedExpression],
            aggregateExpressions: Seq[AggregateExpression],
            sortOrders: Seq[SortOrder],
            limit: Option[Int]): CHLogicalPlan = {
    if (!projectList.forall(CHUtil.isSupportedExpression)
        || !filterPredicates.forall(CHUtil.isSupportedExpression)
        || !groupingExpressions.forall(CHUtil.isSupportedExpression)
        || !aggregateExpressions.forall(CHUtil.isSupportedAggregateExpression)) {
      throw new UnsupportedOperationException
    }
    CHLogicalPlan(
      CHProject(projectList),
      CHFilter(filterPredicates),
      CHAggregate(groupingExpressions, aggregateExpressions),
      CHTopN(sortOrders, limit)
    )
  }
}

case class CHProject(projectList: Seq[Expression]) {
  override def toString: String = projectList.map(util.toPrettySQL).mkString("project=[", ", ", "]")
}

case class CHFilter(predicates: Seq[Expression]) {
  override def toString: String = predicates.map(util.toPrettySQL).mkString("filter=[", ", ", "]")
}

case class CHAggregate(groupingExpressions: Seq[NamedExpression],
                       aggregateExpressions: Seq[AggregateExpression]) {
  override def toString: String =
    Seq(
      if (groupingExpressions.isEmpty) ""
      else
        groupingExpressions
          .map(util.toPrettySQL)
          .mkString("[", ", ", "]"),
      if (aggregateExpressions.isEmpty) ""
      else
        aggregateExpressions
          .map(util.toPrettySQL)
          .mkString("[", ", ", "]")
    ).filter(_.nonEmpty)
      .mkString("agg=[", ", ", "]")
}

case class CHTopN(sortOrders: Seq[SortOrder], n: Option[Int]) {
  override def toString: String =
    Seq(
      if (sortOrders.isEmpty) "" else sortOrders.map(util.toPrettySQL).mkString("[", ", ", "]"),
      n.getOrElse("").toString
    ).filter(_.nonEmpty)
      .mkString("topN=[", ", ", "]")
}
