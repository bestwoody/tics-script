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
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, AttributeSet, Cast, Divide, Expression, IntegerLiteral, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.planning.{PhysicalAggregation, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.ch.mock.{TypesTestPlan, TypesTestRelation}
import org.apache.spark.sql.execution.aggregate.AggUtils
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{SparkSession, Strategy}

class CHStrategy(sparkSession: SparkSession) extends Strategy with Logging {
  // -------------------- Dynamic configurations   --------------------
  val sqlConf: SQLConf = sparkSession.sqlContext.conf
  val enableAggPushdown: Boolean =
    sqlConf.getConfString(CHConfigConst.ENABLE_PUSHDOWN_AGG, "true").toBoolean

  // -------------------- Physical plan generation --------------------
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    try {
      plan.collectFirst {
        case rel@LogicalRelation(_: TypesTestRelation, _: Option[Seq[Attribute]], _) =>
          TypesTestPlan(rel.output, sparkSession) :: Nil
        case rel@LogicalRelation(relation: CHRelation, output: Option[Seq[Attribute]], _) =>
          plan match {
            case ReturnAnswer(rootPlan) =>
              rootPlan match {
                case Limit(IntegerLiteral(limit), Sort(order, true, child)) =>
                  createTopNPlan(limit, order, child.output, child) :: Nil
                case Limit(IntegerLiteral(limit), Project(projectList, Sort(order, true, child))) =>
                  createTopNPlan(limit, order, projectList, child) :: Nil
                case Limit(IntegerLiteral(limit), child) =>
                  CollectLimitExec(limit, createTopNPlan(limit, Nil, child.output, child)) :: Nil
                case other => planLater(other) :: Nil
              }
            case Limit(IntegerLiteral(limit), Sort(order, true, child)) =>
              createTopNPlan(limit, order, child.output, child) :: Nil
            case Limit(IntegerLiteral(limit), Project(projectList, Sort(order, true, child))) =>
              createTopNPlan(limit, order, projectList, child) :: Nil
            case PhysicalOperation(projectList, filterPredicates, LogicalRelation(chr: CHRelation, _, _)) =>
              createCHPlan(rel, chr, projectList, filterPredicates, Seq.empty, Option.empty) :: Nil
            case CHAggregation(groupingExpressions, aggregateExpressions, resultExpressions,
            CHAggregationProjection(filters, _, _, _)) if enableAggPushdown =>
              var aggExp = aggregateExpressions
              var resultExp = resultExpressions
              val rewriteResult = CHAggregation.rewriteAggregation(aggExp, resultExp)
              aggExp = rewriteResult._1
              resultExp = rewriteResult._2
              // Add group / aggregate to CHPlan
              createAggregatePlan(groupingExpressions, aggExp, resultExp, relation, filters)

            case _ => Nil
          }
      }.toSeq.flatten
    } catch {
      case e: UnsupportedOperationException =>
        logWarning("CHStrategy downgrading to Spark plan as strategy failed.")
        Nil
    }
  }

  private def pruneTopNFilterProject(
    relation: LogicalRelation,
    source: CHRelation,
    projectList: Seq[NamedExpression],
    filterPredicates: Seq[Expression],
    sortOrder: Seq[SortOrder],
    limit: Int): SparkPlan = {
    createCHPlan(relation, source, projectList, filterPredicates, sortOrder, Option(limit))
  }

  private def createTopNPlan(
    limit: Int,
    sortOrder: Seq[SortOrder],
    project: Seq[NamedExpression],
    child: LogicalPlan): SparkPlan = {
    child match {
      case PhysicalOperation(projectList, filters, rel@LogicalRelation(source: CHRelation, _, _))
        if filters.forall(CHUtil.isSupportedExpression) =>
        TakeOrderedAndProjectExec(
          limit, sortOrder, project,
          pruneTopNFilterProject(rel, source, projectList, filters, sortOrder, limit)
        )
      case _ => TakeOrderedAndProjectExec(limit, sortOrder, project, planLater(child))
    }
  }

  private def createAggregatePlan(
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    resultExpressions: Seq[NamedExpression],
    relation: CHRelation,
    filterPredicates: Seq[Expression]): Seq[SparkPlan] = {
    val deterministicAggAliases = aggregateExpressions.collect {
      case e if e.deterministic => e.canonicalized -> Alias(e, e.toString())()
    }.toMap

    def aliasPushedPartialResult(e: AggregateExpression): Alias = {
      deterministicAggAliases.getOrElse(e.canonicalized, Alias(e, e.toString)())
    }

    val residualAggregateExpressions = aggregateExpressions.map { aggExpr =>
      // As `aggExpr` is being pushing down to CH, we need to replace the original Catalyst
      // aggregate expressions with new ones that merges the partial aggregation results returned by CH.
      //
      // NOTE: Unlike simple aggregate functions (e.g., `Max`, `Min`, etc.), `Count` must be
      // replaced with a `Sum` to sum up the partial counts returned by CH.
      //
      // NOTE: All `Average`s should have already been rewritten into `Sum`s and `Count`s by the
      // `CHAggregation` pattern extractor.

      // An attribute referring to the partial aggregation results returned by CH.
      val partialResultRef = aliasPushedPartialResult(aggExpr).toAttribute

      aggExpr.aggregateFunction match {
        case e: Max   => aggExpr.copy(aggregateFunction = e.copy(child = partialResultRef))
        case e: Min   => aggExpr.copy(aggregateFunction = e.copy(child = partialResultRef))
        case e: Sum   => aggExpr.copy(aggregateFunction = e.copy(child = partialResultRef))
        case e: First => aggExpr.copy(aggregateFunction = e.copy(child = partialResultRef))
        case e: Count => aggExpr.copy(aggregateFunction = Sum(partialResultRef))
        case _ => aggExpr
      }
    }

    val output = (aggregateExpressions.map(aliasPushedPartialResult) ++ groupingExpressions).map {
      _.toAttribute
    }

    val aggregatesToPushdown = aggregateExpressions.filter(ae =>
      CHUtil.isSupportedAggregate(ae.aggregateFunction))

    // As for project list, we emit all aggregate functions followed by group by columns.
    val projectList = aggregatesToPushdown.map(_.asInstanceOf[Expression]) ++ groupingExpressions

    val chLogicalPlan = CHLogicalPlan(projectList, filterPredicates,
      groupingExpressions, aggregatesToPushdown, Seq.empty, Option.empty)

    val chPlan = new CHScanExec(output, sparkSession, relation, chLogicalPlan)

    AggUtils.planAggregateWithoutDistinct(
      groupingExpressions,
      residualAggregateExpressions,
      resultExpressions,
      chPlan
    )
  }

  private def createCHPlan(
    relation: LogicalRelation,
    chRelation: CHRelation,
    projectList: Seq[NamedExpression],
    filterPredicates: Seq[Expression],
    sortOrders: Seq[SortOrder],
    limit: Option[Int]): SparkPlan = {

    val projectSet = AttributeSet(projectList.flatMap(_.references))
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))
    val nameSet = (projectSet ++ filterSet).map(_.name.toLowerCase()).toSet
    var output = relation.output.filter(attr => nameSet(attr.name.toLowerCase()))
    // TODO: Choose the smallest column (in prime keys, or the timestamp key of MergeTree) as dummy output
    if (output.isEmpty) {
      output = Seq(relation.output.head)
    }

    val (pushdownFilters: Seq[Expression], residualFilters: Seq[Expression]) =
      filterPredicates.partition(CHUtil.isSupportedExpression)
    val residualFilter: Option[Expression] = residualFilters.reduceLeftOption(And)

    val chLogicalPlan = CHLogicalPlan(output, pushdownFilters,
      Seq.empty, Seq.empty, sortOrders, limit)

    val chExec = CHScanExec(output, sparkSession, chRelation, chLogicalPlan)

    if (AttributeSet(projectList.map(_.toAttribute)) == projectSet &&
      filterSet.subsetOf(projectSet)) {
      residualFilter.map(FilterExec(_, chExec)).getOrElse(chExec)
    } else {
      ProjectExec(projectList, residualFilter.map(FilterExec(_, chExec)).getOrElse(chExec))
    }
  }
}

object CHAggregation {
  type ReturnType = PhysicalAggregation.ReturnType

  def rewriteAggregation(
    aggregateExpressions: Seq[AggregateExpression],
    resultExpressions: Seq[NamedExpression]): (Seq[AggregateExpression], Seq[NamedExpression]) = {

    // Rewrites all `Average`s into the form of `Divide(Sum / Count)` so that we can push the
    // converted `Sum`s and `Count`s down to CH.
    val (averages, averagesEliminated) = aggregateExpressions.partition {
      case AggregateExpression(_: Average, _, _, _) => true
      case _ => false
    }

    // An auxiliary map that maps result attribute IDs of all detected `Average`s to corresponding
    // converted `Sum`s and `Count`s.
    val rewriteMap = averages.map {
      case a @ AggregateExpression(Average(ref), _, _, _) =>
        a.resultAttribute -> Seq(
          a.copy(aggregateFunction = Sum(ref), resultId = NamedExpression.newExprId),
          a.copy(aggregateFunction = Count(ref), resultId = NamedExpression.newExprId)
        )
    }.toMap

    val rewrite: PartialFunction[Expression, Expression] = rewriteMap.map {
      case (ref, Seq(sum, count)) =>
        val castedSum = Cast(sum.resultAttribute, DoubleType)
        val castedCount = Cast(count.resultAttribute, DoubleType)
        val division = Cast(Divide(castedSum, castedCount), ref.dataType)
        (ref: Expression) -> Alias(division, ref.name)(exprId = ref.exprId)
    }

    val rewrittenResultExpressions = resultExpressions
      .map { _ transform rewrite }
      .map { case e: NamedExpression => e }

    val rewrittenAggregateExpressions = {
      val extraSumsAndCounts = rewriteMap.values.reduceOption { _ ++ _ } getOrElse Nil
      (averagesEliminated ++ extraSumsAndCounts).distinct
    }

    (rewrittenAggregateExpressions, rewrittenResultExpressions)
  }

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    case PhysicalAggregation(groupingExpressions, aggregateExpressions, resultExpressions, child) =>
      Some(groupingExpressions, aggregateExpressions, resultExpressions, child)
    case _ => Option.empty[ReturnType]
  }
}

object CHAggregationProjection {
  type ReturnType = (Seq[Expression], LogicalRelation, CHRelation, Seq[NamedExpression])

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    // Only push down aggregates projection when all filters can be applied and
    // all projection expressions are column references
    case PhysicalOperation(projects, filters, rel@LogicalRelation(source: CHRelation, _, _))
      if projects.forall(_.isInstanceOf[Attribute]) =>
      Some((filters, rel, source, projects))
    case _ => Option.empty[ReturnType]
  }
}
