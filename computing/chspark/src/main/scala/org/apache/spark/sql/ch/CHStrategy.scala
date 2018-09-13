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
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeSet, Cast, Divide, Expression, IntegerLiteral, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.planning.{PhysicalAggregation, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.aggregate.AggUtils
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{CHContext, SparkSession, Strategy}

case class CHStrategy(getOrCreateCHContext: SparkSession => CHContext)(sparkSession: SparkSession)
    extends Strategy
    with Logging {
  // -------------------- Dynamic configurations   --------------------
  private val chContext = getOrCreateCHContext(sparkSession)
  private val sqlConf: SQLConf = sparkSession.sqlContext.conf
  private val enableAggPushdown: Boolean =
    sqlConf.getConfString(CHConfigConst.ENABLE_PUSHDOWN_AGG, "true").toBoolean

  // -------------------- Physical plan generation --------------------
  override def apply(plan: LogicalPlan): Seq[SparkPlan] =
    try {
      plan
        .collectFirst {
          case rel @ LogicalRelation(relation: CHRelation, _, _, _) =>
            plan match {
              case ReturnAnswer(rootPlan) =>
                rootPlan match {
                  case Limit(IntegerLiteral(limit), Sort(order, true, child)) =>
                    createTopNPlan(limit, order, child.output, child) :: Nil
                  case Limit(
                      IntegerLiteral(limit),
                      Project(projectList, Sort(order, true, child))
                      ) =>
                    createTopNPlan(limit, order, projectList, child) :: Nil
                  case Limit(IntegerLiteral(limit), child) =>
                    CollectLimitExec(limit, collectLimit(limit, child)) :: Nil
                  case other => planLater(other) :: Nil
                }
              case Limit(IntegerLiteral(limit), Sort(order, true, child)) =>
                createTopNPlan(limit, order, child.output, child) :: Nil
              case Limit(IntegerLiteral(limit), Project(projectList, Sort(order, true, child))) =>
                createTopNPlan(limit, order, projectList, child) :: Nil
              case PhysicalOperation(
                  projectList,
                  filterPredicates,
                  LogicalRelation(chr: CHRelation, _, _, _)
                  ) =>
                createCHPlan(rel, chr, projectList, filterPredicates, Seq.empty, Option.empty) :: Nil
              case CHAggregation(
                  groupingExpressions,
                  aggregateExpressions,
                  resultExpressions,
                  CHAggregationProjection(filters, _, _, _)
                  ) if enableAggPushdown =>
                val rewriteResult =
                  CHAggregation.rewriteAggregation(aggregateExpressions, resultExpressions)
                val aggExp = rewriteResult._1
                val resultExp = rewriteResult._2
                createAggregatePlan(groupingExpressions, aggExp, resultExp, relation, filters)
              case _ => Nil
            }
        }
        .toSeq
        .flatten
    } catch {
      case _: UnsupportedOperationException =>
        logDebug("CHStrategy downgrading to Spark plan as strategy failed.")
        Nil
    }

  private def pruneTopNFilterProject(relation: LogicalRelation,
                                     source: CHRelation,
                                     projectList: Seq[NamedExpression],
                                     filterPredicates: Seq[Expression],
                                     sortOrder: Seq[SortOrder],
                                     limit: Int): SparkPlan =
    createCHPlan(relation, source, projectList, filterPredicates, sortOrder, Option(limit))

  private def createTopNPlan(limit: Int,
                             sortOrder: Seq[SortOrder],
                             project: Seq[NamedExpression],
                             child: LogicalPlan): SparkPlan =
    child match {
      case PhysicalOperation(
          projectList,
          filters,
          rel @ LogicalRelation(source: CHRelation, _, _, _)
          ) if filters.forall(CHUtil.isSupportedExpression) =>
        TakeOrderedAndProjectExec(
          limit,
          sortOrder,
          project,
          pruneTopNFilterProject(rel, source, projectList, filters, sortOrder, limit)
        )
      case _ => TakeOrderedAndProjectExec(limit, sortOrder, project, planLater(child))
    }

  private def collectLimit(limit: Int, child: LogicalPlan): SparkPlan = child match {
    case PhysicalOperation(projectList, filters, rel @ LogicalRelation(source: CHRelation, _, _, _))
        if filters.forall(CHUtil.isSupportedExpression) =>
      pruneTopNFilterProject(rel, source, projectList, filters, Nil, limit)
    case _ => planLater(child)
  }

  private def createAggregatePlan(groupingExpressions: Seq[NamedExpression],
                                  aggregateExpressions: Seq[AggregateExpression],
                                  resultExpressions: Seq[NamedExpression],
                                  relation: CHRelation,
                                  filterPredicates: Seq[Expression]): Seq[SparkPlan] = {
    val deterministicAggAliases = aggregateExpressions.collect {
      case e if e.deterministic => e.canonicalized -> Alias(e, e.toString())()
    }.toMap

    def aliasPushedPartialResult(e: AggregateExpression): Alias =
      deterministicAggAliases.getOrElse(e.canonicalized, Alias(e, e.toString)())

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
        case _: Count => aggExpr.copy(aggregateFunction = Sum(partialResultRef))
        case _        => aggExpr
      }
    }

    val aggregateAttributes =
      aggregateExpressions.map(expr => aliasPushedPartialResult(expr).toAttribute)
    val groupAttributes = groupingExpressions.map(_.toAttribute)

    // output of plan should contain all references within aggregates and group by expressions
    val output = aggregateAttributes ++ groupAttributes

    val groupExpressionMap = groupingExpressions.map(expr => expr.exprId -> expr.toAttribute).toMap

    // resultExpression might refer to some of the group by expressions
    // Those expressions originally refer to table columns but now it refers to
    // results pushed down.
    // For example, select a + 1 from t group by a + 1
    // expression a + 1 has been pushed down to CH
    // and in turn a + 1 in projection should be replaced by
    // reference of CH output entirely
    val rewrittenResultExpressions = resultExpressions.map {
      _.transform {
        case e: NamedExpression => groupExpressionMap.getOrElse(e.exprId, e)
      }.asInstanceOf[NamedExpression]
    }

    // As for project list, we emit all aggregate functions followed by group by columns.
    val projectList = aggregateExpressions ++ groupingExpressions

    val chLogicalPlan = CHLogicalPlan(
      projectList,
      filterPredicates,
      groupingExpressions,
      aggregateExpressions,
      Seq.empty,
      Option.empty
    )

    val chPlan = CHScanExec(output, sparkSession, relation, chLogicalPlan)

    AggUtils.planAggregateWithoutDistinct(
      groupAttributes,
      residualAggregateExpressions,
      rewrittenResultExpressions,
      chPlan
    )
  }

  /**
   * Create a CH Plan from Spark Plan
   * The plan will include following parameters
   *
   * @param relation a LogicRelation of CH columns
   * @param chRelation a BaseRelation of CH columns
   * @param projectList project list to output
   * @param filterPredicates all filters in this SparkPlan
   * @param sortOrders determines orderBy parameter
   * @param limit row limit
   * @return a rewritten CHPlan that contains CHScanExec
   */
  private def createCHPlan(relation: LogicalRelation,
                           chRelation: CHRelation,
                           projectList: Seq[NamedExpression],
                           filterPredicates: Seq[Expression],
                           sortOrders: Seq[SortOrder],
                           limit: Option[Int]): SparkPlan = {
    // 1. Compose the lower plan (CH).

    val (pushDownFilters: Seq[Expression], residualFilters: Seq[Expression]) =
      filterPredicates.partition(CHUtil.isSupportedExpression)
    val residualFilter: Option[Expression] = residualFilters.reduceLeftOption(And)

    val (pushDownProjects: Seq[Expression], residualProjects: Seq[Expression]) =
      projectList.partition(CHUtil.isSupportedExpression)

    // Total push down project expressions = supported expressions in projectList + columns in residual projects and filters.
    var totalPushDownProjects = (pushDownProjects.asInstanceOf[Seq[NamedExpression]]
      ++ (residualProjects ++ residualFilters).flatMap(_.references)).distinct
    // TODO: Choose the smallest column (in prime keys, or the timestamp key of MergeTree) as dummy output
    if (totalPushDownProjects.isEmpty) {
      totalPushDownProjects = Seq(relation.output.head)
    }

    val chLogicalPlan =
      CHLogicalPlan(totalPushDownProjects, pushDownFilters, Seq.empty, Seq.empty, sortOrders, limit)

    // 2. Determine the upper plan (Spark).

    // Rewrite project list by replacing push down expression to its reference.
    val rewrittenProjectList = {
      val totalPushDownProjectAttrMap =
        totalPushDownProjects.map(expr => expr.exprId -> expr.toAttribute).toMap
      projectList.map {
        _.transform {
          case e: NamedExpression => totalPushDownProjectAttrMap.getOrElse(e.exprId, e)
        }.asInstanceOf[NamedExpression]
      }
    }
    val rewrittenProjectAttrSet = rewrittenProjectList.map(_.toAttribute).distinct

    // Check if we don't need to extra Spark Project,
    // determined by the following two conditions (and):
    // 1. No column in residual projects.
    // 2. All residual filter columns are in the push down project list.
    val noExtraProject = {
      val pushDownProjectAttrs = AttributeSet(
        pushDownProjects.map(_.asInstanceOf[NamedExpression].toAttribute)
      )
      val residualProjectCols = AttributeSet(residualProjects.flatMap(_.references))
      val residualFilterCols = AttributeSet(residualFilters.flatMap(_.references))

      residualProjectCols.isEmpty && residualFilterCols.subsetOf(pushDownProjectAttrs)
    }

    if (noExtraProject) {
      // DON'T need extra Spark Project.

      // Use rewritten project attributes as CH plan output.
      val chExec = CHScanExec(
        rewrittenProjectAttrSet,
        sparkSession,
        chRelation,
        chLogicalPlan
      )
      residualFilter.map(FilterExec(_, chExec)).getOrElse(chExec)
    } else {
      // Need extra Spark Project.
      // Use total push down project attributes as CH plan output.
      val totalPushDownProjectAttrs = totalPushDownProjects.map(_.toAttribute)
      val chExec = CHScanExec(totalPushDownProjectAttrs, sparkSession, chRelation, chLogicalPlan)

      // Use rewritten project list as Spark Project output.
      ProjectExec(rewrittenProjectList, residualFilter.map(FilterExec(_, chExec)).getOrElse(chExec))
    }
  }
}

object CHAggregation {
  type ReturnType = PhysicalAggregation.ReturnType

  def rewriteAggregation(
    aggregateExpressions: Seq[AggregateExpression],
    resultExpressions: Seq[NamedExpression]
  ): (Seq[AggregateExpression], Seq[NamedExpression]) = {

    // Rewrites all `Average`s into the form of `Divide(Sum / Count)` so that we can push the
    // converted `Sum`s and `Count`s down to CH.
    val (averages, averagesEliminated) = aggregateExpressions.partition {
      case AggregateExpression(_: Average, _, _, _) => true
      case _                                        => false
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
    case PhysicalOperation(projects, filters, rel @ LogicalRelation(source: CHRelation, _, _, _))
        if projects.forall(_.isInstanceOf[Attribute]) =>
      Some((filters, rel, source, projects))
    case _ => Option.empty[ReturnType]
  }
}
