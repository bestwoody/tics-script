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
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, AttributeSet, ExprId, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.planning.{PhysicalAggregation, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.ch.mock._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.{FilterExec, SparkPlan, aggregate}
import org.apache.spark.sql.{SparkSession, Strategy}

import scala.collection.mutable


class CHStrategy(sparkSession: SparkSession, aggPushdown: Boolean) extends Strategy with Logging {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan.collectFirst {
      case rel@LogicalRelation(relation: MockSimpleRelation, _: Option[Seq[Attribute]], _) =>
        MockSimplePlan(rel.output, sparkSession) :: Nil
      case rel@LogicalRelation(relation: MockArrowRelation, _: Option[Seq[Attribute]], _) =>
        MockArrowPlan(rel.output, sparkSession) :: Nil
      case rel@LogicalRelation(relation: TypesTestRelation, _: Option[Seq[Attribute]], _) =>
        TypesTestPlan(rel.output, sparkSession) :: Nil
      case rel@LogicalRelation(relation: CHRelation, output: Option[Seq[Attribute]], _) => {
        plan match {
          case PhysicalOperation(projectList, filterPredicates, LogicalRelation(chr: CHRelation, _, _)) =>
            createCHPlan(relation.tables, rel, projectList, filterPredicates, chr.partitions, chr.decoders) :: Nil
          case _ => Nil
        }
      }
      case CHAggregation(
        groupingExpressions,
        aggregateExpressions,
        resultExpressions,
        CHAggregationProjection(filters, rel, relation, projects)) if aggPushdown => {
        // Add group / aggregate to CHPlan
        groupAggregateProjection(
          groupingExpressions,
          aggregateExpressions,
          resultExpressions,
          projects,
          filters,
          relation,
          rel)
      }
    }.toSeq.flatten
  }

  def groupAggregateProjection(
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    resultExpressions: Seq[NamedExpression],
    projectList: Seq[NamedExpression],
    filterPredicates: Seq[Expression],
    relation: CHRelation,
    rel: LogicalRelation): Seq[SparkPlan] = {

    val aliasMap = mutable.HashMap[(Boolean, Expression), Alias]()
    val avgPushdownRewriteMap = mutable.HashMap[ExprId, List[AggregateExpression]]()

    def toAlias(expr: AggregateExpression) =
      if (!expr.deterministic) {
        Alias(expr, expr.toString())()
      } else {
        aliasMap.getOrElseUpdate(
          (expr.deterministic, expr.canonicalized), Alias(expr, expr.toString)())
      }

    def newAggregate(aggFunc: AggregateFunction, originalAggExpr: AggregateExpression) =
      AggregateExpression(
        aggFunc,
        originalAggExpr.mode,
        originalAggExpr.isDistinct,
        originalAggExpr.resultId)

    val pushdownAggregates = aggregateExpressions.flatMap { aggExpr =>
      avgPushdownRewriteMap.getOrElse(aggExpr.resultId, List(aggExpr))
    }.distinct

    val residualAggregateExpressions = aggregateExpressions.map { aggExpr =>
      aggExpr.aggregateFunction match {
        // here aggExpr is the original AggregationExpression
        // and will be pushed down to TiKV
        case Max(_) => newAggregate(Max(toAlias(aggExpr).toAttribute), aggExpr)
        case Min(_) => newAggregate(Min(toAlias(aggExpr).toAttribute), aggExpr)
        case Count(_) => newAggregate(Sum(toAlias(aggExpr).toAttribute), aggExpr)
        case Sum(_) => newAggregate(Sum(toAlias(aggExpr).toAttribute), aggExpr)
        case First(_, ignoreNullsExpr) =>
          newAggregate(First(toAlias(aggExpr).toAttribute, ignoreNullsExpr), aggExpr)
        case _ => aggExpr
      }
    }

    val aggregation = mutable.ListBuffer[CHSqlAggFunc]()
    val groupByColumn = mutable.ListBuffer[String]()
    extractAggregation(groupingExpressions, pushdownAggregates, relation, aggregation, groupByColumn)

    val output = (pushdownAggregates.map(x => toAlias(x)) ++ groupingExpressions).map(_.toAttribute)

    val (pushdownFilters: Seq[Expression], _: Seq[Expression]) =
      filterPredicates.partition((expression: Expression) => CHUtil.isSupportedFilter(expression))

    val filtersString = if (pushdownFilters.isEmpty) {
      null
    } else {
      CHUtil.getFilterString(pushdownFilters)
    }

    val chSqlAgg = new CHSqlAgg(groupByColumn, aggregation)
    val requiredCols = resultExpressions.map {
      case a@Alias(child, _) =>
        child match {
          case AttributeReference(attributeName, _, _, _) =>
            val idx = aggregateExpressions.map(e => e.aggregateFunction.toString()).indexOf(attributeName)
            aggregation(idx).toString()
          case _ => a.name
        }
      case other => other.name
    }
    val chPlan = CHPlan(resultExpressions.map(_.toAttribute), sparkSession,
      relation.tables, requiredCols, filtersString, chSqlAgg, relation.partitions, relation.decoders)

    //  aggregate.AggUtils.planAggregateWithoutDistinct(
    //    groupingExpressions,
    //    residualAggregateExpressions,
    //    resultExpressions,
    chPlan :: Nil
    //  )
  }

  def extractAggregation(
    groupByList: Seq[NamedExpression],
    aggregates: Seq[AggregateExpression],
    source: CHRelation,
    aggregations: mutable.ListBuffer[CHSqlAggFunc],
    groupByCols: mutable.ListBuffer[String]): Unit = {

    aggregates.foreach {
      case AggregateExpression(Average(arg), _, _, _) =>
        aggregations += new CHSqlAggFunc("AVG", CHUtil.getFilterString(arg))
      case AggregateExpression(Sum(arg), _, _, _) =>
        aggregations += new CHSqlAggFunc("SUM", CHUtil.getFilterString(arg))
      case AggregateExpression(Count(args), _, _, _) =>
        aggregations += new CHSqlAggFunc("COUNT", CHUtil.getFilterString(args.head))
      case AggregateExpression(Min(arg), _, _, _) =>
        aggregations += new CHSqlAggFunc("MIN", CHUtil.getFilterString(arg))
      case AggregateExpression(Max(arg), _, _, _) =>
        aggregations += new CHSqlAggFunc("MAX", CHUtil.getFilterString(arg))
      case AggregateExpression(First(arg, _), _, _, _) =>
        aggregations += new CHSqlAggFunc("FIRST", CHUtil.getFilterString(arg))
      case _ =>
    }

    groupByList.foreach(groupByCols += CHUtil.getFilterString(_))
  }

  private def createCHPlan(
    tables: Seq[CHTableRef],
    relation: LogicalRelation,
    projectList: Seq[NamedExpression],
    filterPredicates: Seq[Expression],
    partitions: Int,
    decoders: Int,
    aggregation: CHSqlAgg = null): SparkPlan = {

    val projectSet = AttributeSet(projectList.flatMap(_.references))
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))
    val requiredColumns = (projectSet ++ filterSet).toSeq.map(_.name)

    val nameSet = requiredColumns.toSet
    var output = relation.output.filter(attr => nameSet(attr.name))

    // TODO: Choose the smallest column (in prime keys, or the timestamp key of MergeTree) as dummy output
    if (output.isEmpty) {
      output = Seq(relation.output.head)
    }

    val (pushdownFilters: Seq[Expression], residualFilters: Seq[Expression]) =
      filterPredicates.partition((expression: Expression) => CHUtil.isSupportedFilter(expression))
    val residualFilter: Option[Expression] = residualFilters.reduceLeftOption(And)

    val filtersString = if (pushdownFilters.isEmpty) {
      null
    } else {
      CHUtil.getFilterString(pushdownFilters)
    }

    val rdd = CHPlan(output, sparkSession, tables, output.map(_.name), filtersString, aggregation,
      partitions, decoders)
    residualFilter.map(FilterExec(_, rdd)).getOrElse(rdd)
  }
}

object CHAggregation {
  type ReturnType = PhysicalAggregation.ReturnType

  def unapply(a: Any): Option[ReturnType] = a match {
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
