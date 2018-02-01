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
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, _}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, AttributeSet, Cast, CreateNamedStruct, Divide, Expression, IntegerLiteral, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, PhysicalAggregation, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.ch.mock.{TypesTestPlan, TypesTestRelation}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.AggUtils
import org.apache.spark.sql.execution.datasources.{CHScanRDD, LogicalRelation}
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{SparkSession, Strategy}

import scala.collection.mutable

class CHStrategy(sparkSession: SparkSession) extends Strategy with Logging {
  // -------------------- Dynamic configurations   --------------------
  val sqlConf: SQLConf = sparkSession.sqlContext.conf

  /**
    * Returns whether Code generation is enable
    *
    * @return true will enable code generation in plan generation,
    *         false otherwise.
    */
  private def enableCodeGen: Boolean = {
    sqlConf.getConfString(CHConfigConst.ENABLE_CODE_GEN, "true").toBoolean
  }

  private def enableAggPushdown: Boolean = {
    sqlConf.getConfString(CHConfigConst.ENABLE_PUSHDOWN_AGG, "true").toBoolean
  }

  /**
    * Matches a plan whose output should be small enough to be used in broadcast join.
    */
  private def canBroadcast(plan: LogicalPlan): Boolean = {
    plan.statistics.isBroadcastable ||
      (plan.statistics.sizeInBytes >= 0 &&
        plan.statistics.sizeInBytes <= sqlConf.autoBroadcastJoinThreshold)
  }

  private def canBuildRight(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | LeftOuter | LeftSemi | LeftAnti => true
    case j: ExistenceJoin => true
    case _ => false
  }

  private def canBuildLeft(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | RightOuter => true
    case _ => false
  }

  // -------------------- Physical plan generation --------------------
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan.collectFirst {
      case rel@LogicalRelation(_: TypesTestRelation, _: Option[Seq[Attribute]], _) =>
        TypesTestPlan(rel.output, sparkSession) :: Nil

      case rel@LogicalRelation(relation: CHRelation, output: Option[Seq[Attribute]], _) => plan match {
        case ReturnAnswer(rootPlan) =>
          rootPlan match {
            case Limit(IntegerLiteral(limit), Sort(order, true, child)) =>
              createTopNPlan(limit, order, child, child.output) :: Nil
            case Limit(IntegerLiteral(limit), Project(projectList, Sort(order, true, child))) =>
              createTopNPlan(limit, order, child, projectList) :: Nil
            case Limit(IntegerLiteral(limit), child) =>
              CollectLimitExec(limit, createTopNPlan(limit, Nil, child, child.output)) :: Nil
            case other => planLater(other) :: Nil
          }

        case Limit(IntegerLiteral(limit), Sort(order, true, child)) =>
          createTopNPlan(limit, order, child, child.output) :: Nil
        case Limit(IntegerLiteral(limit), Project(projectList, Sort(order, true, child))) =>
          createTopNPlan(limit, order, child, projectList) :: Nil
        case PhysicalOperation(projectList, filterPredicates, LogicalRelation(chr: CHRelation, _, _)) =>
          createCHPlan(relation.tables, rel, projectList, filterPredicates, null,
            chr.partitions, chr.decoders, chr.encoders) :: Nil

        case CHAggregation(groupingExpressions, aggregateExpressions, resultExpressions,
          CHAggregationProjection(filters, _, _, projects)) if enableAggPushdown =>
          var aggExp = aggregateExpressions
          var resultExp = resultExpressions
          if (!isSingleCHNode(relation)) {
            val rewriteResult = CHAggregation.rewriteAggregation(aggExp, resultExp)
            aggExp = rewriteResult._1
            resultExp = rewriteResult._2
          }
          // Add group / aggregate to CHPlan
          groupAggregateProjection(
            groupingExpressions, aggExp, resultExp, projects, filters, relation, rel)

        /**
          * Select the proper BROADCAST physical plan for join based on joining keys and size of logical plan.
          * Join plans other than broadcast is not implemented here.
          *
          * At first, uses the [[ExtractEquiJoinKeys]] pattern to find joins where at least some of the
          * predicates can be evaluated by matching join keys. If found,  Join implementations are chosen
          * with the following precedence:
          *
          * - Broadcast: if one side of the join has an estimated physical size that is smaller than the
          *     user-configurable [[SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]] threshold
          *     or if that side has an explicit broadcast hint (e.g. the user applied the
          *     [[org.apache.spark.sql.functions.broadcast()]] function to a DataFrame), then that side
          *     of the join will be broadcasted and the other side will be streamed, with no shuffling
          *     performed. If both sides of the join are eligible to be broadcasted then the
          *
          * If there is no joining keys, Join implementations are chosen with the following precedence:
          * - BroadcastNestedLoopJoin: if one side of the join could be broadcasted
          * - BroadcastNestedLoopJoin
          */
        // --- BroadcastHashJoin --------------------------------------------------------------------

        case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
          if canBuildRight(joinType) && canBroadcast(right) =>
          Seq(joins.BroadcastHashJoinExec(
            leftKeys, rightKeys, joinType, BuildRight, condition, planLater(left), planLater(right)))

        case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
          if canBuildLeft(joinType) && canBroadcast(left) =>
          Seq(joins.BroadcastHashJoinExec(
            leftKeys, rightKeys, joinType, BuildLeft, condition, planLater(left), planLater(right)))

        // --- Without joining keys ------------------------------------------------------------

        // Pick BroadcastNestedLoopJoin if one side could be broadcasted
        case j @ logical.Join(left, right, joinType, condition)
          if canBuildRight(joinType) && canBroadcast(right) =>
          joins.BroadcastNestedLoopJoinExec(
            planLater(left), planLater(right), BuildRight, joinType, condition) :: Nil
        case j @ logical.Join(left, right, joinType, condition)
          if canBuildLeft(joinType) && canBroadcast(left) =>
          joins.BroadcastNestedLoopJoinExec(
            planLater(left), planLater(right), BuildLeft, joinType, condition) :: Nil

        // --- Cases where this strategy does not apply ---------------------------------------------

        case _ => Nil
      }
    }.toSeq.flatten
  }

  def isSingleCHNode(relation: CHRelation): Boolean = {
    relation.tables.lengthCompare(1) == 0
  }

  def extractCHTopN(sortOrder: Seq[SortOrder], limit: Int): CHSqlTopN = {
    var topN = mutable.ListBuffer[CHSqlOrderByCol]()
    sortOrder.foreach(order =>
      order.child match {
        case AttributeReference(name, _, _, _) =>
          topN += CHSqlOrderByCol(name, order.direction.sql)
        case namedStructure@CreateNamedStruct(_) =>
          topN += CHSqlOrderByCol(
            namedStructure.nameExprs.map(CHUtil.expToCHString).mkString(","),
            order.direction.sql,
            namedStructure = true)
      }
    )
    new CHSqlTopN(topN, limit.toString)
  }

  def pruneTopNFilterProject(
    relation: LogicalRelation,
    limit: Int,
    projectList: Seq[NamedExpression],
    filterPredicates: Seq[Expression],
    source: CHRelation,
    sortOrder: Seq[SortOrder]): SparkPlan = {

    createCHPlan(source.tables, relation, projectList, filterPredicates, extractCHTopN(sortOrder, limit),
      source.partitions, source.decoders, source.encoders)
  }

  def createTopNPlan(
    limit: Int,
    sortOrder: Seq[SortOrder],
    child: LogicalPlan,
    project: Seq[NamedExpression]): SparkPlan = {

    child match {
      case PhysicalOperation(projectList, filters, rel@LogicalRelation(source: CHRelation, _, _))
        if filters.forall(CHUtil.isSupportedFilter) =>
        TakeOrderedAndProjectExec(
          limit, sortOrder, project,
          pruneTopNFilterProject(rel, limit, projectList, filters, source, sortOrder)
        )
      case _ => TakeOrderedAndProjectExec(limit, sortOrder, project, planLater(child))
    }
  }

  def groupAggregateProjection(
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    resultExpressions: Seq[NamedExpression],
    projectList: Seq[NamedExpression],
    filterPredicates: Seq[Expression],
    relation: CHRelation,
    rel: LogicalRelation,
    cHSqlTopN: CHSqlTopN = null): Seq[SparkPlan] = {
    val deterministicAggAliases = aggregateExpressions.collect {
      case e if e.deterministic => e.canonicalized -> Alias(e, e.toString())()
    }.toMap

    def aliasPushedPartialResult(e: AggregateExpression): Alias = {
      deterministicAggAliases.getOrElse(e.canonicalized, Alias(e, e.toString())())
    }

    val residualAggregateExpressions = aggregateExpressions.map { aggExpr =>
      // As `aggExpr` is being pushing down to Clickhouse, we need to replace the original Catalyst
      // aggregate expressions with new ones that merges the partial aggregation results returned by
      // Clickhouse.
      //
      // NOTE: Unlike simple aggregate functions (e.g., `Max`, `Min`, etc.), `Count` must be
      // replaced with a `Sum` to sum up the partial counts returned by Clickhouse.
      //
      // NOTE: All `Average`s should have already been rewritten into `Sum`s and `Count`s by the
      // `CHAggregation` pattern extractor.

      // An attribute referring to the partial aggregation results returned by Clickhouse.
      val partialResultRef = aliasPushedPartialResult(aggExpr).toAttribute

      aggExpr.aggregateFunction match {
        case e: Max   => aggExpr.copy(aggregateFunction = e.copy(child = partialResultRef))
        case e: Min   => aggExpr.copy(aggregateFunction = e.copy(child = partialResultRef))
        case e: Sum   => aggExpr.copy(aggregateFunction = e.copy(child = partialResultRef))
        case e: First => aggExpr.copy(aggregateFunction = e.copy(child = partialResultRef))
        case e: Count => if (!isSingleCHNode(relation)) aggExpr.copy(aggregateFunction = Sum(partialResultRef))
                         else aggExpr.copy(aggregateFunction = e.copy(children = partialResultRef::Nil))
        case _ => aggExpr
      }
    }

    val aggregation = mutable.ListBuffer[CHSqlAggFunc]()
    val groupByColumn = mutable.ListBuffer[String]()
    extractAggregation(groupingExpressions, aggregateExpressions.distinct, relation, aggregation, groupByColumn)

    val output = if (!isSingleCHNode(relation)) {
      (aggregateExpressions.map(aliasPushedPartialResult) ++ groupingExpressions).map {
        _.toAttribute
      }
    } else {
      resultExpressions.map(_.toAttribute)
    }

    val (pushdownFilters: Seq[Expression], _: Seq[Expression]) =
      filterPredicates.partition((expression: Expression) => CHUtil.isSupportedFilter(expression))

    val filtersString = if (pushdownFilters.isEmpty) null else CHUtil.expToCHString(pushdownFilters)

    val chSqlAgg = new CHSqlAgg(groupByColumn, aggregation)
    // As for required Columns, first we extract all aggregation functions and put them in the front most,
    // then we append group by columns.
    val requiredCols = if (!isSingleCHNode(relation)) {
      chSqlAgg.functions.map { _.toString } ++ chSqlAgg.groupByColumns
    } else {
      resultExpressions.map {
        case a@Alias(child, _) =>
          child match {
            case AttributeReference(attributeName, _, _, _) =>
              val idx = aggregateExpressions.map(e => e.aggregateFunction.toString()).indexOf(attributeName)
              aggregation(idx).toString()
            case _ => a.name
          }
        case other => other.name
      }
    }

    val chScanRDD = new CHScanRDD(sparkSession, output,
      relation.tables, requiredCols, filtersString, chSqlAgg, cHSqlTopN,
      relation.partitions, relation.decoders, relation.encoders)
    val chPlan = CHScanExec(output, chScanRDD, sparkSession, relation.tables, requiredCols,
      filtersString, chSqlAgg, cHSqlTopN,
      relation.partitions, relation.decoders, relation.encoders, enableCodeGen)

    if (!isSingleCHNode(relation)) {
      AggUtils.planAggregateWithoutDistinct(
        groupingExpressions,
        residualAggregateExpressions,
        resultExpressions,
        chPlan
      )
    } else {
      chPlan :: Nil
    }
  }

  def extractAggregation(
    groupByList: Seq[NamedExpression],
    aggregates: Seq[AggregateExpression],
    source: CHRelation,
    aggregations: mutable.ListBuffer[CHSqlAggFunc],
    groupByCols: mutable.ListBuffer[String]): Unit = {

    aggregates.foreach {
      case AggregateExpression(Average(arg), _, _, _) =>
        aggregations += CHSqlAggFunc("AVG", arg)
      case AggregateExpression(Sum(arg), _, _, _) =>
        aggregations += CHSqlAggFunc("SUM", arg)
      case AggregateExpression(Count(args), _, _, _) =>
        aggregations += CHSqlAggFunc("COUNT", args: _*)
      case AggregateExpression(Min(arg), _, _, _) =>
        aggregations += CHSqlAggFunc("MIN", arg)
      case AggregateExpression(Max(arg), _, _, _) =>
        aggregations += CHSqlAggFunc("MAX", arg)
      case AggregateExpression(First(arg, _), _, _, _) =>
        aggregations += CHSqlAggFunc("FIRST", arg)
      case AggregateExpression(Last(arg, _), _, _, _) =>
        aggregations += CHSqlAggFunc("LAST", arg)
      case _ =>
    }

    groupByList.foreach(groupByCols += CHUtil.expToCHString(_))
  }

  private def createCHPlan(
    tables: Seq[CHTableRef],
    relation: LogicalRelation,
    projectList: Seq[NamedExpression],
    filterPredicates: Seq[Expression],
    chSqlTopN: CHSqlTopN,
    partitions: Int,
    decoders: Int,
    encoders: Int): SparkPlan = {

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

    val filtersString = if (pushdownFilters.isEmpty) null else CHUtil.expToCHString(pushdownFilters)

    val chScanRDD = new CHScanRDD(sparkSession, output, tables, output.map(_.name), filtersString, null, chSqlTopN,
      partitions, decoders, encoders)
    val rdd = CHScanExec(output, chScanRDD, sparkSession, tables, output.map(_.name),
      filtersString, null, chSqlTopN, partitions, decoders, encoders, enableCodeGen)

    if (AttributeSet(projectList.map(_.toAttribute)) == projectSet &&
      filterSet.subsetOf(projectSet)) {
      residualFilter.map(FilterExec(_, rdd)).getOrElse(rdd)
    } else {
      ProjectExec(projectList, residualFilter.map(FilterExec(_, rdd)).getOrElse(rdd))
    }
  }
}

object CHAggregation {
  type ReturnType = PhysicalAggregation.ReturnType

  def rewriteAggregation(
    aggregateExpressions: Seq[AggregateExpression],
    resultExpressions: Seq[NamedExpression]): (Seq[AggregateExpression],Seq[NamedExpression]) = {

    // Rewrites all `Average`s into the form of `Divide(Sum / Count)` so that we can push the
    // converted `Sum`s and `Count`s down to Clickhouse.
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
