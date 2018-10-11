package org.apache.spark.sql.extensions

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{Exists, Expression, ListQuery, NamedExpression, ScalarSubquery}
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.execution.command.{CreateViewCommand, ExplainCommand}
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{CHContext, SparkSession}

case class CHParser(getOrCreateCHContext: SparkSession => CHContext)(sparkSession: SparkSession,
                                                                     delegate: ParserInterface)
    extends ParserInterface {
  private lazy val chContext = getOrCreateCHContext(sparkSession)

  private lazy val internal = new CHSqlParser(sparkSession.sqlContext.conf)

  /**
   * Qualify table name before table resolution, concerning the following conditions:
   * 1 For qualified table name, let it go as the resolution rule that leverages catalog will resolve it correctly.
   * 2 For unqualified table name, a little bit complicated:
   * 2.1 Temp view has precedence over legacy and CH tables, so for temp view we leave it unqualified.
   * 2.2 Create table plan needs to be qualified with CH catalog's current database,
   * otherwise Spark will qualify it using its own session catalog's current database.
   * 2.3 Otherwise we qualify it anyhow and let catalog resolves it as:
   * 2.3.1 Spark's ResolveRelation rule will arbitrarily fail the analysis once it cannot find the table in catalog,
   * unless it is "running directly on files" (refer to ResolveRelations.isRunningDirectlyOnFiles()).
   * If table exists in CH, we lose the opportunity to re-resolve in CH resolution rule. Qualifying is to WAR this constraint.
   * 2.3.2 Qualifying does no harm to catalog's relation lookup.
   */
  private val qualifyTableIdentifier: PartialFunction[LogicalPlan, LogicalPlan] = {
    case r @ UnresolvedRelation(tableIdentifier)
        // When getting temp view, we leverage legacy catalog.
        if tableIdentifier.database.isEmpty && chContext.legacyCatalog
          .getTempView(tableIdentifier.table)
          .isEmpty =>
      r.copy(
        TableIdentifier(
          tableIdentifier.table,
          Some(tableIdentifier.database.getOrElse(chContext.chCatalog.getCurrentDatabase))
        )
      )
    case i @ InsertIntoTable(r @ UnresolvedRelation(tableIdentifier), _, _, _, _)
        // When getting temp view, we leverage legacy catalog.
        if tableIdentifier.database.isEmpty && chContext.legacyCatalog
          .getTempView(tableIdentifier.table)
          .isEmpty =>
      i.copy(
        r.copy(
          TableIdentifier(
            tableIdentifier.table,
            Some(tableIdentifier.database.getOrElse(chContext.chCatalog.getCurrentDatabase))
          )
        )
      )
    case ce @ CreateTable(tableDesc, _, _) if tableDesc.identifier.database.isEmpty =>
      ce.copy(
        tableDesc
          .copy(tableDesc.identifier.copy(database = Some(chContext.chCatalog.getCurrentDatabase)))
      )
    case f @ Filter(condition, _) =>
      f.copy(condition = condition.transform {
        case e @ Exists(plan, _, _)         => e.copy(plan = plan.transform(qualifyTableIdentifier))
        case ls @ ListQuery(plan, _, _, _)  => ls.copy(plan = plan.transform(qualifyTableIdentifier))
        case s @ ScalarSubquery(plan, _, _) => s.copy(plan = plan.transform(qualifyTableIdentifier))
      })
    case p @ Project(projectList, _) =>
      p.copy(projectList = projectList.map(_.transform {
        case s @ ScalarSubquery(plan, _, _) => s.copy(plan = plan.transform(qualifyTableIdentifier))
      }.asInstanceOf[NamedExpression]))
    case cv @ CreateViewCommand(_, _, _, _, _, child, _, _, _) =>
      cv.copy(child = child.transform(qualifyTableIdentifier))
    case w @ With(_, cteRelations) =>
      w.copy(
        cteRelations = cteRelations
          .map(
            tuple =>
              (tuple._1, tuple._2.copy(child = tuple._2.child.transform(qualifyTableIdentifier)))
          )
      )
    case e @ ExplainCommand(logicalPlan, _, _, _) =>
      e.copy(logicalPlan = logicalPlan.transform(qualifyTableIdentifier))
  }

  override def parsePlan(sqlText: String): LogicalPlan =
    internal.parsePlan(sqlText).transform(qualifyTableIdentifier)

  override def parseExpression(sqlText: String): Expression =
    internal.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    internal.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    internal.parseFunctionIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType =
    internal.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType =
    internal.parseDataType(sqlText)
}
