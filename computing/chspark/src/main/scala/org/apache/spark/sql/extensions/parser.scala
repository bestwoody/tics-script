package org.apache.spark.sql.extensions

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{Exists, Expression, ListQuery, NamedExpression, ScalarSubquery}
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, InsertIntoTable, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{CHContext, SparkSession}

case class CHParser(getOrCreateCHContext: SparkSession => CHContext)(sparkSession: SparkSession,
                                                                     delegate: ParserInterface)
    extends ParserInterface {
  private lazy val chContext = getOrCreateCHContext(sparkSession)

  private lazy val internal = new CHSqlParser(sparkSession.sqlContext.conf)

  /**
   * WAR to lead Spark to consider this relation being on local files.
   * Otherwise Spark will lookup this relation in his session catalog.
   * See ResolveRelations.isRunningDirectlyOnFiles() for detail.
   */
  private val qualifyTableIdentifier: PartialFunction[LogicalPlan, LogicalPlan] = {
    case r @ UnresolvedRelation(tableIdentifier)
        if chContext.chCatalog.tableExists(
          TableIdentifier(
            tableIdentifier.table,
            Some(tableIdentifier.database.getOrElse(chContext.chCatalog.getCurrentDatabase))
          )
        ) =>
      r.copy(
        TableIdentifier(
          tableIdentifier.table,
          Some(tableIdentifier.database.getOrElse(chContext.chCatalog.getCurrentDatabase))
        )
      )
    case i @ InsertIntoTable(r @ UnresolvedRelation(tableIdentifier), _, _, _, _)
        if chContext.chCatalog.tableExists(
          TableIdentifier(
            tableIdentifier.table,
            Some(tableIdentifier.database.getOrElse(chContext.chCatalog.getCurrentDatabase))
          )
        ) =>
      i.copy(
        r.copy(
          TableIdentifier(
            tableIdentifier.table,
            Some(tableIdentifier.database.getOrElse(chContext.chCatalog.getCurrentDatabase))
          )
        )
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
