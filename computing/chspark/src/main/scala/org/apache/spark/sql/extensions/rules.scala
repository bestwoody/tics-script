package org.apache.spark.sql.extensions

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.CHSessionCatalog
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.ch.{CHConfigConst, CHRelation, CHTableRef}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.LogicalRelation

case class CHDDLRule(getOrCreateCHContext: SparkSession => CHContext)(sparkSession: SparkSession)
    extends Rule[LogicalPlan] {
  protected val chContext = getOrCreateCHContext(sparkSession)

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    // TODO: support other commands that may concern CH catalog.
    case CHCreateDatabase(databaseName, ifNotExists) =>
      CHCreateDatabaseCommand(chContext, databaseName, ifNotExists)
    case DropDatabaseCommand(databaseName, ifExists, cascade) =>
      new CHDropDatabaseCommand(chContext, databaseName, ifExists, cascade)
    case ShowDatabasesCommand(databasePattern) =>
      new CHShowDatabasesCommand(chContext, databasePattern)
    case SetDatabaseCommand(databaseName) =>
      CHSetDatabaseCommand(chContext, databaseName)
    case CHCreateTable(tableDesc, ifNotExists) =>
      CHCreateTableCommand(chContext, tableDesc, ifNotExists)
    case CreateTableFromTiDB(tiTable, properties, ifNotExists) =>
      CHCreateTableFromTiDBCommand(chContext, tiTable, properties, ifNotExists)
    case LoadDataFromTiDB(tiTable, isOverwrite) =>
      CHLoadDataFromTiDBCommand(chContext, tiTable, isOverwrite)
    case TruncateTableCommand(tableName, partitionSpec) =>
      new CHTruncateTableCommand(chContext, tableName, partitionSpec)
    // TODO: support desc db/column/etc.
    case DescribeTableCommand(table, partitionSpec, isExtended) =>
      new CHDescribeTableCommand(chContext, table, partitionSpec, isExtended)
    case DropTableCommand(tableName, ifExists, isView, purge) =>
      new CHDropTableCommand(chContext, tableName, ifExists, isView, purge)
    case ShowTablesCommand(databaseName, tableIdentifierPattern, isExtended, partitionSpec) =>
      new CHShowTablesCommand(
        chContext,
        databaseName,
        tableIdentifierPattern,
        isExtended,
        partitionSpec
      )
  }
}

case class CHResolutionRule(getOrCreateCHContext: SparkSession => CHContext)(
  sparkSession: SparkSession
) extends Rule[LogicalPlan] {
  protected val chContext = getOrCreateCHContext(sparkSession)

  protected val resolveCHRelation: TableIdentifier => CHRelation =
    (tableIdentifier: TableIdentifier) =>
      new CHRelation(
        CHTableRef
          .ofCluster(
            chContext.cluster,
            tableIdentifier.database.getOrElse(chContext.chCatalog.getCurrentDatabase),
            tableIdentifier.table
          ),
        chContext.sqlContext.conf
          .getConfString(
            CHConfigConst.PARTITIONS_PER_SPLIT,
            CHConfigConst.DEFAULT_PARTITIONS_PER_SPLIT.toString
          )
          .toInt
      )(chContext.sqlContext)

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case UnresolvedRelation(tableIdentifier)
        if chContext.chCatalog
          .catalogOf(tableIdentifier.database)
          .exists(_.isInstanceOf[CHSessionCatalog]) =>
      LogicalRelation(resolveCHRelation(tableIdentifier))
    case i @ InsertIntoTable(UnresolvedRelation(tableIdentifier), _, child, _, _)
        if chContext.chCatalog
          .catalogOf(tableIdentifier.database)
          .exists(_.isInstanceOf[CHSessionCatalog]) =>
      i.copy(table = LogicalRelation(resolveCHRelation(tableIdentifier)))
  }
}
