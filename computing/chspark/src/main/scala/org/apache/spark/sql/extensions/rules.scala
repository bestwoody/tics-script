package org.apache.spark.sql.extensions

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.CHSessionCatalog
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.ch.{CHConfigConst, CHRelation, CHTableRef}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{CreateTable, LogicalRelation}

case class CHDDLRule(getOrCreateCHContext: SparkSession => CHContext)(sparkSession: SparkSession)
    extends Rule[LogicalPlan] {
  private lazy val chContext = getOrCreateCHContext(sparkSession)

  /**
   * Validate catalog of the table being operated on.
   * Flash table cannot be created on legacy catalog and vice-versa.
   * @param database
   * @param isFlash
   */
  private def validateCatalog(database: Option[String], isFlash: Boolean): Unit = {
    if (isFlash && !chContext.chCatalog.catalogOf(database).orNull.isInstanceOf[CHSessionCatalog]) {
      throw new AnalysisException(
        s"Cannot create Flash table under non-Flash database '${database.getOrElse(chContext.chCatalog.getCurrentDatabase)}'"
      )
    }
    if (!isFlash && chContext.chCatalog.catalogOf(database).orNull.isInstanceOf[CHSessionCatalog]) {
      throw new AnalysisException(
        s"Cannot create Spark-managed table under Flash database '${database.getOrElse(chContext.chCatalog.getCurrentDatabase)}'"
      )
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    // TODO: support other commands that may concern CH catalog.
    case CreateFlashDatabase(databaseName, ifNotExists) =>
      CreateFlashDatabaseCommand(chContext, databaseName, ifNotExists)
    case CreateFlashTable(tableDesc, ifNotExists) =>
      validateCatalog(tableDesc.identifier.database, isFlash = true)
      CreateFlashTableCommand(chContext, tableDesc, ifNotExists)
    case CreateFlashTableFromTiDB(tiTable, properties, ifNotExists) =>
      validateCatalog(tiTable.database, isFlash = true)
      CreateFlashTableFromTiDBCommand(chContext, tiTable, properties, ifNotExists)
    case LoadDataFromTiDB(tiTable, isOverwrite) =>
      validateCatalog(tiTable.database, isFlash = true)
      LoadDataFromTiDBCommand(chContext, tiTable, isOverwrite)
    case ct @ CreateTable(tableDesc, _, _) =>
      validateCatalog(tableDesc.identifier.database, isFlash = false)
      ct
    case d @ DropDatabaseCommand(_, _, _) =>
      CHDropDatabaseCommand(chContext, d)
    case s @ ShowDatabasesCommand(_) =>
      CHShowDatabasesCommand(chContext, s)
    case s @ SetDatabaseCommand(_) =>
      CHSetDatabaseCommand(chContext, s)
    case t @ TruncateTableCommand(_, _) =>
      CHTruncateTableCommand(chContext, t)
    // TODO: support desc db/column/etc.
    case d @ DescribeTableCommand(_, _, _) =>
      CHDescribeTableCommand(chContext, d)
    case d @ DropTableCommand(_, _, _, _) =>
      CHDropTableCommand(chContext, d)
    case s @ ShowTablesCommand(_, _, _, _) =>
      CHShowTablesCommand(chContext, s)
    case s @ ShowCreateTableCommand(_) =>
      CHShowCreateTableCommand(chContext, s)
  }
}

case class CHResolutionRule(getOrCreateCHContext: SparkSession => CHContext)(
  sparkSession: SparkSession
) extends Rule[LogicalPlan] {
  protected lazy val chContext = getOrCreateCHContext(sparkSession)

  protected val resolveCHRelation: TableIdentifier => CHRelation =
    (tableIdentifier: TableIdentifier) => {
      val qualified = tableIdentifier.copy(
        database = Some(tableIdentifier.database.getOrElse(chContext.chCatalog.getCurrentDatabase))
      )
      // A decent error.
      if (!chContext.chCatalog.tableExists(qualified)) {
        throw new NoSuchTableException(qualified.database.get, qualified.table)
      }
      new CHRelation(
        CHTableRef
          .ofCluster(
            chContext.cluster,
            qualified.database.get,
            qualified.table
          ),
        chContext.sqlContext.conf
          .getConfString(
            CHConfigConst.PARTITIONS_PER_SPLIT,
            CHConfigConst.DEFAULT_PARTITIONS_PER_SPLIT.toString
          )
          .toInt
      )(chContext.sqlContext)
    }

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case UnresolvedRelation(tableIdentifier)
        if chContext.chCatalog
          .catalogOf(tableIdentifier.database)
          .exists(_.isInstanceOf[CHSessionCatalog]) =>
      LogicalRelation(resolveCHRelation(tableIdentifier))
    case i @ InsertIntoTable(UnresolvedRelation(tableIdentifier), _, _, _, _)
        if chContext.chCatalog
          .catalogOf(tableIdentifier.database)
          .exists(_.isInstanceOf[CHSessionCatalog]) =>
      i.copy(table = LogicalRelation(resolveCHRelation(tableIdentifier)))
  }
}
