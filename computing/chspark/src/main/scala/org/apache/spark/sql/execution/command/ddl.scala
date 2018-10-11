package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{AnalysisException, CHContext, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogTableType}

import scala.util.control.NonFatal

case class CreateFlashDatabaseCommand(chContext: CHContext,
                                      databaseName: String,
                                      ifNotExists: Boolean)
    extends CHCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    chCatalog.createFlashDatabase(
      CatalogDatabase(
        databaseName,
        "flash",
        chCatalog.getDefaultDBPath(databaseName),
        Map.empty[String, String]
      ),
      ifNotExists
    )
    Seq.empty[Row]
  }
}

case class CHDropDatabaseCommand(chContext: CHContext, delegate: DropDatabaseCommand)
    extends CHDelegateCommand(delegate) {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    chCatalog.dropDatabase(delegate.databaseName, delegate.ifExists, delegate.cascade)
    Seq.empty[Row]
  }
}

case class CHDropTableCommand(chContext: CHContext, delegate: DropTableCommand)
    extends CHDelegateCommand(delegate) {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (!chCatalog.isTemporaryTable(delegate.tableName) && chCatalog.tableExists(
          delegate.tableName
        )) {
      // If the command DROP VIEW is to drop a table or DROP TABLE is to drop a view
      // issue an exception.
      chCatalog.getTableMetadata(delegate.tableName).tableType match {
        case CatalogTableType.VIEW if !delegate.isView =>
          throw new AnalysisException(
            "Cannot drop a view with DROP TABLE. Please use DROP VIEW instead"
          )
        case o if o != CatalogTableType.VIEW && delegate.isView =>
          throw new AnalysisException(
            s"Cannot drop a table with DROP VIEW. Please use DROP TABLE instead"
          )
        case _ =>
      }
    }

    if (chCatalog.isTemporaryTable(delegate.tableName) || chCatalog.tableExists(delegate.tableName)) {
      try {
        sparkSession.sharedState.cacheManager.uncacheQuery(sparkSession.table(delegate.tableName))
      } catch {
        case NonFatal(e) => log.warn(e.toString, e)
      }
      chCatalog.refreshTable(delegate.tableName)
      chCatalog.dropTable(delegate.tableName, delegate.ifExists, delegate.purge)
    } else if (delegate.ifExists) {
      // no-op
    } else {
      throw new AnalysisException(s"Table or view not found: ${delegate.tableName.identifier}")
    }
    Seq.empty[Row]
  }
}
