package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{AnalysisException, CHContext, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogTableType}

import scala.util.control.NonFatal

case class CreateFlashDatabaseCommand(chContext: CHContext,
                                      databaseName: String,
                                      ifNotExists: Boolean)
    extends RunnableCommand
    with CHCommand {
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

class CHDropDatabaseCommand(val chContext: CHContext,
                            databaseName: String,
                            ifExists: Boolean,
                            cascade: Boolean)
    extends DropDatabaseCommand(databaseName, ifExists, cascade)
    with CHCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    chCatalog.dropDatabase(databaseName, ifExists, cascade)
    Seq.empty[Row]
  }
}

class CHDropTableCommand(val chContext: CHContext,
                         tableName: TableIdentifier,
                         ifExists: Boolean,
                         isView: Boolean,
                         purge: Boolean)
    extends DropTableCommand(tableName, ifExists, isView, purge)
    with CHCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (!chCatalog.isTemporaryTable(tableName) && chCatalog.tableExists(tableName)) {
      // If the command DROP VIEW is to drop a table or DROP TABLE is to drop a view
      // issue an exception.
      chCatalog.getTableMetadata(tableName).tableType match {
        case CatalogTableType.VIEW if !isView =>
          throw new AnalysisException(
            "Cannot drop a view with DROP TABLE. Please use DROP VIEW instead"
          )
        case o if o != CatalogTableType.VIEW && isView =>
          throw new AnalysisException(
            s"Cannot drop a table with DROP VIEW. Please use DROP TABLE instead"
          )
        case _ =>
      }
    }

    if (chCatalog.isTemporaryTable(tableName) || chCatalog.tableExists(tableName)) {
      try {
        sparkSession.sharedState.cacheManager.uncacheQuery(sparkSession.table(tableName))
      } catch {
        case NonFatal(e) => log.warn(e.toString, e)
      }
      chCatalog.refreshTable(tableName)
      chCatalog.dropTable(tableName, ifExists, purge)
    } else if (ifExists) {
      // no-op
    } else {
      throw new AnalysisException(s"Table or view not found: ${tableName.identifier}")
    }
    Seq.empty[Row]
  }
}
