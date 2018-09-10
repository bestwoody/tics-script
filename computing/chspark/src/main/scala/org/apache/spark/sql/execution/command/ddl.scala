package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{CHContext, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.CatalogDatabase

import scala.util.control.NonFatal

case class CHCreateDatabaseCommand(chContext: CHContext, databaseName: String, ifNotExists: Boolean)
    extends RunnableCommand
    with CHCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    chCatalog.createCHDatabase(
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
    if (chCatalog.tableExists(tableName)) {
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
      throw new NoSuchTableException(tableName.database.getOrElse(""), tableName.table)
    }

    Seq.empty[Row]
  }
}
