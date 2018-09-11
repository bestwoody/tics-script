package org.apache.spark.sql.catalyst.catalog

import com.pingcap.tikv.meta.TiTableInfo
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.CHContext
import org.apache.spark.sql.catalyst.analysis.{EmptyFunctionRegistry, NoSuchDatabaseException, NoSuchTableException}
import org.apache.spark.sql.ch.CHEngine

class CHConcreteSessionCatalog(chContext: CHContext)(chExternalCatalog: CHExternalCatalog)
    extends SessionCatalog(
      chExternalCatalog,
      EmptyFunctionRegistry,
      chContext.sqlContext.conf
    )
    with CHSessionCatalog {
  private def requireDbExists(db: String): Unit =
    if (!databaseExists(db)) {
      throw new NoSuchDatabaseException(db)
    }

  private def requireTableExists(name: TableIdentifier): Unit =
    if (!tableExists(name)) {
      val db = name.database.getOrElse(currentDb)
      throw new NoSuchTableException(db = db, table = name.table)
    }

  override def catalogOf(database: Option[String]): Option[SessionCatalog] = {
    val db = database.getOrElse(getCurrentDatabase)
    if (databaseExists(db))
      Some(this)
    else
      None
  }

  override def createCHDatabase(databaseDesc: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    val dbName = formatDatabaseName(databaseDesc.name)
    chExternalCatalog.createCHDatabase(databaseDesc.copy(dbName), ignoreIfExists)
  }

  override def createCHTable(tableDesc: CatalogTable, ignoreIfExists: Boolean): Unit = {
    val db = formatDatabaseName(tableDesc.identifier.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableDesc.identifier.table)
    requireDbExists(db)
    chExternalCatalog
      .createCHTable(tableDesc.copy(TableIdentifier(table, Some(db))), ignoreIfExists)
  }

  def createTableFromTiDB(database: String,
                          tiTable: TiTableInfo,
                          engine: CHEngine,
                          ignoreIfExists: Boolean): Unit = {
    val db = formatDatabaseName(database)
    val table = formatTableName(tiTable.getName)
    requireDbExists(db)
    chExternalCatalog.createTableFromTiDB(db, tiTable, engine, ignoreIfExists)
  }

  def loadTableFromTiDB(database: String, tiTable: TiTableInfo, isOverwrite: Boolean): Unit = {
    val db = formatDatabaseName(database)
    val table = formatTableName(tiTable.getName)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Some(db)))
    chExternalCatalog.loadTableFromTiDB(db, tiTable, isOverwrite)
  }
}
