package org.apache.spark.sql.catalyst.catalog

import com.pingcap.tikv.meta.TiTableInfo
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.{AnalysisException, CHContext}
import org.apache.spark.sql.catalyst.analysis.EmptyFunctionRegistry
import org.apache.spark.sql.ch.CHEngine

/**
 * The very concrete catalog that operates CH entities through CH external catalog.
 * Does not regard temp views or anything else.
 * @param chContext
 * @param chExternalCatalog
 */
class CHConcreteSessionCatalog(val chContext: CHContext)(chExternalCatalog: CHExternalCatalog)
    extends SessionCatalog(
      chExternalCatalog,
      EmptyFunctionRegistry,
      chContext.sqlContext.conf
    )
    with CHSessionCatalog {

  override def catalogOf(database: Option[String]): Option[SessionCatalog] = {
    val db = database.getOrElse(getCurrentDatabase)
    if (databaseExists(db))
      Some(this)
    else
      None
  }

  override def createFlashDatabase(databaseDesc: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    val dbName = formatDatabaseName(databaseDesc.name)
    if (dbName == globalTempDB) {
      throw new AnalysisException(
        s"$globalTempDB is a system preserved database, " +
          "you cannot create a database with this name."
      )
    }
    chExternalCatalog.createFlashDatabase(databaseDesc.copy(dbName), ignoreIfExists)
  }

  override def createFlashTable(tableDesc: CatalogTable, ignoreIfExists: Boolean): Unit = {
    val db = formatDatabaseName(tableDesc.identifier.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableDesc.identifier.table)
    requireDbExists(db)
    chExternalCatalog
      .createFlashTable(tableDesc.copy(TableIdentifier(table, Some(db))), ignoreIfExists)
  }

  override def createFlashTableFromTiDB(database: String,
                                        tiTable: TiTableInfo,
                                        engine: CHEngine,
                                        ignoreIfExists: Boolean): Unit = {
    val db = formatDatabaseName(database)
    requireDbExists(db)
    chExternalCatalog.createFlashTableFromTiDB(db, tiTable, engine, ignoreIfExists)
  }

  override def loadTableFromTiDB(database: String,
                                 tiTable: TiTableInfo,
                                 isOverwrite: Boolean): Unit = {
    val db = formatDatabaseName(database)
    val table = formatTableName(tiTable.getName)
    requireDbExists(db)
    requireTableExists(TableIdentifier(table, Some(db)))
    chExternalCatalog.loadTableFromTiDB(db, tiTable, isOverwrite)
  }
}
