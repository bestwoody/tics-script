package org.apache.spark.sql.catalyst.catalog

import com.pingcap.tikv.meta.TiTableInfo
import org.apache.spark.sql.ch.CHEngine

/**
 * Constants that are used for CH catalog.
 */
object CHCatalogConst {
  val TAB_META_ENGINE = "engine"
  val TAB_META_PARTITION_NUM = "partition_num"
  val TAB_META_BUCKET_NUM = "bucket_num"

  val COL_META_PRIMARY_KEY = "primary_key"

  val DEFAULT_PARTITION_NUM = 128
  val DEFAULT_BUCKET_NUM = 8192
}

/**
 * Abilities that a CH catalog must have.
 */
trait CHCatalog {
  def createCHDatabase(databaseDesc: CatalogDatabase, ignoreIfExists: Boolean): Unit

  def createCHTable(tableDesc: CatalogTable, ignoreIfExists: Boolean): Unit

  def createTableFromTiDB(db: String,
                          tiTableInfo: TiTableInfo,
                          engine: CHEngine,
                          ignoreIfExists: Boolean): Unit

  def loadTableFromTiDB(db: String, tiTable: TiTableInfo, isOverwrite: Boolean): Unit
}

trait CHSessionCatalog extends SessionCatalog with CHCatalog {

  /**
   * Returns the catalog in which the database is.
   * Use for upper command to identify the catalog in order to take different actions.
   * @param database
   * @return
   */
  def catalogOf(database: Option[String] = None): Option[SessionCatalog]
}

/**
 * Abilities that a CH external catalog must have.
 */
trait CHExternalCatalog extends ExternalCatalog with CHCatalog {
  override final def createCHDatabase(databaseDesc: CatalogDatabase,
                                      ignoreIfExists: Boolean): Unit = {
    postToAll(CreateDatabasePreEvent(databaseDesc.name))
    doCreateCHDatabase(databaseDesc, ignoreIfExists)
    postToAll(CreateDatabaseEvent(databaseDesc.name))
  }

  override final def createCHTable(tableDesc: CatalogTable, ignoreIfExists: Boolean): Unit = {
    postToAll(CreateTablePreEvent(tableDesc.identifier.database.get, tableDesc.identifier.table))
    doCreateCHTable(tableDesc, ignoreIfExists)
    postToAll(CreateTableEvent(tableDesc.identifier.database.get, tableDesc.identifier.table))
  }

  override final def createTableFromTiDB(db: String,
                                         tiTableInfo: TiTableInfo,
                                         engine: CHEngine,
                                         ignoreIfExists: Boolean): Unit = {
    val name = tiTableInfo.getName
    postToAll(CreateTablePreEvent(db, name))
    doCreateTableFromTiDB(db, tiTableInfo, engine, ignoreIfExists)
    postToAll(CreateTableEvent(db, name))
  }

  protected def doCreateCHDatabase(databaseDesc: CatalogDatabase, ignoreIfExists: Boolean): Unit

  protected def doCreateCHTable(tableDesc: CatalogTable, ignoreIfExists: Boolean): Unit

  protected def doCreateTableFromTiDB(database: String,
                                      tiTableInfo: TiTableInfo,
                                      engine: CHEngine,
                                      ignoreIfExists: Boolean): Unit
}
