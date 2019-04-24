package org.apache.spark.sql.catalyst.catalog

import java.util.Locale

import com.pingcap.common.Node
import com.pingcap.tikv.meta.TiTableInfo
import org.apache.spark.sql.CHContext
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchTableException}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.ch.CHEngine
import org.apache.spark.sql.internal.StaticSQLConf

/**
 * Constants that are used for CH catalog.
 */
object CHCatalogConst {
  val TAB_META_ENGINE = "engine"

  val COL_META_PRIMARY_KEY = "primary_key"

  val DEFAULT_PARTITION_NUM = 128
  val DEFAULT_BUCKET_NUM = 8192
}

/**
 * Abilities that a CH catalog must have.
 */
trait CHCatalog {
  def createFlashDatabase(databaseDesc: CatalogDatabase, ignoreIfExists: Boolean): Unit

  def createFlashTable(tableDesc: CatalogTable,
                       query: Option[LogicalPlan],
                       ignoreIfExists: Boolean): Unit

  def createFlashTableFromTiDB(db: String,
                               tiTableInfo: TiTableInfo,
                               engine: CHEngine,
                               ignoreIfExists: Boolean): Unit

  def loadTableFromTiDB(db: String, tiTable: TiTableInfo, isOverwrite: Boolean): Unit

  def truncateTable(tableIdentifier: TableIdentifier): Unit

  // Following are per-CH-node entity existence checking.

  def databaseExists(db: String, node: Node): Boolean

  def tableExists(db: String, table: String, node: Node): Boolean
}

trait CHSessionCatalog extends SessionCatalog with CHCatalog {
  protected val chContext: CHContext

  protected lazy val globalTempDB: String =
    chContext.sparkSession.conf.get(StaticSQLConf.GLOBAL_TEMP_DATABASE).toLowerCase(Locale.ROOT)

  /**
   * Returns the catalog in which the database is.
   * Use for upper command to identify the catalog in order to take different actions.
   * @param database
   * @return
   */
  def catalogOf(database: Option[String] = None): Option[SessionCatalog]

  // Following are helper methods.

  protected def requireDbExists(db: String): Unit =
    if (!databaseExists(db)) {
      throw new NoSuchDatabaseException(db)
    }

  protected def requireTableExists(name: TableIdentifier): Unit =
    if (!tableExists(name)) {
      val db = name.database.getOrElse(currentDb)
      throw new NoSuchTableException(db = db, table = name.table)
    }
}

/**
 * Abilities that a CH external catalog must have.
 */
trait CHExternalCatalog extends ExternalCatalog with CHCatalog {
  override final def createFlashDatabase(databaseDesc: CatalogDatabase,
                                         ignoreIfExists: Boolean): Unit = {
    postToAll(CreateDatabasePreEvent(databaseDesc.name))
    doCreateFlashDatabase(databaseDesc, ignoreIfExists)
    postToAll(CreateDatabaseEvent(databaseDesc.name))
  }

  override final def createFlashTable(tableDesc: CatalogTable,
                                      query: Option[LogicalPlan],
                                      ignoreIfExists: Boolean): Unit = {
    postToAll(CreateTablePreEvent(tableDesc.identifier.database.get, tableDesc.identifier.table))
    doCreateFlashTable(tableDesc, query, ignoreIfExists)
    postToAll(CreateTableEvent(tableDesc.identifier.database.get, tableDesc.identifier.table))
  }

  override final def createFlashTableFromTiDB(db: String,
                                              tiTableInfo: TiTableInfo,
                                              engine: CHEngine,
                                              ignoreIfExists: Boolean): Unit = {
    val name = tiTableInfo.getName
    postToAll(CreateTablePreEvent(db, name))
    doCreateFlashTableFromTiDB(db, tiTableInfo, engine, ignoreIfExists)
    postToAll(CreateTableEvent(db, name))
  }

  protected def doCreateFlashDatabase(databaseDesc: CatalogDatabase, ignoreIfExists: Boolean): Unit

  protected def doCreateFlashTable(tableDesc: CatalogTable,
                                   query: Option[LogicalPlan],
                                   ignoreIfExists: Boolean): Unit

  protected def doCreateFlashTableFromTiDB(database: String,
                                           tiTableInfo: TiTableInfo,
                                           engine: CHEngine,
                                           ignoreIfExists: Boolean): Unit
}
