package org.apache.spark.sql.catalyst.catalog

import java.net.URI
import java.util.concurrent.Callable

import com.pingcap.tikv.meta.TiTableInfo
import org.apache.spark.sql.{AnalysisException, CHContext}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{EmptyFunctionRegistry, NoSuchDatabaseException}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.ch.CHEngine
import org.apache.spark.sql.types.StructType

/**
 * Policy of operating composite catalog, with one CH catalog being either primary or secondary catalog.
 */
trait CompositeCatalogPolicy {
  val primaryCatalog: SessionCatalog
  val secondaryCatalog: SessionCatalog
  val chConcreteCatalog: CHSessionCatalog
}

/**
 * Identical CH catalog policy.
 * @param chContext
 */
case class IdentityPolicy(chContext: CHContext) extends CompositeCatalogPolicy {
  override val primaryCatalog: SessionCatalog = chContext.chConcreteCatalog
  override val secondaryCatalog: SessionCatalog = chContext.chConcreteCatalog
  override val chConcreteCatalog: CHSessionCatalog = chContext.chConcreteCatalog
}

/**
 * Legacy catalog first policy.
 * @param chContext
 */
case class LegacyFirstPolicy(chContext: CHContext) extends CompositeCatalogPolicy {
  override val primaryCatalog: SessionCatalog = chContext.legacyCatalog
  override val secondaryCatalog: SessionCatalog = chContext.chConcreteCatalog
  override val chConcreteCatalog: CHSessionCatalog = chContext.chConcreteCatalog
}

/**
 * A composition of two catalogs that behaves as a concrete catalog.
 * @param chContext
 */
class CHCompositeSessionCatalog(chContext: CHContext)
    extends SessionCatalog(
      chContext.chConcreteCatalog.externalCatalog,
      EmptyFunctionRegistry,
      chContext.sqlContext.conf
    )
    with CompositeCatalogPolicy
    with CHSessionCatalog {

  // TODO: configuration for policy choosing.
  val policy: CompositeCatalogPolicy = LegacyFirstPolicy(chContext)

  override val primaryCatalog: SessionCatalog = policy.primaryCatalog
  override val secondaryCatalog: SessionCatalog = policy.secondaryCatalog
  override val chConcreteCatalog: CHSessionCatalog = policy.chConcreteCatalog

  // Used to manage catalog change by setting current database.
  var currentCatalog: SessionCatalog = primaryCatalog

  // Following are routed to CH catalog.
  override def catalogOf(database: Option[String]): Option[SessionCatalog] =
    database
      .map(db => { Seq(primaryCatalog, secondaryCatalog).find(_.databaseExists(db)) })
      .getOrElse(Some(currentCatalog))

  override def createCHDatabase(databaseDesc: CatalogDatabase, ignoreIfExists: Boolean): Unit =
    chConcreteCatalog.createCHDatabase(databaseDesc, ignoreIfExists)

  override def createCHTable(tableDesc: CatalogTable, ignoreIfExists: Boolean): Unit = {
    if (tableDesc.identifier.database.isEmpty && !currentCatalog.isInstanceOf[CHSessionCatalog]) {
      throw new AnalysisException(
        s"Given table's db is empty and current database '$getCurrentDatabase' is not a Flash database"
      )
    }
    chConcreteCatalog.createCHTable(tableDesc, ignoreIfExists)
  }

  override def createTableFromTiDB(db: String,
                                   tiTableInfo: TiTableInfo,
                                   engine: CHEngine,
                                   ignoreIfExists: Boolean): Unit =
    chConcreteCatalog.createTableFromTiDB(db, tiTableInfo, engine, ignoreIfExists)

  override def loadTableFromTiDB(db: String, tiTable: TiTableInfo, isOverwrite: Boolean): Unit =
    chConcreteCatalog.loadTableFromTiDB(db, tiTable, isOverwrite)

  // Following are handled by composite catalog.
  override def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit =
    catalogOf(Some(db))
      .getOrElse(if (!ignoreIfNotExists) throw new NoSuchDatabaseException(db) else return )
      .dropDatabase(db, ignoreIfNotExists, cascade)

  override def alterDatabase(dbDefinition: CatalogDatabase): Unit =
    catalogOf(Some(dbDefinition.name))
      .getOrElse(throw new NoSuchDatabaseException(dbDefinition.name))
      .alterDatabase(dbDefinition)

  override def getDatabaseMetadata(db: String): CatalogDatabase =
    catalogOf(Some(db))
      .getOrElse(throw new NoSuchDatabaseException(db))
      .getDatabaseMetadata(db)

  override def databaseExists(db: String): Boolean =
    primaryCatalog.databaseExists(db) || secondaryCatalog.databaseExists(db)

  override def listDatabases(): Seq[String] =
    (primaryCatalog.listDatabases() ++ secondaryCatalog.listDatabases()).distinct

  override def listDatabases(pattern: String): Seq[String] =
    (primaryCatalog.listDatabases(pattern) ++ secondaryCatalog.listDatabases(pattern)).distinct

  override def getCurrentDatabase: String = currentCatalog.getCurrentDatabase

  override def setCurrentDatabase(db: String): Unit = {
    currentCatalog = catalogOf(Some(db)).getOrElse(throw new NoSuchDatabaseException(db))
    currentCatalog.setCurrentDatabase(db)
  }

  override def alterTable(tableDefinition: CatalogTable): Unit =
    catalogOf(tableDefinition.identifier.database)
      .getOrElse(
        throw new NoSuchDatabaseException(
          tableDefinition.identifier.database.getOrElse(getCurrentDatabase)
        )
      )
      .alterTable(tableDefinition)

  override def alterTableDataSchema(identifier: TableIdentifier, newDataSchema: StructType): Unit =
    catalogOf(identifier.database)
      .getOrElse(
        throw new NoSuchDatabaseException(identifier.database.getOrElse(getCurrentDatabase))
      )
      .alterTableDataSchema(identifier, newDataSchema)

  override def alterTableStats(identifier: TableIdentifier,
                               newStats: Option[CatalogStatistics]): Unit =
    catalogOf(identifier.database)
      .getOrElse(
        throw new NoSuchDatabaseException(identifier.database.getOrElse(getCurrentDatabase))
      )
      .alterTableStats(identifier, newStats)

  override def tableExists(name: TableIdentifier): Boolean =
    catalogOf(name.database)
      .map {
        // Need to exclude tables from CH's default db.
        case chCatalog: CHSessionCatalog =>
          !name.database.getOrElse(getCurrentDatabase).equals("default") && chCatalog.tableExists(
            name
          )
        case catalog: SessionCatalog => catalog.tableExists(name)
      }
      .getOrElse(throw new NoSuchDatabaseException(name.database.getOrElse(getCurrentDatabase)))

  override def getTableMetadata(name: TableIdentifier): CatalogTable =
    catalogOf(name.database)
      .getOrElse(throw new NoSuchDatabaseException(name.database.getOrElse(getCurrentDatabase)))
      .getTableMetadata(name)

  override def renameTable(oldName: TableIdentifier, newName: TableIdentifier): Unit = {
    val db = formatDatabaseName(oldName.database.getOrElse(getCurrentDatabase))
    newName.database.map(formatDatabaseName).foreach { newDb =>
      if (db != newDb) {
        throw new AnalysisException(
          s"RENAME TABLE source and destination databases do not match: '$db' != '$newDb'"
        )
      }
    }
    catalogOf(oldName.database)
      .getOrElse(throw new NoSuchDatabaseException(oldName.database.getOrElse(getCurrentDatabase)))
      .renameTable(oldName, newName)
  }

  override def dropTable(name: TableIdentifier, ignoreIfNotExists: Boolean, purge: Boolean): Unit =
    catalogOf(name.database)
      .getOrElse(throw new NoSuchDatabaseException(name.database.getOrElse(getCurrentDatabase)))
      .dropTable(name, ignoreIfNotExists, purge)

  override def lookupRelation(name: TableIdentifier): LogicalPlan =
    catalogOf(name.database)
      .getOrElse(throw new NoSuchDatabaseException(name.database.getOrElse(getCurrentDatabase)))
      .lookupRelation(name)

  override def listTables(db: String): Seq[TableIdentifier] =
    catalogOf(Some(db))
      .getOrElse(throw new NoSuchDatabaseException(db))
      .listTables(db)

  override def listTables(db: String, pattern: String): Seq[TableIdentifier] =
    catalogOf(Some(db))
      .getOrElse(throw new NoSuchDatabaseException(db))
      .listTables(db, pattern)

  // Following are all routed to primary catalog.
  override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit =
    primaryCatalog.createDatabase(dbDefinition, ignoreIfExists)

  override def getCachedPlan(t: QualifiedTableName, c: Callable[LogicalPlan]): LogicalPlan =
    primaryCatalog.getCachedPlan(t, c)

  override def getCachedTable(key: QualifiedTableName): LogicalPlan =
    primaryCatalog.getCachedTable(key)

  override def cacheTable(t: QualifiedTableName, l: LogicalPlan): Unit =
    primaryCatalog.cacheTable(t, l)

  override def invalidateCachedTable(key: QualifiedTableName): Unit =
    primaryCatalog.invalidateCachedTable(key)

  override def invalidateAllCachedTables(): Unit = primaryCatalog.invalidateAllCachedTables()

  override def getDefaultDBPath(db: String): URI = primaryCatalog.getDefaultDBPath(db)

  override def createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit =
    primaryCatalog.createTable(tableDefinition, ignoreIfExists)

  override def loadTable(name: TableIdentifier,
                         loadPath: String,
                         isOverwrite: Boolean,
                         isSrcLocal: Boolean): Unit =
    primaryCatalog.loadTable(name, loadPath, isOverwrite, isSrcLocal)

  override def loadPartition(name: TableIdentifier,
                             loadPath: String,
                             spec: TablePartitionSpec,
                             isOverwrite: Boolean,
                             inheritTableSpecs: Boolean,
                             isSrcLocal: Boolean): Unit =
    primaryCatalog.loadPartition(name, loadPath, spec, isOverwrite, inheritTableSpecs, isSrcLocal)

  override def defaultTablePath(tableIdent: TableIdentifier): URI =
    primaryCatalog.defaultTablePath(tableIdent)

  override def createTempView(name: String,
                              tableDefinition: LogicalPlan,
                              overrideIfExists: Boolean): Unit =
    primaryCatalog.createTempView(name, tableDefinition, overrideIfExists)

  override def createGlobalTempView(name: String,
                                    viewDefinition: LogicalPlan,
                                    overrideIfExists: Boolean): Unit =
    primaryCatalog.createGlobalTempView(name, viewDefinition, overrideIfExists)

  override def alterTempViewDefinition(name: TableIdentifier,
                                       viewDefinition: LogicalPlan): Boolean =
    primaryCatalog.alterTempViewDefinition(name, viewDefinition)

  override def getTempView(name: String): Option[LogicalPlan] = primaryCatalog.getTempView(name)

  override def getGlobalTempView(name: String): Option[LogicalPlan] =
    primaryCatalog.getGlobalTempView(name)

  override def dropTempView(name: String): Boolean = primaryCatalog.dropTempView(name)

  override def dropGlobalTempView(name: String): Boolean = primaryCatalog.dropGlobalTempView(name)

  override def getTempViewOrPermanentTableMetadata(name: TableIdentifier): CatalogTable =
    primaryCatalog.getTempViewOrPermanentTableMetadata(name)

  override def isTemporaryTable(name: TableIdentifier): Boolean =
    primaryCatalog.isTemporaryTable(name)

  override def refreshTable(name: TableIdentifier): Unit = primaryCatalog.refreshTable(name)

  override def clearTempTables(): Unit = primaryCatalog.clearTempTables()

  override def createPartitions(tableName: TableIdentifier,
                                parts: Seq[CatalogTablePartition],
                                ignoreIfExists: Boolean): Unit =
    primaryCatalog.createPartitions(tableName, parts, ignoreIfExists)

  override def dropPartitions(tableName: TableIdentifier,
                              specs: Seq[TablePartitionSpec],
                              ignoreIfNotExists: Boolean,
                              purge: Boolean,
                              retainData: Boolean): Unit =
    primaryCatalog.dropPartitions(tableName, specs, ignoreIfNotExists, purge, retainData)

  override def renamePartitions(tableName: TableIdentifier,
                                specs: Seq[TablePartitionSpec],
                                newSpecs: Seq[TablePartitionSpec]): Unit =
    primaryCatalog.renamePartitions(tableName, specs, newSpecs)

  override def alterPartitions(tableName: TableIdentifier,
                               parts: Seq[CatalogTablePartition]): Unit =
    primaryCatalog.alterPartitions(tableName, parts)

  override def getPartition(tableName: TableIdentifier,
                            spec: TablePartitionSpec): CatalogTablePartition =
    primaryCatalog.getPartition(tableName, spec)

  override def listPartitionNames(tableName: TableIdentifier,
                                  partialSpec: Option[TablePartitionSpec]): Seq[String] =
    primaryCatalog.listPartitionNames(tableName, partialSpec)

  override def listPartitions(tableName: TableIdentifier,
                              partialSpec: Option[TablePartitionSpec]): Seq[CatalogTablePartition] =
    primaryCatalog.listPartitions(tableName, partialSpec)

  override def listPartitionsByFilter(tableName: TableIdentifier,
                                      predicates: Seq[Expression]): Seq[CatalogTablePartition] =
    primaryCatalog.listPartitionsByFilter(tableName, predicates)

  override def createFunction(funcDefinition: CatalogFunction, ignoreIfExists: Boolean): Unit =
    primaryCatalog.createFunction(funcDefinition, ignoreIfExists)

  override def dropFunction(name: FunctionIdentifier, ignoreIfNotExists: Boolean): Unit =
    primaryCatalog.dropFunction(name, ignoreIfNotExists)

  override def alterFunction(funcDefinition: CatalogFunction): Unit =
    primaryCatalog.alterFunction(funcDefinition)

  override def getFunctionMetadata(name: FunctionIdentifier): CatalogFunction =
    primaryCatalog.getFunctionMetadata(name)

  override def functionExists(name: FunctionIdentifier): Boolean =
    primaryCatalog.functionExists(name)

  override def loadFunctionResources(resources: Seq[FunctionResource]): Unit =
    primaryCatalog.loadFunctionResources(resources)

  override def registerFunction(funcDefinition: CatalogFunction,
                                overrideIfExists: Boolean,
                                functionBuilder: Option[FunctionBuilder]): Unit =
    primaryCatalog.registerFunction(funcDefinition, overrideIfExists, functionBuilder)

  override def dropTempFunction(name: String, ignoreIfNotExists: Boolean): Unit =
    primaryCatalog.dropTempFunction(name, ignoreIfNotExists)

  override def isTemporaryFunction(name: FunctionIdentifier): Boolean =
    primaryCatalog.isTemporaryFunction(name)

  override def lookupFunctionInfo(name: FunctionIdentifier): ExpressionInfo =
    primaryCatalog.lookupFunctionInfo(name)

  override def lookupFunction(name: FunctionIdentifier, children: Seq[Expression]): Expression =
    primaryCatalog.lookupFunction(name, children)

  override def listFunctions(db: String): Seq[(FunctionIdentifier, String)] =
    primaryCatalog.listFunctions(db)

  override def listFunctions(db: String, pattern: String): Seq[(FunctionIdentifier, String)] =
    primaryCatalog.listFunctions(db, pattern)

  override def reset(): Unit = primaryCatalog.reset()

  override private[sql] def copyStateTo(target: SessionCatalog): Unit =
    primaryCatalog.copyStateTo(target)
}