package org.apache.spark.sql.catalyst.catalog

import java.net.URI
import java.util.concurrent.Callable

import com.pingcap.tikv.meta.TiTableInfo
import org.apache.spark.sql.CHContext
import org.apache.spark.sql.catalyst.{FunctionIdentifier, QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{EmptyFunctionRegistry, NoSuchDatabaseException}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.ch.{CHConfigConst, CHEngine}
import org.apache.spark.sql.types.StructType

/**
 * A composition of two catalogs that behaves as a concrete catalog.
 * It derives Spark's session catalog and overrides the behaviors as below:
 * 1. Methods inherited from trait CHSessionCatalog are directly routed to CH catalog.
 * 2. Methods that need to consider both CH catalog and legacy catalog are implemented using composition logic that
 *    either routes to the corresponding catalog or combine results from both.
 *    And when concerning temp views, use session catalog if needed.
 * 3. The rests are routed to session catalog.
 * @param chContext
 */
class CHCompositeSessionCatalog(val chContext: CHContext)
    extends SessionCatalog(
      chContext.chConcreteCatalog.externalCatalog,
      EmptyFunctionRegistry,
      chContext.sqlContext.conf
    )
    with CHSessionCatalog {

  /**
   * Policy of operating a composition of several catalogs with priorities:
   * 1. A session catalog that operates temp views, which precedence all other catalogs.
   * 2. A primary and a secondary catalog that operate entities other than temp views, primary precedences secondary obviously.
   * 3. A dedicate CH catalog for specialized usages for CH entities.
   */
  trait CHCompositeCatalogPolicy {
    val sessionCatalog: SessionCatalog

    val primaryCatalog: SessionCatalog
    val secondaryCatalog: SessionCatalog

    val chConcreteCatalog: CHSessionCatalog
  }

  /**
   * Legacy catalog first policy.
   * @param chContext
   */
  case class CHLegacyFirstPolicy(chContext: CHContext) extends CHCompositeCatalogPolicy {
    override val sessionCatalog: SessionCatalog = chContext.legacyCatalog
    override val primaryCatalog: SessionCatalog = chContext.legacyCatalog
    override val secondaryCatalog: SessionCatalog = chContext.chConcreteCatalog
    override val chConcreteCatalog: CHSessionCatalog = chContext.chConcreteCatalog
  }

  /**
   * CH catalog first policy.
   * @param chContext
   */
  case class CHFirstPolicy(chContext: CHContext) extends CHCompositeCatalogPolicy {
    override val sessionCatalog: SessionCatalog = chContext.legacyCatalog
    override val primaryCatalog: SessionCatalog = chContext.chConcreteCatalog
    override val secondaryCatalog: SessionCatalog = chContext.legacyCatalog
    override val chConcreteCatalog: CHSessionCatalog = chContext.chConcreteCatalog
  }

  val policy: CHCompositeCatalogPolicy = {
    val catalogPolicy =
      chContext.sqlContext.conf.getConfString(CHConfigConst.CATALOG_POLICY, "legacyfirst")
    if (catalogPolicy.equals("flashfirst"))
      CHFirstPolicy(chContext)
    else if (catalogPolicy.equals("legacyfirst"))
      CHLegacyFirstPolicy(chContext)
    else
      throw new RuntimeException(
        s"Invalid catalog policy: $catalogPolicy, valid options are 'legacyfirst' and 'flashfirst'."
      )
  }

  // Shortcuts.
  val sessionCatalog: SessionCatalog = policy.sessionCatalog
  val primaryCatalog: SessionCatalog = policy.primaryCatalog
  val secondaryCatalog: SessionCatalog = policy.secondaryCatalog
  val chConcreteCatalog: CHSessionCatalog = policy.chConcreteCatalog

  // Used to manage catalog change by setting current database.
  var currentCatalog: SessionCatalog = primaryCatalog

  // Following are routed to CH catalog.

  override def catalogOf(database: Option[String]): Option[SessionCatalog] =
    database
      .map(db => {
        // Global temp db is special, route to session catalog.
        if (db == globalTempDB) {
          Some(sessionCatalog)
        } else {
          Seq(primaryCatalog, secondaryCatalog).find(_.databaseExists(db))
        }
      })
      .getOrElse(Some(currentCatalog))

  override def createFlashDatabase(databaseDesc: CatalogDatabase, ignoreIfExists: Boolean): Unit =
    chConcreteCatalog.createFlashDatabase(databaseDesc, ignoreIfExists)

  override def createFlashTable(tableDesc: CatalogTable,
                                query: Option[LogicalPlan],
                                ignoreIfExists: Boolean): Unit =
    chConcreteCatalog.createFlashTable(tableDesc, query, ignoreIfExists)

  override def createFlashTableFromTiDB(db: String,
                                        tiTableInfo: TiTableInfo,
                                        engine: CHEngine,
                                        ignoreIfExists: Boolean): Unit =
    chConcreteCatalog.createFlashTableFromTiDB(db, tiTableInfo, engine, ignoreIfExists)

  override def loadTableFromTiDB(db: String, tiTable: TiTableInfo, isOverwrite: Boolean): Unit =
    chConcreteCatalog.loadTableFromTiDB(db, tiTable, isOverwrite)

  override def truncateTable(tableIdentifier: TableIdentifier): Unit =
    chConcreteCatalog.truncateTable(tableIdentifier)

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
      .getOrElse(throw new NoSuchDatabaseException(name.database.getOrElse(getCurrentDatabase)))
      .tableExists(name)

  override def getTableMetadata(name: TableIdentifier): CatalogTable =
    catalogOf(name.database)
      .getOrElse(throw new NoSuchDatabaseException(name.database.getOrElse(getCurrentDatabase)))
      .getTableMetadata(name)

  override def getTempViewOrPermanentTableMetadata(name: TableIdentifier): CatalogTable =
    if (isTemporaryTable(name)) {
      sessionCatalog.getTempViewOrPermanentTableMetadata(name)
    } else {
      catalogOf(name.database)
        .getOrElse(throw new NoSuchDatabaseException(name.database.getOrElse(getCurrentDatabase)))
        .getTempViewOrPermanentTableMetadata(name)
    }

  override def renameTable(oldName: TableIdentifier, newName: TableIdentifier): Unit =
    if (isTemporaryTable(oldName)) {
      sessionCatalog.renameTable(oldName, newName)
    } else {
      catalogOf(oldName.database)
        .getOrElse(
          throw new NoSuchDatabaseException(oldName.database.getOrElse(getCurrentDatabase))
        )
        .renameTable(oldName, newName)
    }

  override def dropTable(name: TableIdentifier, ignoreIfNotExists: Boolean, purge: Boolean): Unit =
    if (isTemporaryTable(name)) {
      sessionCatalog.dropTable(name, ignoreIfNotExists, purge)
    } else {
      catalogOf(name.database)
        .getOrElse(throw new NoSuchDatabaseException(name.database.getOrElse(getCurrentDatabase)))
        .dropTable(name, ignoreIfNotExists, purge)
    }

  override def lookupRelation(name: TableIdentifier): LogicalPlan =
    if (isTemporaryTable(name)) {
      sessionCatalog.lookupRelation(name)
    } else {
      catalogOf(name.database)
        .getOrElse(throw new NoSuchDatabaseException(name.database.getOrElse(getCurrentDatabase)))
        .lookupRelation(name)
    }

  /**
   * List tables method is the only special one (so far at least) that must fuse the results from session catalog
   * (to get temp views) and (legacy + CH) catalog.
   * @param db
   * @param pattern
   * @return
   */
  override def listTables(db: String, pattern: String): Seq[TableIdentifier] = {
    val dbName = formatDatabaseName(db)
    catalogOf(Some(dbName))
      .getOrElse(throw new NoSuchDatabaseException(dbName)) match {
      case chCatalog: CHSessionCatalog =>
        // For CH catalog, get temp views (both global and local) in a little bit hacky way:
        // If the passed-in db is global temp db, leverage session catalog, otherwise db might not exist in session catalog, we use current db instead.
        // And finally plus the CH tables.
        val tempViews = if (dbName == globalTempDB) {
          sessionCatalog
            .listTables(dbName, pattern)
        } else {
          sessionCatalog
            .listTables(sessionCatalog.getCurrentDatabase, pattern)
            .filter(sessionCatalog.isTemporaryTable)
        }
        val tables = chCatalog.listTables(db, pattern)
        tables ++ tempViews
      case catalog =>
        // Non-CH catalog's list table already contains temp views.
        catalog.listTables(db, pattern)
    }
  }

  override def refreshTable(name: TableIdentifier): Unit =
    if (isTemporaryTable(name)) {
      sessionCatalog.refreshTable(name)
    } else {
      catalogOf(name.database)
        .getOrElse(throw new NoSuchDatabaseException(name.database.getOrElse(getCurrentDatabase)))
        .refreshTable(name)
    }

  // Following are all routed to session catalog.

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
    sessionCatalog.createTempView(name, tableDefinition, overrideIfExists)

  override def createGlobalTempView(name: String,
                                    viewDefinition: LogicalPlan,
                                    overrideIfExists: Boolean): Unit =
    sessionCatalog.createGlobalTempView(name, viewDefinition, overrideIfExists)

  override def alterTempViewDefinition(name: TableIdentifier,
                                       viewDefinition: LogicalPlan): Boolean =
    sessionCatalog.alterTempViewDefinition(name, viewDefinition)

  override def getTempView(name: String): Option[LogicalPlan] = sessionCatalog.getTempView(name)

  override def getGlobalTempView(name: String): Option[LogicalPlan] =
    sessionCatalog.getGlobalTempView(name)

  override def dropTempView(name: String): Boolean = sessionCatalog.dropTempView(name)

  override def dropGlobalTempView(name: String): Boolean = sessionCatalog.dropGlobalTempView(name)

  override def isTemporaryTable(name: TableIdentifier): Boolean =
    sessionCatalog.isTemporaryTable(name)

  override def clearTempTables(): Unit = sessionCatalog.clearTempTables()

  override def createPartitions(tableName: TableIdentifier,
                                parts: Seq[CatalogTablePartition],
                                ignoreIfExists: Boolean): Unit =
    sessionCatalog.createPartitions(tableName, parts, ignoreIfExists)

  override def dropPartitions(tableName: TableIdentifier,
                              specs: Seq[TablePartitionSpec],
                              ignoreIfNotExists: Boolean,
                              purge: Boolean,
                              retainData: Boolean): Unit =
    sessionCatalog.dropPartitions(tableName, specs, ignoreIfNotExists, purge, retainData)

  override def renamePartitions(tableName: TableIdentifier,
                                specs: Seq[TablePartitionSpec],
                                newSpecs: Seq[TablePartitionSpec]): Unit =
    sessionCatalog.renamePartitions(tableName, specs, newSpecs)

  override def alterPartitions(tableName: TableIdentifier,
                               parts: Seq[CatalogTablePartition]): Unit =
    sessionCatalog.alterPartitions(tableName, parts)

  override def getPartition(tableName: TableIdentifier,
                            spec: TablePartitionSpec): CatalogTablePartition =
    sessionCatalog.getPartition(tableName, spec)

  override def listPartitionNames(tableName: TableIdentifier,
                                  partialSpec: Option[TablePartitionSpec]): Seq[String] =
    sessionCatalog.listPartitionNames(tableName, partialSpec)

  override def listPartitions(tableName: TableIdentifier,
                              partialSpec: Option[TablePartitionSpec]): Seq[CatalogTablePartition] =
    sessionCatalog.listPartitions(tableName, partialSpec)

  override def listPartitionsByFilter(tableName: TableIdentifier,
                                      predicates: Seq[Expression]): Seq[CatalogTablePartition] =
    sessionCatalog.listPartitionsByFilter(tableName, predicates)

  override def createFunction(funcDefinition: CatalogFunction, ignoreIfExists: Boolean): Unit =
    sessionCatalog.createFunction(funcDefinition, ignoreIfExists)

  override def dropFunction(name: FunctionIdentifier, ignoreIfNotExists: Boolean): Unit =
    sessionCatalog.dropFunction(name, ignoreIfNotExists)

  override def alterFunction(funcDefinition: CatalogFunction): Unit =
    sessionCatalog.alterFunction(funcDefinition)

  override def getFunctionMetadata(name: FunctionIdentifier): CatalogFunction =
    sessionCatalog.getFunctionMetadata(name)

  override def functionExists(name: FunctionIdentifier): Boolean =
    sessionCatalog.functionExists(name)

  override def loadFunctionResources(resources: Seq[FunctionResource]): Unit =
    sessionCatalog.loadFunctionResources(resources)

  override def registerFunction(funcDefinition: CatalogFunction,
                                overrideIfExists: Boolean,
                                functionBuilder: Option[FunctionBuilder]): Unit =
    sessionCatalog.registerFunction(funcDefinition, overrideIfExists, functionBuilder)

  override def dropTempFunction(name: String, ignoreIfNotExists: Boolean): Unit =
    sessionCatalog.dropTempFunction(name, ignoreIfNotExists)

  override def isTemporaryFunction(name: FunctionIdentifier): Boolean =
    sessionCatalog.isTemporaryFunction(name)

  override def lookupFunctionInfo(name: FunctionIdentifier): ExpressionInfo =
    sessionCatalog.lookupFunctionInfo(name)

  override def lookupFunction(name: FunctionIdentifier, children: Seq[Expression]): Expression =
    sessionCatalog.lookupFunction(name, children)

  override def listFunctions(db: String): Seq[(FunctionIdentifier, String)] =
    sessionCatalog.listFunctions(db)

  override def listFunctions(db: String, pattern: String): Seq[(FunctionIdentifier, String)] =
    sessionCatalog.listFunctions(db, pattern)

  override def reset(): Unit = sessionCatalog.reset()

  override private[sql] def copyStateTo(target: SessionCatalog): Unit =
    sessionCatalog.copyStateTo(target)
}
