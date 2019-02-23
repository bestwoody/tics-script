package org.apache.spark.sql.catalyst.catalog

import com.pingcap.theflash.SparkCHClientInsert
import com.pingcap.tikv.meta.TiTableInfo
import org.apache.spark.sql.{CHContext, Dataset}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{DatabaseAlreadyExistsException, NoSuchDatabaseException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.ch.CHUtil.Partitioner
import org.apache.spark.sql.ch._
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

class CHDirectExternalCatalog(chContext: CHContext) extends CHExternalCatalog {

  // Following are routed to CH catalog.
  override protected def doCreateFlashDatabase(databaseDesc: CatalogDatabase,
                                               ignoreIfExists: Boolean): Unit = {
    if (!ignoreIfExists && databaseExists(databaseDesc.name)) {
      // A decent exception.
      throw new DatabaseAlreadyExistsException(databaseDesc.name)
    }
    CHUtil.createDatabase(databaseDesc.name, chContext.cluster, ignoreIfExists)
  }

  override protected def doCreateFlashTable(tableDesc: CatalogTable,
                                            query: Option[LogicalPlan],
                                            ignoreIfExists: Boolean): Unit = {
    val database = tableDesc.identifier.database.get
    val table = tableDesc.identifier.table
    if (!ignoreIfExists && tableExists(tableDesc.database, tableDesc.identifier.table)) {
      // A decent exception.
      throw new TableAlreadyExistsException(tableDesc.database, tableDesc.identifier.table)
    }
    val engine = CHEngine.fromCatalogTable(tableDesc)
    if (query.nonEmpty) {
      val df = Dataset.ofRows(chContext.sparkSession, query.get)
      val schema = df.schema
      CHUtil.createTable(database, table, schema, engine, ignoreIfExists, chContext.cluster)
      val chRelation = CHRelation(
        CHTableRef
          .ofCluster(
            chContext.cluster,
            tableDesc.identifier.database.get,
            tableDesc.identifier.table
          ),
        chContext.sqlContext.conf
          .getConfString(
            CHConfigConst.PARTITIONS_PER_SPLIT,
            CHConfigConst.DEFAULT_PARTITIONS_PER_SPLIT.toString
          )
          .toInt
      )(chContext.sqlContext, chContext)
      chRelation.insert(Dataset.ofRows(chContext.sparkSession, query.get), true)
    } else {
      CHUtil
        .createTable(database, table, tableDesc.schema, engine, ignoreIfExists, chContext.cluster)
    }
  }

  override protected def doCreateFlashTableFromTiDB(database: String,
                                                    tiTableInfo: TiTableInfo,
                                                    engine: CHEngine,
                                                    ignoreIfExists: Boolean): Unit = {
    if (!ignoreIfExists && tableExists(database, tiTableInfo.getName)) {
      // A decent exception.
      throw new TableAlreadyExistsException(database, tiTableInfo.getName)
    }
    CHUtil.createTable(database, tiTableInfo, engine, ignoreIfExists, chContext.cluster)
  }

  override def loadTableFromTiDB(db: String, tiTable: TiTableInfo, isOverwrite: Boolean): Unit = {
    requireTableExists(db, tiTable.getName)
    val df = chContext.tiContext.getDataFrame(db, tiTable.getName)
    // TODO: sampling.
    val clientBatchSize = chContext.sqlContext.conf
      .getConfString(
        CHConfigConst.CLIENT_BATCH_SIZE,
        SparkCHClientInsert.CLIENT_BATCH_INSERT_COUNT.toString
      )
      .toInt
    val storageBatchRows = chContext.sqlContext.conf
      .getConfString(
        CHConfigConst.STORAGE_BATCH_ROWS,
        SparkCHClientInsert.STORAGE_BATCH_INSERT_COUNT_ROWS.toString
      )
      .toLong
    val storageBatchBytes = chContext.sqlContext.conf
      .getConfString(
        CHConfigConst.STORAGE_BATCH_BYTES,
        SparkCHClientInsert.STORAGE_BATCH_INSERT_COUNT_BYTES.toString
      )
      .toLong
    Partitioner.fromTiTableInfo(tiTable) match {
      case Partitioner(Partitioner.Hash, keyIndex) =>
        CHUtil.insertDataHash(
          df,
          db,
          tiTable.getName,
          keyIndex,
          fromTiDB = true,
          clientBatchSize,
          storageBatchRows,
          storageBatchBytes,
          chContext.cluster
        )
      case Partitioner(Partitioner.Random, _) =>
        CHUtil.insertDataRandom(
          df,
          db,
          tiTable.getName,
          fromTiDB = true,
          clientBatchSize,
          storageBatchRows,
          storageBatchBytes,
          chContext.cluster
        )
    }
  }

  override def truncateTable(tableIdentifier: TableIdentifier): Unit =
    CHUtil.truncateTable(tableIdentifier, chContext.cluster)

  override protected def doDropDatabase(db: String,
                                        ignoreIfNotExists: Boolean,
                                        cascade: Boolean): Unit = {
    if (!ignoreIfNotExists && !databaseExists(db)) {
      // A decent exception.
      throw new NoSuchDatabaseException(db)
    }
    CHUtil.dropDatabase(db, chContext.cluster, ignoreIfNotExists)
  }

  override def databaseExists(db: String): Boolean =
    CHUtil.listDatabases(chContext.cluster.nodes.head).contains(db.toLowerCase())

  override def listDatabases(): Seq[String] = CHUtil.listDatabases(chContext.cluster.nodes.head)

  override def listDatabases(pattern: String): Seq[String] =
    StringUtils.filterPattern(listDatabases(), pattern)

  override protected def doDropTable(db: String,
                                     table: String,
                                     ignoreIfNotExists: Boolean,
                                     purge: Boolean): Unit = {
    if (!ignoreIfNotExists && !tableExists(db, table)) {
      // A decent exception.
      throw new NoSuchTableException(db, table)
    }
    CHUtil.dropTable(db, table, chContext.cluster, ignoreIfNotExists)
  }

  override def getTable(db: String, table: String): CatalogTable = {
    val chTableRef = CHTableRef.ofNode(chContext.cluster.nodes(0), db, table)
    val stmt = CHUtil.getShowCreateTable(chTableRef)
    val engine = CHEngine.fromCreateStatement(stmt)
    val schema = engine.mapFields(CHUtil.getFields(chTableRef))
    val properties = engine.toProperties
    CatalogTable(
      TableIdentifier(table, Some(db)),
      CatalogTableType.EXTERNAL,
      CatalogStorageFormat.empty,
      new StructType(schema),
      properties = properties.toMap
    )
  }

  override def tableExists(db: String, table: String): Boolean =
    CHUtil.listTables(db, chContext.cluster.nodes.head).contains(table.toLowerCase())

  override def listTables(db: String): Seq[String] =
    CHUtil.listTables(db, chContext.cluster.nodes.head)

  override def listTables(db: String, pattern: String): Seq[String] =
    StringUtils.filterPattern(listTables(db), pattern)

  // Following are unimplemented.
  override protected def doCreateDatabase(dbDefinition: CatalogDatabase,
                                          ignoreIfExists: Boolean): Unit = ???

  override protected def doAlterDatabase(dbDefinition: CatalogDatabase): Unit = ???

  override def getDatabase(db: String): CatalogDatabase = ???

  override def setCurrentDatabase(db: String): Unit = ???

  override protected def doCreateTable(tableDefinition: CatalogTable,
                                       ignoreIfExists: Boolean): Unit = ???

  override protected def doRenameTable(db: String, oldName: String, newName: String): Unit = ???

  override protected def doAlterTable(tableDefinition: CatalogTable): Unit = ???

  override protected def doAlterTableDataSchema(db: String,
                                                table: String,
                                                newDataSchema: StructType): Unit = ???

  override protected def doAlterTableStats(db: String,
                                           table: String,
                                           stats: Option[CatalogStatistics]): Unit = ???

  override def loadTable(db: String,
                         table: String,
                         loadPath: String,
                         isOverwrite: Boolean,
                         isSrcLocal: Boolean): Unit = ???

  override def loadPartition(db: String,
                             table: String,
                             loadPath: String,
                             partition: TablePartitionSpec,
                             isOverwrite: Boolean,
                             inheritTableSpecs: Boolean,
                             isSrcLocal: Boolean): Unit = ???

  override def loadDynamicPartitions(db: String,
                                     table: String,
                                     loadPath: String,
                                     partition: TablePartitionSpec,
                                     replace: Boolean,
                                     numDP: Int): Unit = ???

  override def createPartitions(db: String,
                                table: String,
                                parts: Seq[CatalogTablePartition],
                                ignoreIfExists: Boolean): Unit = ???

  override def dropPartitions(db: String,
                              table: String,
                              parts: Seq[TablePartitionSpec],
                              ignoreIfNotExists: Boolean,
                              purge: Boolean,
                              retainData: Boolean): Unit = ???

  override def renamePartitions(db: String,
                                table: String,
                                specs: Seq[TablePartitionSpec],
                                newSpecs: Seq[TablePartitionSpec]): Unit = ???

  override def alterPartitions(db: String, table: String, parts: Seq[CatalogTablePartition]): Unit =
    ???

  override def getPartition(db: String,
                            table: String,
                            spec: TablePartitionSpec): CatalogTablePartition = ???

  override def getPartitionOption(db: String,
                                  table: String,
                                  spec: TablePartitionSpec): Option[CatalogTablePartition] = ???

  override def listPartitionNames(db: String,
                                  table: String,
                                  partialSpec: Option[TablePartitionSpec]): Seq[String] = ???

  override def listPartitions(db: String,
                              table: String,
                              partialSpec: Option[TablePartitionSpec]): Seq[CatalogTablePartition] =
    ???

  override def listPartitionsByFilter(db: String,
                                      table: String,
                                      predicates: Seq[Expression],
                                      defaultTimeZoneId: String): Seq[CatalogTablePartition] = ???

  override protected def doCreateFunction(db: String, funcDefinition: CatalogFunction): Unit = ???

  override protected def doDropFunction(db: String, funcName: String): Unit = ???

  override protected def doAlterFunction(db: String, funcDefinition: CatalogFunction): Unit = ???

  override protected def doRenameFunction(db: String, oldName: String, newName: String): Unit = ???

  override def getFunction(db: String, funcName: String): CatalogFunction = ???

  override def functionExists(db: String, funcName: String): Boolean = ???

  override def listFunctions(db: String, pattern: String): Seq[String] = ???
}
