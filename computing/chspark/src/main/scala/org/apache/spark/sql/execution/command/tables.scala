package org.apache.spark.sql.execution.command

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.ch.{CHEngine, CHUtil, MutableMergeTree}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

case class CHCreateTableCommand(chContext: CHContext,
                                tableDesc: CatalogTable,
                                ignoreIfExists: Boolean)
    extends RunnableCommand
    with CHCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    chCatalog.createCHTable(tableDesc, ignoreIfExists)
    Seq.empty[Row]
  }
}

case class CHCreateTableFromTiDBCommand(chContext: CHContext,
                                        tiTable: TableIdentifier,
                                        properties: Map[String, String],
                                        ifNotExists: Boolean)
    extends RunnableCommand
    with CHCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val ti = new TiContext(sparkSession)
    val db = tiTable.database.getOrElse(chCatalog.getCurrentDatabase)
    val tiTableInfo = ti.meta.getTable(db, tiTable.table)
    if (tiTableInfo.isEmpty) {
      throw new IllegalArgumentException(s"Table $db.${tiTable.table} not exists")
    }
    val engine =
      CHEngine.withNameSafe(properties(CHCatalogConst.TAB_META_ENGINE)) match {
        case CHEngine.MutableMergeTree =>
          val partitionNum =
            properties.get(CHCatalogConst.TAB_META_PARTITION_NUM).map(_.toInt)
          val pkList = tiTableInfo.get.getColumns.filter(_.isPrimaryKey).map(_.getName)
          val bucketNum = properties(CHCatalogConst.TAB_META_BUCKET_NUM).toInt
          MutableMergeTree(partitionNum, pkList, bucketNum)
        case other => throw new RuntimeException(s"Invalid CH engine $other")
      }
    chCatalog.createTableFromTiDB(db, tiTableInfo.get, engine, ifNotExists)
    Seq.empty[Row]
  }
}

case class CHLoadDataFromTiDBCommand(chContext: CHContext,
                                     tiTable: TableIdentifier,
                                     isOverwrite: Boolean)
    extends RunnableCommand
    with CHCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val ti = new TiContext(sparkSession)
    val db = tiTable.database.getOrElse(chCatalog.getCurrentDatabase)
    val tiTableInfo = ti.meta.getTable(db, tiTable.table)
    if (tiTableInfo.isEmpty) {
      throw new IllegalArgumentException(s"Table $db.${tiTable.table} not exists")
    }
    chCatalog.loadTableFromTiDB(db, tiTableInfo.get, isOverwrite)
    Seq.empty[Row]
  }
}

class CHTruncateTableCommand(val chContext: CHContext,
                             tableName: TableIdentifier,
                             partitionSpec: Option[TablePartitionSpec])
    extends TruncateTableCommand(tableName, partitionSpec)
    with CHCommand {
  override def run(sparkSession: SparkSession): Seq[Row] =
    chCatalog
      .catalogOf(tableName.database)
      .getOrElse(
        throw new NoSuchDatabaseException(
          tableName.database.getOrElse(chCatalog.getCurrentDatabase)
        )
      ) match {
      case _: CHSessionCatalog =>
        CHUtil.truncateTable(tableName, chContext.cluster)
        Seq.empty[Row]
      case _: SessionCatalog => super.run(sparkSession)
    }
}

class CHDescribeTableCommand(val chContext: CHContext,
                             table: TableIdentifier,
                             partitionSpec: TablePartitionSpec,
                             isExtended: Boolean)
    extends DescribeTableCommand(table, partitionSpec, isExtended)
    with CHCommand {
  override def run(sparkSession: SparkSession): Seq[Row] =
    chCatalog
      .catalogOf(table.database)
      .getOrElse(
        throw new NoSuchDatabaseException(table.database.getOrElse(chCatalog.getCurrentDatabase))
      ) match {
      case _: CHSessionCatalog =>
        val result = new ArrayBuffer[Row]
        if (partitionSpec.nonEmpty) {
          throw new AnalysisException(
            s"DESC PARTITION is not allowed on Flash table: ${table.identifier}"
          )
        }
        val metadata = chCatalog.getTableMetadata(table)
        describeSchema(metadata.schema, result, header = false)

        if (isExtended) {
          describeFormattedTableInfo(metadata, result)
        }

        result
      case _: SessionCatalog => super[DescribeTableCommand].run(sparkSession)
    }

  private def describeSchema(schema: StructType,
                             buffer: ArrayBuffer[Row],
                             header: Boolean): Unit = {
    if (header) {
      append(buffer, s"# ${output.head.name}", output(1).name, output(2).name)
    }
    schema.foreach { column =>
      append(buffer, column.name, column.dataType.simpleString, column.getComment().orNull)
    }
  }

  private def describeFormattedTableInfo(table: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    // So far we only have engine name and primary key for extended information.
    // TODO: Add more extended table information.
    append(buffer, "", "", "")
    append(buffer, "# Detailed Table Information", "", "")
    append(
      buffer,
      "Engine",
      table.properties.getOrElse(CHCatalogConst.TAB_META_ENGINE, "Unknown"),
      ""
    )
    append(
      buffer,
      "PK",
      table.schema
        .filter(_.metadata.getBoolean(CHCatalogConst.COL_META_PRIMARY_KEY))
        .map(_.name)
        .mkString(","),
      ""
    )
  }

  private def append(buffer: ArrayBuffer[Row],
                     column: String,
                     dataType: String,
                     comment: String): Unit =
    buffer += Row(column, dataType, comment)
}

class CHShowTablesCommand(val chContext: CHContext,
                          databaseName: Option[String],
                          tableIdentifierPattern: Option[String],
                          isExtended: Boolean,
                          partitionSpec: Option[TablePartitionSpec])
    extends ShowTablesCommand(databaseName, tableIdentifierPattern, isExtended, partitionSpec)
    with CHCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val db = databaseName.getOrElse(chCatalog.getCurrentDatabase)
    // Show the information of tables.
    val tables =
      tableIdentifierPattern.map(chCatalog.listTables(db, _)).getOrElse(chCatalog.listTables(db))
    tables.map { tableIdent =>
      val database = tableIdent.database.getOrElse("")
      val tableName = tableIdent.table
      val isTemp = chCatalog.isTemporaryTable(tableIdent)
      if (isExtended) {
        val information = chCatalog.getTempViewOrPermanentTableMetadata(tableIdent).simpleString
        Row(database, tableName, isTemp, s"$information\n")
      } else {
        Row(database, tableName, isTemp)
      }
    }
  }
}
