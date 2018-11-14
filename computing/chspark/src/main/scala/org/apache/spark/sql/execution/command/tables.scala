package org.apache.spark.sql.execution.command

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.ch._
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer

case class CreateFlashTableCommand(chContext: CHContext,
                                   tableDesc: CatalogTable,
                                   query: Option[LogicalPlan],
                                   ignoreIfExists: Boolean)
    extends CHCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    chCatalog.createFlashTable(tableDesc, query, ignoreIfExists)

    Seq.empty[Row]
  }
}

case class CreateFlashTableFromTiDBCommand(chContext: CHContext,
                                           tiContext: TiContext,
                                           tiTable: TableIdentifier,
                                           properties: Map[String, String],
                                           ifNotExists: Boolean)
    extends CHCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val ti = tiContext
    val db = tiTable.database.getOrElse(chCatalog.getCurrentDatabase)
    val tiTableInfo = ti.meta.getTable(db, tiTable.table)
    if (tiTableInfo.isEmpty) {
      throw new IllegalArgumentException(s"Table $db.${tiTable.table} not exists")
    }
    val engine = CHEngine.fromTiTableInfo(tiTableInfo.get, properties)
    chCatalog.createFlashTableFromTiDB(db, tiTableInfo.get, engine, ifNotExists)
    Seq.empty[Row]
  }
}

case class LoadDataFromTiDBCommand(chContext: CHContext,
                                   tiTable: TableIdentifier,
                                   isOverwrite: Boolean)
    extends CHCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val ti = chContext.tiContext
    val db = tiTable.database.getOrElse(chCatalog.getCurrentDatabase)
    val tiTableInfo = ti.meta.getTable(db, tiTable.table)
    if (tiTableInfo.isEmpty) {
      throw new IllegalArgumentException(s"Table $db.${tiTable.table} not exists")
    }
    chCatalog.loadTableFromTiDB(db, tiTableInfo.get, isOverwrite)
    Seq.empty[Row]
  }
}

case class CHTruncateTableCommand(chContext: CHContext, delegate: TruncateTableCommand)
    extends CHDelegateCommand(delegate) {
  override def run(sparkSession: SparkSession): Seq[Row] =
    chCatalog
      .catalogOf(delegate.tableName.database)
      .getOrElse(
        throw new NoSuchDatabaseException(
          delegate.tableName.database.getOrElse(chCatalog.getCurrentDatabase)
        )
      ) match {
      case _: CHSessionCatalog =>
        chCatalog.truncateTable(delegate.tableName)
        Seq.empty[Row]
      case _: SessionCatalog => super.run(sparkSession)
    }
}

case class CHDescribeTableCommand(chContext: CHContext, delegate: DescribeTableCommand)
    extends CHDelegateCommand(delegate) {
  override def run(sparkSession: SparkSession): Seq[Row] =
    chCatalog
      .catalogOf(delegate.table.database)
      .getOrElse(
        throw new NoSuchDatabaseException(
          delegate.table.database.getOrElse(chCatalog.getCurrentDatabase)
        )
      ) match {
      case _: CHSessionCatalog =>
        val result = new ArrayBuffer[Row]
        if (delegate.partitionSpec.nonEmpty) {
          throw new AnalysisException(
            s"DESC PARTITION is not allowed on Flash table: ${delegate.table.identifier}"
          )
        }
        val metadata = chCatalog.getTableMetadata(delegate.table)
        describeSchema(metadata.schema, result, header = false)

        if (delegate.isExtended) {
          describeFormattedTableInfo(metadata, result)
        }

        result
      case _: SessionCatalog => super.run(sparkSession)
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
      CHEngine.fromCatalogTable(table).sql,
      ""
    )
    append(
      buffer,
      "PK",
      table.schema
        .filter(
          f =>
            f.metadata.contains(CHCatalogConst.COL_META_PRIMARY_KEY) && f.metadata
              .getBoolean(CHCatalogConst.COL_META_PRIMARY_KEY)
        )
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

case class CHShowTablesCommand(chContext: CHContext, delegate: ShowTablesCommand)
    extends CHDelegateCommand(delegate) {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val db = delegate.databaseName.getOrElse(chCatalog.getCurrentDatabase)
    // Show the information of tables.
    val tables =
      delegate.tableIdentifierPattern
        .map(chCatalog.listTables(db, _))
        .getOrElse(chCatalog.listTables(db))
    tables.map { tableIdent =>
      val database = tableIdent.database.getOrElse("")
      val tableName = tableIdent.table
      val isTemp = chCatalog.isTemporaryTable(tableIdent)
      if (delegate.isExtended) {
        val information = chCatalog.getTempViewOrPermanentTableMetadata(tableIdent).simpleString
        Row(database, tableName, isTemp, s"$information\n")
      } else {
        Row(database, tableName, isTemp)
      }
    }
  }
}

case class CHShowCreateTableCommand(chContext: CHContext, delegate: ShowCreateTableCommand)
    extends CHDelegateCommand(delegate) {
  private def showCreateCHTable(tableMetadata: CatalogTable): String = {
    val builder = StringBuilder.newBuilder
    builder ++= s"CREATE TABLE ${delegate.table.quotedString}"

    val schema = tableMetadata.schema
      .map(col => {
        val pk =
          if (col.metadata.contains(CHCatalogConst.COL_META_PRIMARY_KEY) && col.metadata
                .getBoolean(CHCatalogConst.COL_META_PRIMARY_KEY)) " PRIMARY KEY"
          else ""
        val notNull = if (col.nullable) "" else " NOT NULL"
        s"`${col.name}` ${col.dataType.sql}$pk$notNull"
      })
      .mkString("(", ",", ")")
    builder ++= schema

    val engine = s" USING ${CHEngine.fromCatalogTable(tableMetadata).sql}"
    builder ++= engine

    builder.toString()
  }

  override def run(sparkSession: SparkSession): Seq[Row] =
    chCatalog
      .catalogOf(delegate.table.database)
      .getOrElse(
        throw new NoSuchDatabaseException(
          delegate.table.database.getOrElse(chCatalog.getCurrentDatabase)
        )
      ) match {
      case _: CHSessionCatalog =>
        val tableMetadata = chCatalog.getTableMetadata(delegate.table)
        val stmt = showCreateCHTable(tableMetadata)
        Seq(Row(stmt))
      case _: SessionCatalog => super.run(sparkSession)
    }
}
