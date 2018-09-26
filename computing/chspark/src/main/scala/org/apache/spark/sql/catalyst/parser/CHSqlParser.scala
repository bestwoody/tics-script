package org.apache.spark.sql.catalyst.parser

import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.ParseCancellationException
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.ch.CHEngine
import org.apache.spark.sql.execution.{SparkSqlAstBuilder, SparkSqlParser}
import org.apache.spark.sql.extensions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import scala.collection.mutable

class CHSqlParser(conf: SQLConf) extends SparkSqlParser(conf) {
  override val astBuilder: SparkSqlAstBuilder = new CHSqlAstBuilder(conf)

  override def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    logDebug(s"Parsing command: $command")

    val lexer = new SqlBaseLexer(new UpperCaseCharStream(CharStreams.fromString(command)))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new SqlBaseParser(tokenStream)
    parser.addParseListener(PostProcessor)
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)

    try {
      try {
        // first, try parsing with potentially faster SLL mode
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        toResult(parser)
      } catch {
        case e: ParseCancellationException =>
          // if we fail, parse with LL mode
          tokenStream.seek(0) // rewind input stream
          parser.reset()

          // Try Again.
          parser.getInterpreter.setPredictionMode(PredictionMode.LL)
          toResult(parser)
      }
    } catch {
      case e: ParseException if e.command.isDefined =>
        throw e
      case e: ParseException =>
        throw e.withCommand(command)
      case e: AnalysisException =>
        val position = Origin(e.line, e.startPosition)
        throw new ParseException(Option(command), e.message, position, position)
    }
  }
}

class CHSqlAstBuilder(conf: SQLConf) extends SparkSqlAstBuilder(conf) with SqlBaseVisitor[AnyRef] {
  import ParserUtils._

  override def visitCreateDatabase(ctx: SqlBaseParser.CreateDatabaseContext): LogicalPlan =
    withOrigin(ctx) {
      if (ctx.FLASH() != null) {
        CreateFlashDatabase(ctx.identifier.getText, ctx.EXISTS != null)
      } else {
        super.visitCreateDatabase(ctx)
      }
    }

  override def visitCreateTable(ctx: SqlBaseParser.CreateTableContext): LogicalPlan =
    withOrigin(ctx) {
      import ctx._

      if (tableProvider() != null && tableProvider().chTableProvider() != null) {
        val (table, temp, ifNotExists, external) = visitCreateTableHeader(ctx.createTableHeader)
        if (temp) {
          operationNotAllowed("CREATE TEMPORARY TABLE for Flash", ctx)
        }
        if (ctx.query != null) {
          operationNotAllowed("CTAS for Flash", ctx)
        }
        val schema = Option(ctx.colTypeList()).map(createSchema)
        if (schema.isEmpty) {
          operationNotAllowed("Empty schema", ctx)
        }
        val pkList = schema.get.filter(_.metadata.getBoolean(CHCatalogConst.COL_META_PRIMARY_KEY))
        if (pkList.isEmpty) {
          operationNotAllowed("Flash table with no primary key", ctx)
        }
        val properties = visitChEngine(tableProvider().chTableProvider().chEngine())

        val tableDesc = CatalogTable(
          table,
          CatalogTableType.EXTERNAL,
          CatalogStorageFormat.empty,
          schema.get,
          Some("flash"),
          properties = properties
        )

        CreateFlashTable(tableDesc, ifNotExists)
      } else {
        super.visitCreateTable(ctx)
      }
    }

  override def visitCreateTableFromTiDB(
    ctx: SqlBaseParser.CreateTableFromTiDBContext
  ): LogicalPlan =
    withOrigin(ctx) {
      import ctx._

      if (tableProvider() == null || tableProvider().chTableProvider() == null) {
        operationNotAllowed("No engine specified for Flash table", ctx)
      }

      val tiTable = visitTableIdentifier(ctx.source)
      val properties = visitChEngine(tableProvider().chTableProvider().chEngine())

      CreateFlashTableFromTiDB(tiTable, properties, ctx.EXISTS != null)
    }

  override def visitLoadDataFromTiDB(ctx: SqlBaseParser.LoadDataFromTiDBContext): LogicalPlan =
    withOrigin(ctx) {
      LoadDataFromTiDB(
        visitTableIdentifier(ctx.tableIdentifier),
        isOverwrite = ctx.OVERWRITE != null
      )
    }

  override def visitChEngine(ctx: SqlBaseParser.ChEngineContext): Map[String, String] =
    withOrigin(ctx) {
      import ctx._

      if (mmtEngine() == null) {
        operationNotAllowed("Flash table using storage format other than MMT", ctx)
      }

      val properties = mutable.Map[String, String]()

      properties += (CHCatalogConst.TAB_META_ENGINE -> CHEngine.MutableMergeTree.toString)
      // Partition number could be empty, respecting not specifying partition number in CH create table statement.
      if (partitionNum != null) {
        properties += (CHCatalogConst.TAB_META_PARTITION_NUM -> partitionNum.getText)
      }
      if (bucketNum != null) {
        properties += (CHCatalogConst.TAB_META_BUCKET_NUM -> bucketNum.getText)
      } else {
        properties += (CHCatalogConst.TAB_META_BUCKET_NUM -> CHCatalogConst.DEFAULT_BUCKET_NUM.toString)
      }

      properties.toMap
    }

  override def visitPrimaryKey(ctx: SqlBaseParser.PrimaryKeyContext): AnyRef = ???

  override def visitNotNull(ctx: SqlBaseParser.NotNullContext): AnyRef = ???

  override def visitColType(ctx: SqlBaseParser.ColTypeContext): StructField = withOrigin(ctx) {
    import ctx._

    val builder = new MetadataBuilder
    // Add comment to metadata
    if (STRING != null) {
      builder.putString("comment", string(STRING))
    }
    // Add Hive type string to metadata.
    val rawDataType = typedVisit[DataType](ctx.dataType)
    val cleanedDataType = HiveStringType.replaceCharType(rawDataType)
    if (rawDataType != cleanedDataType) {
      builder.putString(HIVE_TYPE_STRING, rawDataType.catalogString)
    }
    var (nullable, isPk) = (true, false)
    // Add not null to metadata.
    if (notNull() != null) {
      nullable = false
    }
    // Add primary key to metadata.
    if (primaryKey() != null) {
      nullable = false
      isPk = true
    }

    builder.putBoolean(CHCatalogConst.COL_META_PRIMARY_KEY, isPk)

    StructField(identifier.getText, cleanedDataType, nullable, builder.build())
  }
}
