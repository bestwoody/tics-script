package org.apache.spark.sql.catalyst.parser

import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.ParseCancellationException
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.ch.{CHEngine, LogEngine, MutableMergeTreeEngine, TxnMergeTreeEngine}
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
        val (table, temp, ifNotExists, _) = visitCreateTableHeader(ctx.createTableHeader)
        if (temp) {
          operationNotAllowed("CREATE TEMPORARY TABLE for Flash", ctx)
        }

        val schema = Option(ctx.colTypeList()).map(createSchema)
        val properties = visitChEngine(tableProvider().chTableProvider().chEngine())

        if (ctx.query != null) {
          // Get the backing query.
          val query = plan(ctx.query)
          val tableDesc = CatalogTable(
            table,
            CatalogTableType.EXTERNAL,
            CatalogStorageFormat.empty,
            schema = schema.getOrElse(new StructType),
            provider = Some("flash"),
            properties = properties
          )

          // Don't allow explicit specification of schema for CTAS
          if (schema.nonEmpty) {
            operationNotAllowed(
              "Schema may not be specified in a Create Table As Select (CTAS) statement",
              ctx
            )
          }
          CreateFlashTable(tableDesc, Some(query), ifNotExists)
        } else {
          if (schema.isEmpty) {
            operationNotAllowed("Schema is not defined in Create Table", ctx)
          }
          val pkList = schema.get.filter(
            f =>
              f.metadata.contains(CHCatalogConst.COL_META_PRIMARY_KEY) && f.metadata
                .getBoolean(CHCatalogConst.COL_META_PRIMARY_KEY)
          )
          if (pkList.isEmpty) {
            operationNotAllowed("Flash mutable table with no primary key", ctx)
          }
          val tableDesc = CatalogTable(
            table,
            CatalogTableType.EXTERNAL,
            CatalogStorageFormat.empty,
            schema = schema.getOrElse(new StructType),
            provider = Some("flash"),
            properties = properties
          )
          CreateFlashTable(tableDesc, None, ifNotExists)
        }
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

      val engine =
        if (mmtEngine() != null) {
          visitMmtEngine(mmtEngine())
        } else if (tmtEngine() != null) {
          visitTmtEngine(tmtEngine())
        } else if (logEngine() != null) {
          visitLogEngine(logEngine())
        } else {
          operationNotAllowed("Flash table storage engine is not specified", ctx)
        }

      engine.toProperties.toMap
    }

  override def visitLogEngine(ctx: SqlBaseParser.LogEngineContext): LogEngine =
    withOrigin(ctx) {
      LogEngine()
    }

  override def visitMmtEngine(ctx: SqlBaseParser.MmtEngineContext): MutableMergeTreeEngine =
    withOrigin(ctx) {
      import ctx._

      // Partition number could be empty, respecting not specifying partition number in CH create table statement.
      val pn = if (partitionNum != null) Some(partitionNum.getText.toInt) else None
      val bn = if (bucketNum != null) bucketNum.getText.toInt else CHCatalogConst.DEFAULT_BUCKET_NUM
      MutableMergeTreeEngine(pn, Seq.empty[String], bn)
    }

  override def visitTmtEngine(ctx: SqlBaseParser.TmtEngineContext): TxnMergeTreeEngine =
    withOrigin(ctx) {
      import ctx._

      // Partition number could be empty, respecting not specifying partition number in CH create table statement.
      val pn = if (partitionNum != null) Some(partitionNum.getText.toInt) else None
      val bn = if (bucketNum != null) bucketNum.getText.toInt else CHCatalogConst.DEFAULT_BUCKET_NUM
      val ti = tableInfo.getText
      TxnMergeTreeEngine(pn, Seq.empty[String], bn, ti)
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

    if (isPk) {
      builder.putBoolean(CHCatalogConst.COL_META_PRIMARY_KEY, true)
    }

    StructField(identifier.getText, cleanedDataType, nullable, builder.build())
  }
}
