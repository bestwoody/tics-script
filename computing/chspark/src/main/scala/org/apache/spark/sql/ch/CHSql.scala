/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.ch

import com.pingcap.ch.columns.CHColumnDate
import com.pingcap.ch.datatypes._
import com.pingcap.ch.datatypes.CHTypeNumber.{CHTypeInt32, CHTypeUInt16, CHTypeUInt8, _}
import com.pingcap.theflash.TypeMappingJava
import com.pingcap.tikv.meta.{TiColumnInfo, TiTableInfo}
import com.pingcap.tikv.types.MySQLType
import com.pingcap.tispark.TiUtils
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Abs, Add, Alias, And, AttributeReference, BinaryArithmetic, Cast, Coalesce, CreateNamedStruct, Divide, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, IfNull, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, Multiply, Not, Or, Remainder, Subtract, UnaryMinus}
import org.apache.spark.sql.ch.hack.Hack
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._

/**
 * Compiler that compiles CHLogicalPlan/CHTableRef to CH SQL string.
 * Note that every identifier, i.e. db name, table name, and column name appearing in the query
 * will be lower-cased and back-quoted.
 */
object CHSql {
  type TiDBIntType = com.pingcap.tikv.types.IntegerType
  type TiDBDecimalType = com.pingcap.tikv.types.DecimalType

  def insertStmt(database: String, table: String) =
    s"INSERT INTO ${getBackQuotedAbsTabName(database, table)} VALUES"

  def dropTableStmt(database: String, table: String, ifExists: Boolean = true): String =
    if (ifExists) {
      s"DROP TABLE IF EXISTS ${getBackQuotedAbsTabName(database, table)}"
    } else {
      s"DROP TABLE ${getBackQuotedAbsTabName(database, table)}"
    }

  def createDatabaseStmt(database: String, ifNotExists: Boolean = true): String =
    if (ifNotExists) {
      s"CREATE DATABASE IF NOT EXISTS `${database.toLowerCase()}`"
    } else {
      s"CREATE DATABASE `${database.toLowerCase()}`"
    }

  def dropDatabaseStmt(database: String, ifExists: Boolean): String =
    if (ifExists) {
      s"DROP DATABASE IF EXISTS `${database.toLowerCase()}`"
    } else {
      s"DROP DATABASE `${database.toLowerCase()}`"
    }

  def createTableStmt(database: String,
                      schema: StructType,
                      primaryKeys: Array[String],
                      table: String,
                      partitionNum: Option[Int] = None): String = {
    val schemaStr = compileSchema(schema)
    val pkStr = compilePKList(schema, primaryKeys)
    s"CREATE TABLE ${getBackQuotedAbsTabName(database, table)} ($schemaStr) ENGINE = MutableMergeTree(${partitionNum
      .map(_.toString + ", ")
      .getOrElse("")}($pkStr), 8192)"
  }

  def createTableStmt(database: String, table: TiTableInfo, partitionNum: Option[Int]): String = {
    val schemaStr = compileSchema(table)
    val pkStr = compilePKList(table)
    s"CREATE TABLE ${getBackQuotedAbsTabName(database, table.getName)} ($schemaStr) ENGINE = MutableMergeTree(${partitionNum
      .map(_.toString + ", ")
      .getOrElse("")}($pkStr), 8192)"
  }

  case class Query(private val projection: String,
                   private val table: CHTableRef,
                   private val filter: String,
                   private val aggregation: String,
                   private val topN: String) {
    def buildQuery(partition: String): String =
      buildQueryInternal(CHSql.compileTable(table, partition))

    def buildQuery(): String =
      buildQueryInternal(CHSql.compileTable(table))

    private def buildQueryInternal(from: String): String =
      s"$projection$from$filter$aggregation$topN"

    override def toString: String = buildQuery()
  }

  def tableEngine(table: CHTableRef): String =
    s"SELECT engine FROM system.tables WHERE database = '${table.database}' AND name = '${table.table}'"

  /**
   * Compose a query string based on given input table and CH logical plan.
   * @param table
   * @param chLogicalPlan
   * @param useSelraw
   * @return
   */
  def query(table: CHTableRef, chLogicalPlan: CHLogicalPlan, useSelraw: Boolean = false): Query =
    Query(
      compileProject(chLogicalPlan.chProject, useSelraw),
      table,
      compileFilter(chLogicalPlan.chFilter),
      compileAggregate(chLogicalPlan.chAggregate),
      compileTopN(chLogicalPlan.chTopN)
    )

  /**
   * Query partition list of a table
   * @param table
   * @return
   */
  def partitionList(table: CHTableRef): String =
    s"SELECT DISTINCT(partition) FROM system.parts WHERE database = '${table.database}' AND table = '${table.table}' AND active = 1"

  /**
   * Compose a desc table string.
   * @param table
   * @return
   */
  def desc(table: CHTableRef): String =
    "DESC " + getBackQuotedAbsTabName(table.database, table.table)

  /**
   * Show tables
   * @param database to perform show table
   * @return
   */
  def showTables(database: String): String =
    s"SHOW TABLES FROM `${database.toLowerCase()}`"

  /**
   * Show databases
   * @return
   */
  def showDatabases(): String =
    s"SHOW DATABASES"

  /**
   * Compose a count(*) SQL string.
   * @param table
   * @param useSelraw
   * @return
   */
  def count(table: CHTableRef, useSelraw: Boolean = false): String =
    (if (useSelraw) "SELRAW" else "SELECT") + " COUNT(*) FROM " + getBackQuotedAbsTabName(
      table.database,
      table.table
    )

  private def compileSchema(schema: StructType): String = {
    val sb = new StringBuilder()
    var first = true
    schema.fields foreach { field =>
      val colDef = Hack
        .hackColumnDef(field.name, field.dataType, field.nullable)
        .getOrElse(
          s"`${field.name.toLowerCase()}` ${TypeMappingJava.sparkTypeToCHType(field.dataType, field.nullable).name()}"
        )
      if (first) {
        sb.append(colDef)
      } else {
        sb.append(s", $colDef")
      }
      first = false
    }
    sb.toString()
  }

  private def analyzeColumnDef(column: TiColumnInfo, table: TiTableInfo): String = {
    val dataType = column.getType
    val tp = dataType.getType

    val isUnsigned = dataType match {
      case d: TiDBIntType => d.isUnsigned
      case _              => false
    }

    var chType = tp match {
      case MySQLType.TypeBit   => CHTypeUInt64.instance
      case MySQLType.TypeTiny  => if (isUnsigned) CHTypeUInt8.instance else CHTypeInt8.instance
      case MySQLType.TypeShort => if (isUnsigned) CHTypeUInt16.instance else CHTypeInt16.instance
      case MySQLType.TypeYear  => CHTypeInt16.instance
      case MySQLType.TypeLong | MySQLType.TypeInt24 =>
        if (isUnsigned) CHTypeUInt32.instance else CHTypeInt32.instance
      case MySQLType.TypeFloat  => CHTypeFloat32.instance
      case MySQLType.TypeDouble => CHTypeFloat64.instance
      case MySQLType.TypeNewDecimal | MySQLType.TypeDecimal =>
        val tidbDecimalType = dataType.asInstanceOf[TiDBDecimalType]
        new CHTypeDecimal(tidbDecimalType.getLength.toInt, tidbDecimalType.getDecimal)
      case MySQLType.TypeTimestamp | MySQLType.TypeDatetime => CHTypeDateTime.instance
      case MySQLType.TypeDuration                           => CHTypeInt64.instance
      case MySQLType.TypeLonglong                           => if (isUnsigned) CHTypeUInt64.instance else CHTypeInt64.instance
      case MySQLType.TypeDate | MySQLType.TypeNewDate       => CHTypeDate.instance
      case MySQLType.TypeString | MySQLType.TypeVarchar | MySQLType.TypeTinyBlob |
          MySQLType.TypeMediumBlob | MySQLType.TypeLongBlob | MySQLType.TypeBlob |
          MySQLType.TypeVarString =>
        CHTypeString.instance
      case _ => throw new IllegalArgumentException(s"dataType not supported $dataType")
    }

    if (!column.isPrimaryKey && !dataType.isNotNull) {
      chType = new CHTypeNullable(chType)
    }
    val sparkType = TiUtils.toSparkDataType(dataType)
    val columnName =
      Hack.hackColumnName(column.getName, sparkType).getOrElse(column.getName.toLowerCase())
    s"`$columnName` ${chType.name()}"
  }

  private def compileSchema(table: TiTableInfo): String =
    table.getColumns
      .map(column => analyzeColumnDef(column, table))
      .mkString(",")

  private def compilePKList(table: TiTableInfo): String =
    table.getColumns
      .filter(c => c.isPrimaryKey)
      .map(
        c =>
          s"`${Hack.hackColumnName(c.getName, TiUtils.toSparkDataType(c.getType)).getOrElse(c.getName)}`"
      )
      .mkString(",")

  private def compilePKList(schema: StructType, primaryKeys: Array[String]) =
    primaryKeys
      .map(
        pk =>
          schema
            .collectFirst {
              case field if field.name.equalsIgnoreCase(pk) =>
                s"`${Hack.hackColumnName(field.name, field.dataType).getOrElse(field.name.toLowerCase())}`"
            }
            .getOrElse(throw new IllegalArgumentException(s"column $pk does not exists"))
      )
      .mkString(", ")

  private def compileProject(chProject: CHProject, useSelraw: Boolean): String =
    (if (useSelraw) "SELRAW " else "SELECT ") + chProject.projectList
      .map(compileExpression)
      .mkString(", ")

  private def compileTable(table: CHTableRef, partitions: String = null): String =
    if (partitions == null || partitions.isEmpty) {
      s" FROM ${getBackQuotedAbsTabName(table.database, table.mappedName)}"
    } else {
      s" FROM ${getBackQuotedAbsTabName(table.database, table.mappedName)} PARTITION $partitions"
    }

  private def compileFilter(chFilter: CHFilter): String =
    if (chFilter.predicates.isEmpty) ""
    else " WHERE " + chFilter.predicates.reduceLeftOption(And).map(compileExpression).get

  private def compileAggregate(chAggregate: CHAggregate): String =
    if (chAggregate.groupingExpressions.isEmpty) ""
    else " GROUP BY " + chAggregate.groupingExpressions.map(compileExpression).mkString(", ")

  private def compileTopN(chTopN: CHTopN): String =
    (if (chTopN.sortOrders.isEmpty) ""
     else
       " ORDER BY " + chTopN.sortOrders
         .map(so => {
           so.child match {
             case ns @ CreateNamedStruct(_) =>
               // Spark will compile order by expression `(a + b, a)` to
               // `named_struct("col1", a + b, "a", a)`.
               // Need to emit the expression list enclosed by ().
               throw new UnsupportedOperationException("NamedStruct currently unsupported")
//               s"${ns.valExprs.map(compileExpression).mkString("(", ", ", ")")} ${so.direction.sql} ${so.nullOrdering.sql}"
             case _ => s"${compileExpression(so.child)} ${so.direction.sql} ${so.nullOrdering.sql}"
           }
         })
         .mkString(", ")) + chTopN.n.map(" LIMIT " + _).getOrElse("")

  private def compileAggregateExpression(ae: AggregateExpression): String =
    (ae.aggregateFunction, ae.isDistinct) match {
      case (_, true) =>
        throw new UnsupportedOperationException(
          s"Aggregate Function ${ae.toString} push down is not supported by CHSql."
        )
      case (Count(children), _) => s"COUNT(${children.map(compileExpression).mkString(", ")})"
      case (Min(child), _)      => s"MIN(${compileExpression(child)})"
      case (Max(child), _)      => s"MAX(${compileExpression(child)})"
      case (Sum(child), _) =>
        child.dataType match {
          case DecimalType() =>
            throw new UnsupportedOperationException(
              s"Aggregate Function ${ae.toString} over decimal value is not supported by CHSql."
            )
          case _ =>
        }
        s"SUM(${compileExpression(child)})"
      case (Average(_), _) =>
        throw new UnsupportedOperationException(s"Unexpected ${ae.toString} found when compiling.")
      case _ =>
        throw new UnsupportedOperationException(
          s"Aggregate Function ${ae.toString} push down is not supported by CHSql."
        )
    }

  def compileType(dataType: DataType, nullable: Boolean): String =
    TypeMappingJava.sparkTypeToCHType(dataType, nullable).name()

  /**
   *  For cases like (a + b), due to inconsistent type promotion between Spark
   *  and CH, we might need to wrap as `Cast` explicitly to ensure correctness.
   *
   *  Suppose a is Int8 and b is Int32,
   *  Spark consider a + b as InterType(Int32) while CH consider it as Int64(LongType).
   *  To ensure correctness, results will be forced to cast to Spark's promoted type.
   *  Discussions are present in https://github.com/pingcap/theflash/pull/545#discussion_r211110397
   *
   *  e.g., Spark -> CH expressions would be compiled in following rules.
   *  (a + b)                    -> Cast((`a` + `b`) as Int32)
   *  (a + b) * (a - b)          -> Cast((Cast((`a` + `b`) as Int32) * Cast((`a` - `b`) as Int32)) as Int32)
   *  (a + b) as a               -> Cast((`a` + `b`) as Int32) as `a`
   *  Cast(a + b + c as String)  -> Cast((Cast((Cast((`a` + `b`) as Int32) + `c`) as Int32) as String) as `Cast(a + b + c as String)`
   *  (a + b * (a + b))          -> Cast((`a` + Cast((`b` * Cast((`a` + `b`) as Int32)) as Int32)) as Int32)) as `(a + b * (a + b))`
   *
   * @param arithmetic Binary Arithmetic
   * @return compiled CH Expression
   */
  def compileBinaryArithmetic(arithmetic: BinaryArithmetic): String = {
    val compileArithmetic = arithmetic match {
      case Add(left, right) =>
        s"(${compileExpression(left)} + ${compileExpression(right)})"
      case Subtract(left, right) =>
        s"(${compileExpression(left)} - ${compileExpression(right)})"
      case Multiply(left, right) =>
        s"(${compileExpression(left)} * ${compileExpression(right)})"
      case Divide(left, right) =>
        s"(${compileExpression(left)} / ${compileExpression(right)})"
      case Remainder(left, right) if !right.canonicalized.equals(Literal(0)) =>
        s"(${compileExpression(left)} % ${compileExpression(right)})"
      case _ =>
        throw new UnsupportedOperationException(
          s"Binary Arithmetic Function ${arithmetic.toString} push down is not supported by CHSql."
        )
    }
    s"CAST($compileArithmetic AS ${compileType(arithmetic.dataType, nullable = arithmetic.nullable)})"
  }

  def compileCoalesce(ce: Coalesce, idx: Integer): String =
    if (idx == 1) {
      s"ifNull(${compileExpression(ce.children.head)}, ${compileExpression(ce.children(1))})"
    } else if (idx > 1) {
      s"ifNull(${compileCoalesce(ce, idx - 1)}, ${compileExpression(ce.children(idx))})"
    } else {
      compileExpression(ce.children.head)
    }

  def compileAttributeName(name: String): String = s"`${name.replace("`", "\\`")}`"

  /**
   * Compile Spark Expressions into CH Expressions
   *
   * @param expression Spark expressions to be compiled
   * @return compiled CH expressions
   */
  def compileExpression(expression: Expression): String =
    expression match {
      case Literal(value, dataType) =>
        if (dataType == null || value == null) {
          "NULL"
        } else {
          dataType match {
            case StringType => "'" + escapeString(value.toString) + "'"
            case DateType   =>
              // Date in storage starts from 1400-01-01
              (value.asInstanceOf[java.lang.Integer] - CHColumnDate.DATE_EPOCH_OFFSET).toString
            case TimestampType =>
              // DateTime in storage is stored as seconds rather than milliseconds
              (value.asInstanceOf[java.lang.Long] / 1000000).toString
            case BooleanType =>
              val isTrue = if (value.toString.equalsIgnoreCase("TRUE")) 1 else 0
              s"CAST($isTrue AS UInt8)"
            case _ => value.toString
          }
        }
      case attr: AttributeReference =>
        compileAttributeName(Hack.hackAttributeReference(attr).getOrElse(attr.name).toLowerCase())
      //      case ns @ CreateNamedStruct(_) => ns.valExprs.map(compileExpression).mkString("(", ", ", ")")
      case cast @ Cast(child, dataType) =>
        if (!Hack.hackSupportCast(cast).getOrElse(CHUtil.isSupportedExpression(child))) {
          throw new UnsupportedOperationException(
            s"Shouldn't be casting expression $expression to type $dataType."
          )
        }
        try {
          s"CAST(${compileExpression(child)} AS ${compileType(dataType, nullable = child.nullable)})"
        } catch {
          // Unsupported target type, downgrading to not casting.
          case _: UnsupportedOperationException => s"${compileExpression(child)}"
        }
      case Alias(child, name) => s"${compileExpression(child)} AS ${compileAttributeName(name)}"
      case IsNotNull(child)   => s"${compileExpression(child)} IS NOT NULL"
      case IsNull(child)      => s"${compileExpression(child)} IS NULL"
      case UnaryMinus(child)  => s"(-${compileExpression(child)})"
      case Not(child)         => s"NOT ${compileExpression(child)}"
      case e @ Abs(child) =>
        s"CAST(abs(${compileExpression(child)}) AS ${compileType(e.dataType, nullable = child.nullable)})"
      case be @ BinaryArithmetic(_) => compileBinaryArithmetic(be)
      case GreaterThan(left, right) => s"(${compileExpression(left)} > ${compileExpression(right)})"
      case GreaterThanOrEqual(left, right) =>
        s"(${compileExpression(left)} >= ${compileExpression(right)})"
      case LessThan(left, right) => s"(${compileExpression(left)} < ${compileExpression(right)})"
      case LessThanOrEqual(left, right) =>
        s"(${compileExpression(left)} <= ${compileExpression(right)})"
      case EqualTo(left, right) => s"(${compileExpression(left)} = ${compileExpression(right)})"
      case And(left, right)     => s"(${compileExpression(left)} AND ${compileExpression(right)})"
      case Or(left, right)      => s"(${compileExpression(left)} OR ${compileExpression(right)})"
      case In(value, list) =>
        s"${compileExpression(value)} IN (${list.map(compileExpression).mkString(", ")})"
      case IfNull(left, right, _) =>
        s"ifNull(${compileExpression(left)}, ${compileExpression(right)})"
      case ce @ Coalesce(_) =>
        compileCoalesce(ce, ce.children.length - 1)
      case ae @ AggregateExpression(_, _, _, _) => compileAggregateExpression(ae)
      // TODO: Support more expression types.
      case _ =>
        throw new UnsupportedOperationException(
          s"Expression $expression is not supported by CHSql."
        )
    }

  /**
   * Escape a string to CH string literal.
   * See: http://clickhouse-docs.readthedocs.io/en/latest/query_language/syntax.html
   * @param value
   * @return
   */
  private def escapeString(value: String) =
    value.flatMap {
      case '\\' => "\\\\"
      case '\'' => "\\'"
      case '\b' => "\\b"
      case '\f' => "\\f"
      case '\r' => "\\r"
      case '\n' => "\\n"
      case '\t' => "\\t"
      case '\0' => "\\0"
      case c    => s"$c"
    }

  /**
   * Get the back-quoted absolute name of a table.
   * @param table
   * @return
   */
  private def getBackQuotedAbsTabName(database: String, table: String): String =
    if (database == null || database.isEmpty) s"`${table.toLowerCase()}`"
    else s"`${database.toLowerCase()}`.`${table.toLowerCase()}`"
}
