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
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Abs, Add, Alias, And, AttributeReference, BinaryArithmetic, CaseWhen, Cast, Coalesce, CreateNamedStruct, Divide, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, IfNull, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, Multiply, Not, Or, Remainder, StringLPad, StringRPad, StringTrim, StringTrimLeft, StringTrimRight, Subtract, UnaryMinus}
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
                      table: String,
                      schema: StructType,
                      chEngine: CHEngine,
                      ifNotExists: Boolean): String = {
    val schemaStr = compileSchema(schema)
    val engine = compileEngine(chEngine)
    s"CREATE TABLE ${if (ifNotExists) "IF NOT EXISTS " else ""}${getBackQuotedAbsTabName(database, table)} ($schemaStr) ENGINE = $engine"
  }

  def createTableStmt(database: String,
                      table: TiTableInfo,
                      chEngine: CHEngine,
                      ifNotExists: Boolean): String = {
    val schemaStr = compileSchema(table)
    val engine = compileEngine(chEngine)
    s"CREATE TABLE ${if (ifNotExists) "IF NOT EXISTS " else ""}${getBackQuotedAbsTabName(database, table.getName)} ($schemaStr) ENGINE = $engine"
  }

  def truncateTable(database: String, table: String) =
    s"TRUNCATE TABLE ${getBackQuotedAbsTabName(database, table)}"

  def tableEngine(table: CHTableRef): String =
    s"SELECT engine FROM system.tables WHERE database = '${table.database}' AND name = '${table.table}'"

  /**
   * Compose a query string based on given input table and CH logical plan.
   * @param table
   * @param chLogicalPlan
   * @param partitions
   * @param useSelraw
   * @return
   */
  def query(table: CHTableRef,
            chLogicalPlan: CHLogicalPlan,
            partitions: Array[String] = null,
            useSelraw: Boolean = false): String =
    s"${compileProject(chLogicalPlan.chProject, useSelraw)}${compileTable(table, partitions)}${compileFilter(
      chLogicalPlan.chFilter
    )}${compileAggregate(chLogicalPlan.chAggregate)}${compileTopN(chLogicalPlan.chTopN)}"

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
   * Compose a show create table string.
   * @param table
   * @return
   */
  def showCreateTable(table: CHTableRef): String =
    s"SHOW CREATE TABLE `${table.database}`.`${table.table}`"

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
      val colDef =
        s"`${compileStructField(field)}` ${TypeMappingJava.sparkTypeToCHType(field.dataType, field.nullable).name()}"
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
    val columnName = column.getName.toLowerCase()
    s"`$columnName` ${chType.name()}"
  }

  private def compileSchema(table: TiTableInfo): String =
    table.getColumns
      .map(column => analyzeColumnDef(column, table))
      .mkString(",")

  private def compileEngine(chEngine: CHEngine): String =
    chEngine match {
      case mmt: MutableMergeTreeEngine =>
        compileMutableMergeTree(mmt)
      case tmt: TxnMergeTreeEngine =>
        compileTxnMergeTree(tmt)
      case logEngine: LogEngine => compileLogEngine(logEngine)
      case _                    => throw new UnsupportedOperationException(s"Engine ${chEngine.name}")
    }

  private def compileLogEngine(logEngine: LogEngine): String = "Log"

  private def compileMutableMergeTree(mmt: MutableMergeTreeEngine): String =
    s"${mmt.name}(${mmt.partitionNum
      .map(_.toString + ", ")
      .getOrElse("")}${mmt.pkList.map("`" + _.toLowerCase() + "`").mkString("(", ",", ")")}, ${mmt.bucketNum})"

  private def compileTxnMergeTree(tmt: TxnMergeTreeEngine): String =
    s"${tmt.name}(${tmt.partitionNum
      .map(_.toString + ", ")
      .getOrElse("")}${tmt.pkList.map("`" + _.toLowerCase() + "`").mkString("(", ",", ")")}, ${tmt.bucketNum}), '${tmt.tableInfo}'"

  private def compilePKList(table: TiTableInfo): String =
    table.getColumns
      .filter(c => c.isPrimaryKey)
      .map(
        c => s"`${c.getName}`"
      )
      .mkString(",")

  private def compilePKList(schema: StructType, primaryKeys: Array[String]) =
    primaryKeys
      .map(
        pk =>
          schema
            .collectFirst {
              case field if field.name.equalsIgnoreCase(pk) =>
                s"`${compileStructField(field)}`"
            }
            .getOrElse(throw new IllegalArgumentException(s"column $pk does not exists"))
      )
      .mkString(", ")

  private def compileProject(chProject: CHProject, useSelraw: Boolean): String =
    (if (useSelraw) "SELRAW " else "SELECT ") + chProject.projectList
      .map(compileExpression)
      .mkString(", ")

  private def compileTable(table: CHTableRef, partitions: Array[String] = null): String =
    if (partitions == null || partitions.isEmpty) {
      s" FROM ${getBackQuotedAbsTabName(table.database, table.mappedName)}"
    } else {
      s" FROM ${getBackQuotedAbsTabName(table.database, table.mappedName)} PARTITION (${partitions
        .map(part => s"'$part'")
        .mkString(",")})"
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

  def compileCaseWhenExpression(ce: CaseWhen): String = {
    val caseBranch = ce.branches
      .map {
        case (condition, returnVal) =>
          s"WHEN ${compileExpression(condition)} THEN ${compileExpression(returnVal)}"
      }
      .mkString(" ")
    // Spark treat else branch in CASE WHEN statements without else as "ELSE null"
    val elseBranch = ce.elseValue.map(x => s"ELSE ${compileExpression(x)}").getOrElse("ELSE null")
    s"CASE $caseBranch $elseBranch END"
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
        compileAttributeName(attr.name.toLowerCase())
      // case ns @ CreateNamedStruct(_) => ns.valExprs.map(compileExpression).mkString("(", ", ", ")")
      case Cast(child, dataType, _) =>
        if (!CHUtil.isSupportedExpression(child)) {
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
      case be: BinaryArithmetic     => compileBinaryArithmetic(be)
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
      case ce: CaseWhen =>
        compileCaseWhenExpression(ce)
      case IfNull(left, right, _) =>
        s"ifNull(${compileExpression(left)}, ${compileExpression(right)})"
      case ce: Coalesce =>
        compileCoalesce(ce, ce.children.length - 1)
      case ae: AggregateExpression => compileAggregateExpression(ae)
      case StringTrim(src, trimStr) =>
        s"trim(${compileExpression(src)}${trimStr.map(t => s",  ${compileExpression(t)}").getOrElse("")})"
      case StringTrimLeft(src, trimStr) =>
        s"ltrim(${compileExpression(src)}${trimStr.map(t => s",  ${compileExpression(t)}").getOrElse("")})"
      case StringTrimRight(src, trimStr) =>
        s"rtrim(${compileExpression(src)}${trimStr.map(t => s",  ${compileExpression(t)}").getOrElse("")})"
      case StringLPad(str, len, pad) =>
        s"lpad(${compileExpression(str)}, ${compileExpression(len)}, ${compileExpression(pad)})"
      case StringRPad(str, len, pad) =>
        s"rpad(${compileExpression(str)}, ${compileExpression(len)}, ${compileExpression(pad)})"
      // TODO: Support more expression types.
      case _ =>
        throw new UnsupportedOperationException(
          s"Expression $expression is not supported by CHSql."
        )
    }

  private def compileStructField(field: StructField): String =
    field.name.toLowerCase()

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
