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

import com.pingcap.ch.datatypes.{CHTypeDateTime, CHTypeNullable, CHTypeString}
import com.pingcap.ch.datatypes.CHTypeNumber.{CHTypeInt32, CHTypeUInt16, CHTypeUInt8, _}
import com.pingcap.theflash.TypeMappingJava
import com.pingcap.tikv.meta.{TiColumnInfo, TiTableInfo}
import com.pingcap.tikv.types.MySQLType
import com.pingcap.tispark.TiUtils
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Abs, Add, And, AttributeReference, Cast, CreateNamedStruct, Divide, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, Multiply, Not, Or, Remainder, Subtract, UnaryMinus}
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

  def insertStmt(database: String, table: String) = s"INSERT INTO ${getBackQuotedAbsTabName(database, table)} VALUES"

  def dropTableStmt(database: String, table: String) : String = {
    s"DROP TABLE IF EXISTS ${getBackQuotedAbsTabName(database, table)}"
  }

  def createDatabaseStmt(database: String) : String = {
    s"CREATE DATABASE `${database.toLowerCase()}`"
  }

  def dropDatabaseStmt(database: String) : String = {
    s"DROP DATABASE IF EXISTS `${database.toLowerCase()}`"
  }

  def createTableStmt(database: String,
                      schema: StructType,
                      primaryKeys: Array[String],
                      table: String): String = {
    val schemaStr = compileSchema(schema)
    val pkStr = compilePKList(schema, primaryKeys)
    s"CREATE TABLE ${getBackQuotedAbsTabName(database, table)} ($schemaStr) ENGINE = MutableMergeTree(($pkStr), 8192)"
  }

  def createTableStmt(database: String,
                      table: TiTableInfo,
                      partitionNum: Int): String = {
    val schemaStr = compileSchema(table)
    val pkStr = compilePKList(table)
    s"CREATE TABLE ${getBackQuotedAbsTabName(database, table.getName)} ($schemaStr) ENGINE = MutableMergeTree($partitionNum, ($pkStr), 8192)"
  }

  case class Query(private val projection: String,
                   private val table: CHTableRef,
                   private val filter: String,
                   private val aggregation: String,
                   private val topN: String) {
    def buildQuery(partition: String): String = {
      buildQueryInternal(CHSql.compileTable(table, partition))
    }

    def buildQuery(): String = {
      buildQueryInternal(CHSql.compileTable(table))
    }

    private def buildQueryInternal(from: String): String = {
      s"$projection$from$filter$aggregation$topN"
    }

    override def toString: String = buildQuery()
  }

  /**
    * Compose a query string based on given input table and CH logical plan.
    * @param table
    * @param chLogicalPlan
    * @param useSelraw
    * @return
    */
  def query(table: CHTableRef, chLogicalPlan: CHLogicalPlan, useSelraw: Boolean = false): Query = {
    Query(compileProject(chLogicalPlan.chProject, useSelraw),
      table,
      compileFilter(chLogicalPlan.chFilter),
      compileAggregate(chLogicalPlan.chAggregate),
      compileTopN(chLogicalPlan.chTopN))
  }

  /**
    * Query partition list of a table
    * @param table
    * @return
    */
  def partitionList(table: CHTableRef): String = {
    s"SELECT DISTINCT(partition) FROM system.parts WHERE database = '${table.database}' AND table = '${table.table}' AND active = 1"
  }

  /**
    * Compose a desc table string.
    * @param table
    * @return
    */
  def desc(table: CHTableRef): String = {
    "DESC " + getBackQuotedAbsTabName(table.database, table.table)
  }

  /**
    * Show tables
    * @param database to perform show table
    * @return
    */
  def showTables(database: String): String = {
    s"SHOW TABLES FROM `${database.toLowerCase()}`"
  }

  /**
    * Show databases
    * @return
    */
  def showDatabases(): String = {
    s"SHOW DATABASES"
  }

  /**
    * Compose a count(*) SQL string.
    * @param table
    * @param useSelraw
    * @return
    */
  def count(table: CHTableRef, useSelraw: Boolean = false): String = {
    (if (useSelraw) "SELRAW" else "SELECT") + " COUNT(*) FROM " + getBackQuotedAbsTabName(table.database, table.table)
  }

  private def compileSchema(schema: StructType): String = {
    val sb = new StringBuilder()
    var first = true
    schema.fields foreach { field =>
      val colDef = Hack.hackColumnDef(field.name, field.dataType, field.nullable).getOrElse(
        s"`${field.name.toLowerCase()}` ${TypeMappingJava.sparkTypeToCHType(field.dataType, field.nullable).name()}")
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
    val dataType = column.getType()
    val tp = dataType.getType()

    val isUnsigned = if (dataType.isInstanceOf[TiDBIntType]) {
      dataType.asInstanceOf[TiDBIntType].isUnsigned
    } else {
      false
    }

    var chType = tp match {
      case MySQLType.TypeBit => CHTypeUInt64.instance
      case MySQLType.TypeTiny => if (isUnsigned) CHTypeUInt8.instance else CHTypeInt8.instance
      case MySQLType.TypeShort => if (isUnsigned) CHTypeUInt16.instance else CHTypeInt16.instance
      case MySQLType.TypeYear => CHTypeInt16.instance
      case MySQLType.TypeLong | MySQLType.TypeInt24 => if (isUnsigned) CHTypeUInt32.instance else CHTypeInt32.instance
      case MySQLType.TypeFloat => CHTypeFloat32.instance
      case MySQLType.TypeDouble | MySQLType.TypeNewDecimal | MySQLType.TypeDecimal => {
        // TODO: Remove Decimal Hack
        if (dataType.isInstanceOf[TiDBDecimalType] &&
            dataType.asInstanceOf[TiDBDecimalType].getDecimal() == 0) {
          CHTypeString.instance
        } else {
          CHTypeFloat64.instance
        }
      }
      case MySQLType.TypeTimestamp | MySQLType.TypeDatetime => CHTypeDateTime.instance
      case MySQLType.TypeDuration => CHTypeInt64.instance
      case MySQLType.TypeLonglong => if (isUnsigned) CHTypeUInt64.instance else CHTypeInt64.instance
      case MySQLType.TypeDate | MySQLType.TypeNewDate => CHTypeInt32.instance
      case MySQLType.TypeString | MySQLType.TypeVarchar | MySQLType.TypeTinyBlob
           | MySQLType.TypeMediumBlob | MySQLType.TypeLongBlob
           | MySQLType.TypeBlob | MySQLType.TypeVarString => CHTypeString.instance
      case _ => throw new IllegalArgumentException(s"dataType not supported $dataType")
    }

    if (!column.isPrimaryKey() && !dataType.isNotNull) {
      chType = new CHTypeNullable(chType)
    }
    val sparkType = TiUtils.toSparkDataType(dataType)
    val columnName = Hack.hackColumnName(column.getName(), sparkType).getOrElse(column.getName.toLowerCase())
    s"`$columnName` ${chType.name()}"
  }

  private def compileSchema(table: TiTableInfo): String = {
    table.getColumns()
      .map(column => analyzeColumnDef(column, table))
      .mkString(",")
  }

  private def compilePKList(table: TiTableInfo): String = {
    table.getColumns
      .filter(c => c.isPrimaryKey)
      .map(c => s"`${Hack.hackColumnName(c.getName, TiUtils.toSparkDataType(c.getType)).getOrElse(c.getName)}`")
      .mkString(",")
  }

  private def compilePKList(schema: StructType, primaryKeys: Array[String]) =
    primaryKeys.map(pk => schema.collectFirst {
      case field if field.name.equalsIgnoreCase(pk) => s"`${Hack.hackColumnName(field.name, field.dataType).getOrElse(field.name.toLowerCase())}`"
    }.getOrElse(throw new IllegalArgumentException(s"column $pk does not exists"))).mkString(", ")

  private def compileProject(chProject: CHProject, useSelraw: Boolean): String = {
    (if (useSelraw) "SELRAW " else "SELECT ") + chProject.projectList.map(compileExpression)
      .mkString(", ")
  }

  private def compileTable(table: CHTableRef, partitions: String = null): String = {
    if (partitions == null || partitions.isEmpty) {
      s" FROM ${getBackQuotedAbsTabName(table.database, table.mappedName)}"
    } else {
      s" FROM ${getBackQuotedAbsTabName(table.database, table.mappedName)} PARTITION $partitions"
    }
  }

  private def compileFilter(chFilter: CHFilter): String = {
    if (chFilter.predicates.isEmpty) ""
    else " WHERE " + chFilter.predicates.reduceLeftOption(And).map(compileExpression).get
  }

  private def compileAggregate(chAggregate: CHAggregate): String = {
    if (chAggregate.groupingExpressions.isEmpty) ""
    else " GROUP BY " + chAggregate.groupingExpressions.map(compileExpression).mkString(", ")
  }

  private def compileTopN(chTopN: CHTopN): String = {
    (if (chTopN.sortOrders.isEmpty) "" else " ORDER BY " + chTopN.sortOrders.map(so => {
      so.child match {
        case ns@CreateNamedStruct(_) =>
          // Spark will compile order by expression `(a + b, a)` to
          // `named_struct("col1", a + b, "a", a)`.
          // Need to emit the expression list enclosed by ().
          ns.valExprs.map(compileExpression).mkString("(", ", ", ") ") + so.direction.sql
        case _ => compileExpression(so.child) + " " + so.direction.sql
      }}).mkString(", ")) + chTopN.n.map(" LIMIT " + _).getOrElse("")
  }

  private def compileAggregateExpression(ae: AggregateExpression): String = {
    (ae.aggregateFunction, ae.isDistinct) match {
      case (Average(child), false) => s"AVG(${compileExpression(child)})"
      case (Count(children), false) => s"COUNT(${children.map(compileExpression).mkString(", ")})"
      case (Min(child), false) => s"MIN(${compileExpression(child)})"
      case (Max(child), false) => s"MAX(${compileExpression(child)})"
      case (Sum(child), false) => s"SUM(${compileExpression(child)})"
      case _ => throw new UnsupportedOperationException(s"Aggregate Function ${ae.toString} is not supported by CHSql.")
    }
  }

  def compileExpression(expression: Expression): String = {
    expression match {
      case Literal(value, dataType) =>
        if (dataType == null || value == null) {
          "NULL"
        } else {
          dataType match {
            case StringType => "'" + escapeString(value.toString) + "'"
            case BooleanType =>
              val isTrue = if (value.toString.equalsIgnoreCase("TRUE")) 1 else 0
              s"CAST($isTrue AS UInt8)"
            case _ => value.toString
          }
        }
      case attr: AttributeReference => s"`${Hack.hackAttributeReference(attr).toLowerCase()}`"
      case ns @ CreateNamedStruct(_) => ns.valExprs.map(compileExpression).mkString("(", ", ", ")")
      case cast @ Cast(child, dataType) =>
        if (!Hack.hackSupportCast(cast).getOrElse(CHUtil.isSupportedExpression(child))) {
          throw new UnsupportedOperationException(s"Shouldn't be casting expression $expression to type $dataType.")
        }
        try {
          val dataTypeName = TypeMappingJava.sparkTypeToCHType(dataType, child.nullable).name()
          s"CAST(${compileExpression(child)} AS $dataTypeName)"
        } catch {
          // Unsupported target type, downgrading to not casting.
          case _: UnsupportedOperationException => s"${compileExpression(child)}"
        }
      case IsNotNull(child) => s"${compileExpression(child)} IS NOT NULL"
      case IsNull(child) => s"${compileExpression(child)} IS NULL"
      case UnaryMinus(child) => s"(-${compileExpression(child)})"
      case Not(child) => s"NOT ${compileExpression(child)}"
      case Abs(child) => s"ABS(${compileExpression(child)})"
      case Add(left, right) => s"(${compileExpression(left)} + ${compileExpression(right)})"
      case Subtract(left, right) => s"(${compileExpression(left)} - ${compileExpression(right)})"
      case Multiply(left, right) => s"(${compileExpression(left)} * ${compileExpression(right)})"
      case Divide(left, right) => s"(${compileExpression(left)} / ${compileExpression(right)})"
      case Remainder(left, right) => s"(${compileExpression(left)} % ${compileExpression(right)})"
      case GreaterThan(left, right) => s"(${compileExpression(left)} > ${compileExpression(right)})"
      case GreaterThanOrEqual(left, right) => s"(${compileExpression(left)} >= ${compileExpression(right)})"
      case LessThan(left, right) => s"(${compileExpression(left)} < ${compileExpression(right)})"
      case LessThanOrEqual(left, right) => s"(${compileExpression(left)} <= ${compileExpression(right)})"
      case EqualTo(left, right) => s"(${compileExpression(left)} = ${compileExpression(right)})"
      case And(left, right) => s"(${compileExpression(left)} AND ${compileExpression(right)})"
      case Or(left, right) => s"(${compileExpression(left)} OR ${compileExpression(right)})"
      case In(value, list) => s"${compileExpression(value)} IN (${list.map(compileExpression).mkString(", ")})"
      case ae @ AggregateExpression(_, _, _, _) => compileAggregateExpression(ae)
      case Sum(child) => s"SUM(${compileExpression(child)})"
      // TODO: Support more expression types.
      case _ => throw new UnsupportedOperationException(s"Expression $expression is not supported by CHSql.")
    }
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
      case c => s"$c"
    }

  /**
    * Get the back-quoted absolute name of a table.
    * @param table
    * @return
    */
  private def getBackQuotedAbsTabName(database: String, table: String): String =
    if (database == null || database.isEmpty) s"`${table.toLowerCase()}`" else s"`${database.toLowerCase()}`.`${table.toLowerCase()}`"
}
