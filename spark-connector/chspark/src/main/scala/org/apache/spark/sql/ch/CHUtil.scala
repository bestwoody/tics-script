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

import scala.util.Random

import org.apache.arrow.vector.VectorSchemaRoot

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.apache.spark.sql.types.{DoubleType, FloatType}
import org.apache.spark.sql.types.{ByteType, IntegerType, LongType, ShortType}

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.IsNotNull
import org.apache.spark.sql.catalyst.expressions.Add
import org.apache.spark.sql.catalyst.expressions.Subtract
import org.apache.spark.sql.catalyst.expressions.Multiply
import org.apache.spark.sql.catalyst.expressions.Divide
import org.apache.spark.sql.catalyst.expressions.Remainder
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.GreaterThan
import org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual
import org.apache.spark.sql.catalyst.expressions.LessThan
import org.apache.spark.sql.catalyst.expressions.LessThanOrEqual
import org.apache.spark.sql.catalyst.expressions.EqualTo
import org.apache.spark.sql.catalyst.expressions.Not
import org.apache.spark.sql.catalyst.expressions.Cast

import org.apache.spark.sql.catalyst.expressions.aggregate.Min
import org.apache.spark.sql.catalyst.expressions.aggregate.Max
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.catalyst.expressions.aggregate.Count


object CHUtil {
  def getFields(table: CHTableRef): Array[StructField] = {
    val metadata = new MetadataBuilder().putString("name", table.mappedName).build()

    val resp = new CHExecutorParall(CHUtil.genQueryId, CHSql.desc(table.absName), table.host, table.port, table.absName, 1)
    var fields = new Array[StructField](0)

    var names = new Array[String](0)
    var types = new Array[String](0)

    var block: resp.Result = resp.next

    while (block != null) {
      val columns = block.decoded.block.getFieldVectors
      if (columns.size < 2) {
        block.close
        resp.close
        throw new Exception("Send desc table to get schema failed")
      }

      val accNames = columns.get(0).getAccessor()
      for (i <- 0 until accNames.getValueCount) {
          names :+= accNames.getObject(i).toString
      }
      val accTypes = columns.get(1).getAccessor
      for (i <- 0 until accTypes.getValueCount) {
          types :+= accTypes.getObject(i).toString
      }

      block.close
      block = resp.next
    }

    resp.close

    for (i <- 0 until names.length) {
      // TODO: Get nullable info (from where?)
      val field = StructField(names(i), stringToSparkType(types(i)), nullable = true, metadata)
      fields :+= field
    }

    fields
  }

  // TODO: Pushdown more, like `In`
  def isSupportedFilter(exp: Expression): Boolean = {
    // println("PROBE isSupportedFilter:" + exp.getClass.getName + ", " + exp)
    exp match {
      case _: Literal => true
      case _: AttributeReference => true
      case _: Cast => true
      // TODO: Don't pushdown IsNotNull maybe better
      case IsNotNull(child) =>
        isSupportedFilter(child)
      case Add(lhs, rhs) =>
        isSupportedFilter(lhs) && isSupportedFilter(rhs)
      case Subtract(lhs, rhs) =>
        isSupportedFilter(lhs) && isSupportedFilter(rhs)
      case Multiply(lhs, rhs) =>
        isSupportedFilter(lhs) && isSupportedFilter(rhs)
      case Divide(lhs, rhs) =>
        isSupportedFilter(lhs) && isSupportedFilter(rhs)
      case Remainder(lhs, rhs) =>
        isSupportedFilter(lhs) && isSupportedFilter(rhs)
      // TODO: Check Alias's handling is OK
      case Alias(child, name) =>
        isSupportedFilter(child)
      case GreaterThan(lhs, rhs) =>
        isSupportedFilter(lhs) && isSupportedFilter(rhs)
      case GreaterThanOrEqual(lhs, rhs) =>
        isSupportedFilter(lhs) && isSupportedFilter(rhs)
      case LessThan(lhs, rhs) =>
        isSupportedFilter(lhs) && isSupportedFilter(rhs)
      case LessThanOrEqual(lhs, rhs) =>
        isSupportedFilter(lhs) && isSupportedFilter(rhs)
      case EqualTo(lhs, rhs) =>
        isSupportedFilter(lhs) && isSupportedFilter(rhs)
      // TODO: !=
      case Not(child) =>
        isSupportedFilter(child)
      case _ => false
    }
  }

  def expToCHString(filters: Seq[Expression]): String = {
    filters.map(filter => {
      "(" + expToCHString(filter) + ")"
    }).mkString(" AND ")
  }

  def expToCHString(filter: Expression): String = {
    filter match {
      case Literal(value, dataType) =>
        sparkValueToString(value, dataType)
      case attr: AttributeReference =>
        attr.name
      case Cast(child, dataType) =>
        getCastString(expToCHString(child), dataType)
      case IsNotNull(child) =>
        expToCHString(child) + " IS NOT NULL"
      case Add(lhs, rhs) =>
        expToCHString(lhs) + " + " + expToCHString(rhs)
      case Subtract(lhs, rhs) =>
        expToCHString(lhs) + " - " + expToCHString(rhs)
      case Multiply(lhs, rhs) =>
        expToCHString(lhs) + " * " + expToCHString(rhs)
      case Divide(lhs, rhs) =>
        expToCHString(lhs) + " / " + expToCHString(rhs)
      // TODO: Check Remainder's handling is OK
      case Remainder(lhs, rhs) =>
        expToCHString(lhs) + " % " + expToCHString(rhs)
      // TODO: Check Alias's handling is OK
      case Alias(child, name) =>
        expToCHString(child) + " AS " + name
      case GreaterThan(lhs, rhs) =>
        expToCHString(lhs) + " > " + expToCHString(rhs)
      case GreaterThanOrEqual(lhs, rhs) =>
        expToCHString(lhs) + " >= " + expToCHString(rhs)
      case LessThan(lhs, rhs) =>
        expToCHString(lhs) + " < " + expToCHString(rhs)
      case LessThanOrEqual(lhs, rhs) =>
        expToCHString(lhs) + " <= " + expToCHString(rhs)
      case EqualTo(lhs, rhs) =>
        expToCHString(lhs) + " = " + expToCHString(rhs)
      // case Not(EqualTo(lhs), rhs) =>
      //  getFilterString(lhs) + " != " + getFilterString(rhs)
      case Sum(child) =>
        "SUM(" + expToCHString(child) + ")"
      case Average(child) =>
        "AVG(" + expToCHString(child) + ")"
      case Count(children) =>
        "COUNT(" + children.map(expToCHString).mkString(",") + ")"
      case Min(child) =>
        "MIN(" + expToCHString(child) + ")"
      case Max(child) =>
        "MAX(" + expToCHString(child) + ")"
      case Not(child) =>
        "NOT " + expToCHString(child)
    }
  }

  def genQueryId(): String = {
    "chspark-" + Random.nextInt.toString + "-" + Random.nextInt.toString
  }

  private def getCastString(value: String, dataType: DataType) = {
    // TODO: Handle cast
    value
  }

  private def stringToSparkType(name: String): DataType = {
    // May have bugs: promote unsiged types, and ignore uint64 overflow
    // TODO: Support all types
    if (name.startsWith("FixedString")) {
      StringType
    } else {
      name match {
        case "String" => StringType
        case "DateTime" => TimestampType
        case "Date" => TimestampType
        case "Int8" => ByteType
        case "Int16" => ShortType
        case "Int32" => IntegerType
        case "Int64" => LongType
        case "UInt8" => IntegerType
        case "UInt16" => IntegerType
        case "UInt32" => LongType
        case "UInt64" => LongType
        case "Float32" => FloatType
        case "Float64" => DoubleType
        case _ => throw new Exception("stringToFieldType unhandled type name: " + name)
      }
    }
  }

  private def sparkValueToString(value: Any, dataType: DataType): String = {
    if (dataType == null) {
      null
    } else {
      dataType match {
        case StringType => "'" + value.toString + "'"
        case _ => value.toString
      }
    }
  }
}
