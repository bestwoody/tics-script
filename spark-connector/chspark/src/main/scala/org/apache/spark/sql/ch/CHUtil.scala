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

import org.apache.arrow.vector.VectorSchemaRoot;

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.MetadataBuilder

import org.apache.spark.sql.types.{StringType, TimestampType}
import org.apache.spark.sql.types.{FloatType, DoubleType}
import org.apache.spark.sql.types.{ByteType, ShortType, IntegerType, LongType}

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


object CHUtil {
  def getFields(table: CHTableRef): Array[StructField] = {
    val metadata = new MetadataBuilder().putString("name", table.mappedName).build()

    val resp = new CHExecutorParall(CHSql.desc(table.absName), table.host, table.port, table.absName, 1)
    var fields = new Array[StructField](0)

    var names = new Array[String](0)
    var types = new Array[String](0)

    var block: resp.Result = null

    while (resp.hasNext) {
      block = resp.next
      if (block != null) {
        val columns = block.decoded.block.getFieldVectors
        if (columns.size < 2) {
          block.close
          resp.close
          // TODO: Exception classify
          throw new Exception("Send desc table to get schema failed")
        }

        val accNames = columns.get(0).getAccessor();
        for (i <- 0 until accNames.getValueCount) {
            names :+= accNames.getObject(i).toString
        }
        val accTypes = columns.get(1).getAccessor;
        for (i <- 0 until accTypes.getValueCount) {
            types :+= accTypes.getObject(i).toString
        }

        block.close
      }
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

  def getFilterString(filters: Seq[Expression]): String = {
    filters.map(filter => {
      "(" + getFilterString(filter) + ")"
    }).mkString(" AND ")
  }

  private def getFilterString(filter: Expression): String = {
    filter match {
      case Literal(value, dataType) =>
        sparkValueToString(value, dataType)
      case attr: AttributeReference =>
        attr.name
      case Cast(child, dataType) =>
        getCastString(getFilterString(child), dataType)
      case IsNotNull(child) =>
        getFilterString(child) + " IS NOT NULL"
      case Add(lhs, rhs) =>
        getFilterString(lhs) + " + " + getFilterString(rhs)
      case Subtract(lhs, rhs) =>
        getFilterString(lhs) + " - " + getFilterString(rhs)
      case Multiply(lhs, rhs) =>
        getFilterString(lhs) + " * " + getFilterString(rhs)
      case Divide(lhs, rhs) =>
        getFilterString(lhs) + " / " + getFilterString(rhs)
      // TODO: Check Remainder's handling is OK
      case Remainder(lhs, rhs) =>
        getFilterString(lhs) + " % " + getFilterString(rhs)
      // TODO: Check Alias's handling is OK
      case Alias(child, name) =>
        getFilterString(child) + " AS " + name
      case GreaterThan(lhs, rhs) =>
        getFilterString(lhs) + " > " + getFilterString(rhs)
      case GreaterThanOrEqual(lhs, rhs) =>
        getFilterString(lhs) + " >= " + getFilterString(rhs)
      case LessThan(lhs, rhs) =>
        getFilterString(lhs) + " < " + getFilterString(rhs)
      case LessThanOrEqual(lhs, rhs) =>
        getFilterString(lhs) + " <= " + getFilterString(rhs)
      case EqualTo(lhs, rhs) =>
        getFilterString(lhs) + " = " + getFilterString(rhs)
      // case Not(EqualTo(lhs), rhs) =>
      //  getFilterString(lhs) + " != " + getFilterString(rhs)
      case Not(child) =>
        "NOT " + getFilterString(child)
    }
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
