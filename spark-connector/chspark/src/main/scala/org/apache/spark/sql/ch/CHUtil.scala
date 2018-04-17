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

import java.util.UUID

import org.apache.spark.sql.catalyst.expressions.{Add, Alias, AttributeReference, Cast, Divide, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, IsNotNull, LessThan, LessThanOrEqual, Literal, Multiply, Not, Remainder, Subtract}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types._

object CHUtil {
  def getFields(table: CHTableRef): Array[StructField] = {
    val metadata = new MetadataBuilder().putString("name", table.mappedName).build()

    val resp = new CHExecutorParall(CHUtil.genQueryId("D"), CHSql.desc(table), table.host, table.port, table.absName, 1)
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

      val fieldVector = columns.get(0)
      for (i <- 0 until fieldVector.getValueCount) {
          names :+= fieldVector.getObject(i).toString
      }
      val typeVector = columns.get(1)
      for (i <- 0 until typeVector.getValueCount) {
          types :+= typeVector.getObject(i).toString
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

  def getRowCount(table: CHTableRef, useSelraw: Boolean = false): Long = {
    val resp = new CHExecutorParall(CHUtil.genQueryId("C"), CHSql.count(table, useSelraw), table.host, table.port, table.absName, 1)
    var block: resp.Result = resp.next

    if (block == null) {
      resp.close
      0
    } else {
      val columns = block.decoded.block.getFieldVectors
      if (columns.size != 1) {
        block.close
        resp.close
        throw new Exception("Send table row count request, wrong response")
      }

      val acc = columns.get(0)
      if (acc.getValueCount != 1) {
        throw new Exception("Send table row count request, get too much response")
      }

      val rows: Long = acc.getObject(0).asInstanceOf[Long]
      block.close
      resp.close
      rows
    }
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

  def isSupportedAggregate(aggregateFunction: AggregateFunction): Boolean = {
    aggregateFunction match {
      case Average(_) => true
      case Sum(_) => true
      case Count(_) => true
      case Min(_) => true
      case Max(_) => true
      case First(_, _) => true
      case Last(_, _) => true
      case _ => false
    }
  }

  def genQueryId(prefix: String): String = {
    this.synchronized {
      prefix + UUID.randomUUID.toString
    }
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
        case "Date" => DateType
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
}
