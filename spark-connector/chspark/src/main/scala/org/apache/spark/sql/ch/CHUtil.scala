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


object CHUtil {
  def getFields(table: CHTableRef): Array[StructField] = {
    val metadata = new MetadataBuilder().putString("name", table.mappedName).build()

    val resp = new CHResponse(CHSql.desc(table.absName), table.host, table.port, CHEnv.arrowDecoder)
    var fields = new Array[StructField](0)

    var names = new Array[String](0)
    var types = new Array[String](0)

    var block: VectorSchemaRoot = resp.next

    while (resp.hasNext && (block != null)) {
      val columns = block.getFieldVectors
      if (columns.size < 2) {
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

      block = resp.next
    }

    for (i <- 0 until names.length) {
      // TODO: Get nullable info (from where?)
      val field = StructField(names(i), stringToFieldType(types(i)), nullable = true, metadata)
      fields :+= field
    }

    fields
  }

  def stringToFieldType(name: String): DataType = {
    // May have bugs: promote unsiged types, and ignore uint64 overflow
    // TODO: Support all types
    name match {
      case "String" => StringType
      case "DateTime" => TimestampType
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

  def getFilterString(filter: Seq[Expression]): String = {
    println("FF:" + filter)
    val strs = filter.map(x => {
      val s = getFilterString(x)
      println("X: " + x + " => " + s)
      s match {
        case null => null
        case _ => "(" + s + ")"
      }
    })
    println("SS:" + strs)
    val strSet = strs.toSet
    if (strSet(null)) {
      null
    } else {
      strs.mkString(" AND ")
    }
  }

  def getFilterString(filter: Expression): String = {
    "<TODO>"
  }

  def isSupportedFilter(filter: Expression): Boolean = {
    false
  }
}
