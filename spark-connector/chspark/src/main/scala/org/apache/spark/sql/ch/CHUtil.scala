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

import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, LongType, StringType}


object CHUtil {
  def getFields(host: String, port: Int, database: String, table: String): Array[StructField] = {
    val mappedTableName = CHSql.mappedTableName(database, table)
    val metadata = new MetadataBuilder().putString("name", mappedTableName).build()

    val resp = new CHResponse(CHSql.desc(database, table), host, port, null)
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
      for (i <- 0 to accNames.getValueCount) {
          names :+= accNames.getObject(i).asInstanceOf[String]
      }
      val accTypes = columns.get(0).getAccessor;
      for (i <- 0 to accTypes.getValueCount) {
          types :+= accTypes.getObject(i).asInstanceOf[String]
      }

      block = resp.next
    }

    for (i <- 0 to names.length) {
      // TODO: Get nullable info (from where?)
      val field = StructField(names(i), stringToFieldType(types(i)), nullable = true, metadata)
      fields :+= field
    }

    fields
  }

  def stringToFieldType(name: String): DataType = {
    // TODO: Support all types
    name match {
      case "String" => StringType
      // case "DateTime"
      // case "Int8"
      // case "Int16"
      case "Int32" => IntegerType
      case "Int64" => LongType
      // case "UInt8"
      // case "UInt16"
      // case "UInt32"
      // case "UInt64"
      case "Float32" => FloatType
      case "Float64" => DoubleType
      case _ => throw new Exception("stringToFieldType unhandled type name: " + name)
    }
  }
}
