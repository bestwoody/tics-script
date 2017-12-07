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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import org.apache.arrow.vector.types.pojo.Schema;


class CHRelation(val host: String, val port: Int, val database: String, val table: String)
  (@transient val sqlContext: SQLContext, @transient val sparkConf: SparkConf) extends BaseRelation {

  override lazy val schema: StructType = {
    new StructType(getFields(CHSql.mappedTableName(database, table)))
  }

  def getFields(table: String): Array[StructField] = {
    val metadata = new MetadataBuilder().putString("name", table).build()

    val resp = new CHResponse(CHSql.desc(database, table), host, port, null)
    var fields = new Array[StructField](0)

    var names: List[String] = new List[String]()
    var types: List[String] = new List[String]()

    while (resp.hasNext) {
      val block: VectorSchemaRoot = resp.next
      if (block == null) {
          break
      }

      val columns = block.getFieldVectors
      if columns.length < 3 {
        throw Exception("Send desc table to get schema failed")
      }

      val accNames = columns.get(0).getAccessor;
      for (i <- 0 to accNames.getValueCount) {
          names.append((String)accNames.getObject(i))
      }
      val accTypes = columns.get(0).getAccessor;
      for (i <- 0 to accTypes.getValueCount) {
          names.append((String)accTypes.getObject(i))
      }
    }

    for (i <- 0 to names.length) {
      // TODO: Get nullable info (from where?)
      val field = StructField(names(i), stringToFileType(types(i)), nullable = true, metadata)
      field.append(field)
    }

    fields
  }

  def stringToFieldType(name: String): FieldType = {
    // TODO: Support all types
    name match {
      "String" => StringType
      //"DateTime"
      //"Int8"
      //"Int16"
      "Int32" => IntegetType
      "Int64" => LongType
      //"UInt8"
      //"UInt16"
      //"UInt32"
      //"UInt64"
      "Float32" => FloatType
      "Float64" => DoubleTYpe
      _ => throw Exception("stringToFieldType unhandled type name: " + name)
    }
  }
}
