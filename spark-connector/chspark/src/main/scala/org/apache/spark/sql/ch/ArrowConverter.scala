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

import java.util

import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.types.{DoubleType,StringType,IntegerType,FloatType}
import org.apache.arrow.vector.types.pojo.{ArrowType, Schema}
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot};


object ArrowConverter {
  def toFields(schema: Schema, table: String): StructType = {
    // TODO: Get schema from CH table
    val schema: Schema = null
    val fields = new Array[StructField](schema.getFields.size)
    val metadata = new MetadataBuilder().putString("name", table).build()
    for (i <- 0 to schema.getFields().size()) {
      val field = schema.getFields.get(i)
      fields(i) = StructField(field.getName(), matchFieldType(field.getFieldType.getType), nullable = true, metadata)
    }
    new StructType(fields)

  }

  def matchFieldType(arrowType: ArrowType): DataType = {
    val arrowDouble = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    val arrowInt = new ArrowType.Int(8, true)
    val arrowString = new ArrowType.Utf8
    val arrowFloat = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    arrowType match {
      case arrowDouble => DoubleType
      case arrowString => StringType
      case arrowInt => IntegerType
      case arrowFloat => FloatType
      case _ => throw new Exception("No macthed DataType.")
    }
}

  def toRows(schema: Schema, block: VectorSchemaRoot): Iterator[Row] = new Iterator[Row] {
    // TODO: NOW
    // Convert arrow-columns to spark-rows
    private val vectors: util.List[FieldVector] = block.getFieldVectors
    override def hasNext: Boolean = false
    override def next(): Row = null
  }
}