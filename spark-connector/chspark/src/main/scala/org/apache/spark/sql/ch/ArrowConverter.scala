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
import org.apache.spark.sql.types.{DoubleType, StringType, IntegerType, FloatType}

import org.apache.arrow.vector.types.pojo.{ArrowType, Schema}
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}

import scala.collection.mutable.ArrayBuffer


object ArrowConverter {
  def toFields(schema: Schema, table: String): StructType = {
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
    val arrowString = new ArrowType.Utf8
    val arrowInt = new ArrowType.Int(8, true)
    val arrowFloat = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    val arrowDouble = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    arrowType match {
      case arrowString => StringType
      case arrowInt => IntegerType
      case arrowFloat => FloatType
      case arrowDouble => DoubleType
      case _ => throw new Exception("No macthed DataType.")
    }
  }

  def matchDataTypeToArray(dataType: DataType, values: Any): String = {
    dataType match {
      case StringType => values.toString
      case IntegerType => values.toString
      case FloatType => values.toString
      case DoubleType => values.toString
      case _ => throw new Exception("Unsupported types DataType.")
    }
  }

  def toRows(schema: Schema, table: String, block: VectorSchemaRoot): Iterator[Row] = new Iterator[Row] {
    // TODO: NOW
    // Convert arrow-columns to spark-rows
    private val structType: StructType = toFields(schema, table)
    private val dataTypes: Array[DataType] = structType.fields.map(_.dataType)
    private val vectors: util.List[FieldVector] = block.getFieldVectors

    val rowArray = new Array[ArrayBuffer[String]](vectors.size())
    if (vectors.isEmpty) {
      Nil
    } else {
      for (i <- 0 to vectors.size()) {
        val fieldVector = vectors.get(i)
        val accessor = fieldVector.getAccessor
        val dataType = dataTypes(i)
        for (j <- 0 to accessor.getValueCount) {
          val value = accessor.getObject(j)
          val fieldValue = matchDataTypeToArray(dataType, value)
          rowArray(j) = new ArrayBuffer[String](i) += fieldValue
        }
      }
    }

    val iterator = rowArray.iterator

    override def hasNext: Boolean = false

    override def next(): Row = toSparkRow(iterator.next)

    def toSparkRow(row: ArrayBuffer[String]): Row = {
      val rowlines = row.map(p => Row(p))
      Row.fromSeq(rowlines)
    }
  }
}