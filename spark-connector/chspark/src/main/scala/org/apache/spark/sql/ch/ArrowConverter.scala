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
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType}
import org.apache.arrow.vector.types.pojo.{ArrowType, Schema}
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}

import scala.collection.mutable.ArrayBuffer


object ArrowConverter {
  def toFields(schema: Schema, table: String): StructType = {
    val metadata = new MetadataBuilder().putString("name", table).build
    val fields = new Array[StructField](schema.getFields.size)
    for (i <- 0 to schema.getFields.size) {
      val field = schema.getFields.get(i)
      fields(i) = StructField(field.getName, fieldType(field.getType.getTypeID), field.isNullable, metadata)
    }
    new StructType(fields)
  }

  def fieldType(arrowType: ArrowTypeID): DataType = {
    // TODO: Handle all types
    arrowType match {
      case ArrowType.ArrowTypeID.Utf8 => StringType
      case ArrowType.ArrowTypeID.Int => IntegerType
      case ArrowType.ArrowTypeID.FloatingPoint => FloatType
      case ArrowType.ArrowTypeID.FloatingPoint  => DoubleType
      case _ => throw new Exception("No macthed DataType.")
    }
  }

  def toRows(schema: Schema, table: String, block: VectorSchemaRoot): Iterator[Row] = new Iterator[Row] {
    val vectors: util.List[FieldVector] = block.getFieldVectors
    var curr = 0;

    override def hasNext: Boolean = {
      !vectors.isEmpty && (curr != vectors(0).size)
    }

    override def next(): Row = {
      val row = new Array[Any](vectors.size)
      for (i <- 0 to vectors.size) {
        row(i) = vectors.get(i).getAccessor.getObject(curr)
      }
      Row.fromSeq(row)
    }
  }
}
