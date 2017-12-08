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

import java.lang.Character

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructField, StructType}
//import org.apache.spark.sql.types.{StringType}
//import org.apache.spark.sql.types.{FloatType, DoubleType}
//import org.apache.spark.sql.types.{ShortType, IntegerType, LongType}

import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID

import scala.collection.mutable.ArrayBuffer


object ArrowConverter {
  def toRows(schema: Schema, table: String, block: VectorSchemaRoot): Iterator[Row] = new Iterator[Row] {
    val columns = block.getFieldVectors
    var curr = 0;
    val rows = if (!columns.isEmpty) {
      columns.get(0).getAccessor.getValueCount
    } else {
      0
    }

    override def hasNext: Boolean = {
      curr < rows
    }

    override def next(): Row = {
      val fields = new Array[Any](columns.size)
      for (i <- 0 until fields.length) {
        fields(i) = fromArrow(columns.get(i).getAccessor.getObject(curr))
      }
      curr += 1
      Row.fromSeq(fields)
    }
  }

  private def fromArrow(value: Any): Any = {
    // TODO: Handle all types
    value match {
      case text: Text => text.toString
      case char: Character => {
        Character.getNumericValue(char)
      }
      case _ => value
    }
  }
}
