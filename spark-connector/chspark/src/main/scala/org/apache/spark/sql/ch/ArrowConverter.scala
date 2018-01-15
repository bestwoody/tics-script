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
import java.sql.Timestamp

import org.apache.spark.sql.Row

import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.spark.sql.catalyst.expressions.GenericRow

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._


class CHRows(private val schema: Schema, private val table: String, private val block: CHExecutor.Result)
  extends Iterator[Row] {

  val columns = block.block.getFieldVectors
  val columnNumber = columns.size
  val fieldTypes = columns.asScala.map(x => x.getField.getType)

  val rows: Int = if (columns.isEmpty) 0 else columns.get(0).getAccessor().getValueCount
  var cache = new Array[Row](rows)

  for (r <- 0 until rows) {
    val fields = new Array[Any](columnNumber)
    for (i <- 0 until fields.length) {
      fields(i) = ArrowConverter.fromArrow(fieldTypes(i), columns.get(i).getAccessor.getObject(r))
    }
    cache(r) = new GenericRow(fields)
  }

  var curr = 0
  override def hasNext: Boolean = { curr < rows }

  override def next(): Row = {
    val result = cache(curr)
    curr += 1
    result
  }

  def close(): Unit = {
    for (i <- 0 until columns.size) {
      columns.get(i).close
    }
    block.close
  }
}

object ArrowConverter {
  // TODO: Faster algorithm
  val uint8Reverser: Int = 0x100
  val uint16Reverser: Int = 0x10000
  val uint32Reverser: Long = 0x100000000L

  def toRows(schema: Schema, table: String, block: CHExecutor.Result): Iterator[Row] = new CHRows(schema, table, block)

  def fromArrow(arrowType: ArrowType, value: Any): Any = {
    arrowType match {
      case time: ArrowType.Time => new Timestamp(value.asInstanceOf[Long] * 1000)
      case _ => {
        value match {
          case text: Text => text.toString
          case int8: Byte => {
            if (arrowType.asInstanceOf[ArrowType.Int].getIsSigned) {
              int8
            } else {
              if (int8 >= 0) {
                int8.asInstanceOf[Int]
              } else {
                int8.asInstanceOf[Int] + uint8Reverser
              }
            }
          }
          case char: Character => {
            val int16 = Character.getNumericValue(char)
            if (int16 >= 0) {
              int16.asInstanceOf[Int]
            } else {
              int16.asInstanceOf[Int] + uint16Reverser
            }
          }
          case int32: Int => {
            if (arrowType.isInstanceOf[ArrowType.Time]) {
              int32
            } else if (arrowType.asInstanceOf[ArrowType.Int].getIsSigned) {
              int32
            } else {
              if (int32 >= 0) {
                int32.asInstanceOf[Long]
              } else {
                int32.asInstanceOf[Long] + uint32Reverser
              }
            }
          }
          case _ => value
        }
      }
    }
  }
}
