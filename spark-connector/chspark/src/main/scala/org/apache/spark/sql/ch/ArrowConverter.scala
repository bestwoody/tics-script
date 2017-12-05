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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.catalyst.InternalRow

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.VectorSchemaRoot;


object ArrowConverter {
  def toFields(schema: Schema): Array[StructField] = {
    // TODO: NOW
    // Convert arrow-schema to spark-fields
    new Array[StructField](0)
  }

  def toRows(block: VectorSchemaRoot): Iterator[Row] = new Iterator[Row] {
    // TODO: NOW
    // Convert arrow-columns to spark-rows
    override def hasNext: Boolean = false
    override def next(): Row = null
  }
}
