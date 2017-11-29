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

// TODO: more specific import
import org.apache.spark._

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.sources.BaseRelation


class CHRelation(tableName: String)(@transient val sqlContext: SQLContext) extends BaseRelation {
  override lazy val schema: StructType = {
    val fields = new Array[StructField](1)
    val name="col1"
    val metadata = new MetadataBuilder().putString("name", name).build()
    fields(0) = StructField(name, StringType, nullable = true, metadata)
    new StructType(fields)
  }
}
