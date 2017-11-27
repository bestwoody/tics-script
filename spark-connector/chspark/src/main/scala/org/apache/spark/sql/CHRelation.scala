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

package org.apache.spark.sql

//import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.{SparkConf, sql}

class CHRelation(tableName: String)(@transient val sqlContext: SQLContext) extends BaseRelation {
  // TODO
  // override lazy val schema: StructType = Utils.getSchemaFromTable(table)
  override lazy val schema: StructType = {
    val fields = new Array[StructField](1)
    val name="col1"
    val metadata = new MetadataBuilder().putString("name", name).build()
    fields(0) = StructField(name, sql.types.StringType, nullable = true, metadata)
    new StructType(fields)
  }
}
