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

import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.arrow.vector.types.pojo.Schema


class CHRelation(val tables: Seq[CHTableRef], val partitions: Int, val decoders: Int, val encoders: Int)
  (@transient val sqlContext: SQLContext, @transient val sparkConf: SparkConf) extends BaseRelation {

  // TODO: Parallel check tables' schema are the same
  override lazy val schema: StructType = {
    new StructType(CHUtil.getFields(tables(0)))
  }
}
