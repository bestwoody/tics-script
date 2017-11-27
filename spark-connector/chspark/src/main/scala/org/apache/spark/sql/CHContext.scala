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

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

import org.apache.spark.sql.ch.CHRelation
import org.apache.spark.sql.ch.CHStrategy

class CHContext (val session: SparkSession) extends Serializable with Logging {
  val sqlContext: SQLContext = session.sqlContext
  //val conf: SparkConf = session.sparkContext.conf

  session.experimental.extraStrategies ++= Seq(new CHStrategy(sqlContext))

  def chMapDatabase(): Unit = {
    val name = "test"
    val rel = new CHRelation(name)(sqlContext)
    sqlContext.baseRelationToDataFrame(rel).createTempView(name)
  }

  def sql(sqlText: String): DataFrame = {
    sqlContext.sql(sqlText)
  }
}
