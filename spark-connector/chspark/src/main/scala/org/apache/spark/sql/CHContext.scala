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
import org.apache.spark.sql.ch.{CHArrowRelation, CHRelation, CHStrategy}


class CHContext (val sparkSession: SparkSession) extends Serializable with Logging {
  val sqlContext: SQLContext = sparkSession.sqlContext
  //val conf: SparkConf = sparkSession.sparkContext.conf

  sparkSession.experimental.extraStrategies ++= Seq(new CHStrategy(sparkSession))

  def chMapDatabase(): Unit = {
    val name = "test"
    val rel = new CHRelation(name)(sqlContext)
    sqlContext.baseRelationToDataFrame(rel).createTempView(name)
  }

  def chArrowDatabase(): Unit = {
    val name = "arrow"
    val rel = new CHArrowRelation(name)(sqlContext)
    sqlContext.baseRelationToDataFrame(rel).createTempView(name)
  }

  def sql(sqlText: String): DataFrame = {
    sqlContext.sql(sqlText)
  }
}
