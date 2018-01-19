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
import org.apache.spark.sql.ch.CHStrategy

import org.apache.spark.sql.ch.CHRelation
import org.apache.spark.sql.ch.CHSql
import org.apache.spark.sql.ch.CHTableRef
import org.apache.spark.sql.ch.mock.TypesTestRelation

class CHContext (val sparkSession: SparkSession)
  extends Serializable with Logging {

  val sqlContext: SQLContext = sparkSession.sqlContext

  sparkSession.experimental.extraStrategies ++= Seq(new CHStrategy(sparkSession))

  def mapTypesTestTable(name: String = "types-test"): Unit = {
    val rel = new TypesTestRelation(name)(sqlContext)
    sqlContext.baseRelationToDataFrame(rel).createTempView(name)
  }

  def mapCHTable(
    host: String = "127.0.0.1",
    port: Int = 9006,
    database: String = null,
    table: String,
    partitions: Int = 8,
    decoders: Int = 8,
    encoders: Int = 0): Unit = {

    val conf: SparkConf = sparkSession.sparkContext.conf
    val tableRef = new CHTableRef(host, port, database, table)
    val rel = new CHRelation(Seq(tableRef), partitions, decoders, encoders)(sqlContext, conf)
    sqlContext.baseRelationToDataFrame(rel).createTempView(tableRef.mappedName)
  }

  def mapCHClusterTable(
    addresses: Seq[(String, Int)] = Seq(("127.0.0.1", 9006)),
    database: String = null,
    table: String,
    partitions: Int = 8,
    decoders: Int = 8,
    encoders: Int = 0): Unit = {

    val conf: SparkConf = sparkSession.sparkContext.conf
    val tableRefList: Seq[CHTableRef] =
      addresses.map(addr => new CHTableRef(addr._1, addr._2, database, table))
    val rel = new CHRelation(tableRefList, partitions, decoders, encoders)(sqlContext, conf)
    sqlContext.baseRelationToDataFrame(rel).createTempView(tableRefList(0).mappedName)
  }

  def mapCHClusterTableSimple(
    hosts: String = "127.0.0.1",
    port: Int = 9006,
    database: String = null,
    table: String,
    partitions: Int = 8,
    decoders: Int = 8,
    encoders: Int = 0): Unit = {

    val conf: SparkConf = sparkSession.sparkContext.conf
    val tableRefList: Seq[CHTableRef] =
      hosts.split(",").map(host => new CHTableRef(host, port, database, table))
    val rel = new CHRelation(tableRefList, partitions, decoders, encoders)(sqlContext, conf)
    sqlContext.baseRelationToDataFrame(rel).createTempView(tableRefList(0).mappedName)
  }

  def sql(sqlText: String): DataFrame = {
    sqlContext.sql(sqlText)
  }
}
