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

import java.util.{SplittableRandom, UUID}

import com.pingcap.common.{Cluster, Node}
import com.pingcap.theflash.{SparkCHClientInsert, SparkCHClientSelect, TypeMappingJava}
import com.pingcap.tikv.meta.TiTableInfo
import org.apache.spark.Partitioner
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Abs, Add, And, AttributeReference, Cast, CreateNamedStruct, Divide, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, Multiply, Not, Or, Remainder, Subtract, UnaryMinus}
import org.apache.spark.sql.ch.hack.Hack
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.JavaConversions._
import scala.collection.mutable

object CHUtil {

  object Partitioner extends Enumeration {
    val Hash, Random = Value
  }

  def createTable(database: String,
                  table: String,
                  schema: StructType,
                  primaryKeys: Array[String],
                  cluster: Cluster): (Partitioner.Value, Int) = {
    try {
      cluster.nodes.foreach(node => createTable(database, table, schema, primaryKeys, node))
      if (primaryKeys.length == 1) {
        val primaryKey = primaryKeys.head.toLowerCase()
        val (col, index) = schema.fields.zipWithIndex.filter {
          case (c, _) => c.name.toLowerCase() == primaryKey
        }.head
        if (col.dataType.isInstanceOf[IntegralType]) {
          (Partitioner.Hash, index)
        } else {
          (Partitioner.Random, -1)
        }
      } else {
        (Partitioner.Random, -1)
      }
    } catch {
      // roll back if any exception
      case e: Throwable =>
        cluster.nodes.foreach(node => tryDropTable(database, table, node))
        throw e
    }
  }

  def createTable(database: String,
                  table: TiTableInfo,
                  cluster: Cluster,
                  partitionNum: Int = 128): (Partitioner.Value, Int) = {
    try {
      cluster.nodes.foreach(node => createTable(database, table, node, partitionNum))
      if (table.isPkHandle) {
        val (_, index) = table.getColumns.zipWithIndex.filter {
          case (c, _) => c.isPrimaryKey
        }.head
        (Partitioner.Hash, index)
      } else {
        (Partitioner.Random, -1)
      }
    } catch {
      // roll back if any exception
      case e: Throwable =>
        cluster.nodes.foreach(node => tryDropTable(database, table.getName, node))
        throw e
    }
  }

  private def createTable(database: String,
                          table: TiTableInfo,
                          node: Node,
                          partitionNum: Int): Unit = {
    var client: SparkCHClientSelect = null
    try {
      val queryString = CHSql.createTableStmt(database, table, partitionNum)
      client = new SparkCHClientSelect(queryString, node.host, node.port)
      while (client.hasNext) {
        client.next()
      }
    } finally {
      if (client != null) {
        client.close()
      }
    }
  }

  private def createTable(database: String,
                          table: String,
                          schema: StructType,
                          primaryKeys: Array[String],
                          node: Node): Unit = {
    var client: SparkCHClientSelect = null
    try {
      val queryString = CHSql.createTableStmt(database, schema, primaryKeys, table)
      client = new SparkCHClientSelect(queryString, node.host, node.port)
      while (client.hasNext) {
        client.next()
      }
    } finally {
      if (client != null) {
        client.close()
      }
    }
  }

  def dropTable(database: String, table: String, cluster: Cluster): Unit = {
    try {
      cluster.nodes.foreach(node => dropTable(database, table, node))
    } catch {
      case e: Throwable =>
        // try best to drop as many as possible
        // TODO: try avoid partially drop table
        cluster.nodes.foreach(node => tryDropTable(database, table, node))
        throw e
    }
  }

  def createDatabase(database: String, cluster: Cluster): Unit = {
    try {
      cluster.nodes.foreach(node => createDatabase(database, node))
    } catch {
      case e: Throwable =>
        // try best to drop as many as possible
        // TODO: try avoid partially drop table
        cluster.nodes.foreach(node => tryDropDatabase(database, node))
        throw e
    }
  }

  def dropDatabase(database: String, cluster: Cluster): Unit = {
    try {
      cluster.nodes.foreach(node => dropDatabase(database, node))
    } catch {
      case e: Throwable => throw e
    }
  }

  private def tryDropDatabase(database: String, node: Node) = {
    try {
      dropDatabase(database, node)
    } catch {
      case _: Throwable => // ignore
    }
  }

  private def tryDropTable(database: String, table: String, node: Node) = {
    try {
      dropTable(database, table, node)
    } catch {
      case _: Throwable => // ignore
    }
  }

  private def dropTable(database: String, table: String, node: Node): Unit = {
    val queryString = CHSql.dropTableStmt(database, table)
    var client: SparkCHClientSelect = null
    try {
      client = new SparkCHClientSelect(queryString, node.host, node.port)
      while (client.hasNext) {
        client.next()
      }
    } finally {
      if (client != null) {
        client.close()
      }
    }
  }

  private def createDatabase(database: String, node: Node): Unit = {
    val queryString = CHSql.createDatabaseStmt(database)
    var client: SparkCHClientSelect = null
    try {
      client = new SparkCHClientSelect(queryString, node.host, node.port)
      while (client.hasNext) {
        client.next()
      }
    } finally {
      if (client != null) {
        client.close()
      }
    }
  }

  private def dropDatabase(database: String, node: Node): Unit = {
    val queryString = CHSql.dropDatabaseStmt(database)
    var client: SparkCHClientSelect = null
    try {
      client = new SparkCHClientSelect(queryString, node.host, node.port)
      while (client.hasNext) {
        client.next()
      }
    } finally {
      if (client != null) {
        client.close()
      }
    }
  }

  class ConsistentPartitioner(val numNodes: Int, val multiplier: Int) extends Partitioner {
    def numPartitions: Int = numNodes * multiplier
    def rng = new SplittableRandom
    private val mappingTable = scala.util.Random.shuffle((0 until numPartitions).toList)

    def getPartition(key: Any): Int = {
      val intKey = key.asInstanceOf[Int]
      val seed = intKey * multiplier + rng.nextInt(0, multiplier)
      // further randomize bucket for better parallelism
      mappingTable(seed)
    }

    override def equals(other: Any): Boolean = other match {
      case h: ConsistentPartitioner =>
        h.numNodes == numNodes && h.multiplier == multiplier
      case _ =>
        false
    }

    override def hashCode: Int = numPartitions
  }

  def insertDataHash(df: DataFrame,
                     database: String,
                     table: String,
                     offset: Int,
                     cluster: Cluster,
                     fromTiDB: Boolean,
                     batchSize: Int,
                     parallelism: Int = 4): Unit = {
    val nodeNum = cluster.nodes.length
    val hash = (row: Row) =>
      (row.get(offset).asInstanceOf[Number].longValue() % nodeNum).asInstanceOf[Int]
    val shuffledRDD =
      df.rdd.keyBy(hash).partitionBy(new ConsistentPartitioner(nodeNum, parallelism))
    val schema = df.schema

    val insertMethod: (SparkCHClientInsert, Row) => Unit =
      if (fromTiDB) { (client: SparkCHClientInsert, row: Row) =>
        client.insertFromTiDB(row)
      } else { (client: SparkCHClientInsert, row: Row) =>
        client.insert(row)
      }

    shuffledRDD.foreachPartition { iter =>
      savePartition(database, table, cluster, iter, parallelism, schema, batchSize, insertMethod)
    }
  }

  def insertDataRandom(df: DataFrame,
                       database: String,
                       table: String,
                       cluster: Cluster,
                       fromTiDB: Boolean,
                       batchSize: Int): Unit = {

    val partitionMapper: mutable.HashMap[Int, Node] = mutable.HashMap()
    var i = 0
    val repartitionedDF = df.repartition(cluster.nodes.length)
    for (partition <- repartitionedDF.rdd.partitions) {
      val node = cluster.nodes(i)
      partitionMapper.put(partition.index, Node(node.host, node.port))
      i += 1
    }
    val schema = repartitionedDF.schema
    val insertMethod: (SparkCHClientInsert, Row) => Unit =
      if (fromTiDB) { (client: SparkCHClientInsert, row: Row) =>
        client.insert(row)
      } else { (client: SparkCHClientInsert, row: Row) =>
        client.insertFromTiDB(row)
      }

    repartitionedDF.rdd
      .mapPartitionsWithIndex { (index, iterator) =>
        {
          val node = partitionMapper(index)
          List(
            savePartition(
              database,
              table,
              node.host,
              node.port,
              iterator,
              schema,
              batchSize,
              insertMethod
            )
          ).iterator
        }
      }
      .collect()
  }

  // Do a one to one partition insertion
  def savePartition(database: String,
                    table: String,
                    cluster: Cluster,
                    iterator: Iterator[(Int, Row)],
                    multiplier: Int,
                    schema: StructType,
                    batchSize: Int,
                    insertMethod: (SparkCHClientInsert, Row) => Unit): Int = {

    var client: SparkCHClientInsert = null
    try {
      var totalCount = 0
      while (iterator.hasNext) {
        val res = iterator.next()
        val idx = res._1
        val row = res._2
        if (client == null) {
          val node = cluster.nodes(idx)
          client = new SparkCHClientInsert(CHSql.insertStmt(database, table), node.host, node.port)
          client.setBatch(batchSize)
          client.insertPrefix()
        }
        insertMethod(client, row)
        totalCount += 1
      }
      if (client != null) {
        client.insertSuffix()
      }
      totalCount
    } finally {
      if (client != null) {
        client.close()
      }
    }
  }

  // Do a one to one partition insertion
  def savePartition(database: String,
                    table: String,
                    host: String,
                    port: Int,
                    iterator: Iterator[Row],
                    schema: StructType,
                    batchSize: Int,
                    insertMethod: (SparkCHClientInsert, Row) => Unit): Int = {
    var client: SparkCHClientInsert = null
    try {
      client = new SparkCHClientInsert(CHSql.insertStmt(database, table), host, port)
      client.setBatch(batchSize)
      client.insertPrefix()
      var totalCount = 0
      while (iterator.hasNext) {
        val row = iterator.next()
        insertMethod(client, row)
        totalCount += 1
      }
      client.insertSuffix()
      totalCount
    } finally {
      if (client != null) {
        client.close()
      }
    }
  }

  def getPartitionList(table: CHTableRef): Array[String] = {
    val client = new SparkCHClientSelect(
      CHUtil.genQueryId("P"),
      CHSql.partitionList(table),
      table.host,
      table.port
    )
    try {
      var partitions = new Array[String](0)

      if (client.hasNext) {
        val block = client.next()
        if (block.numCols != 1) {
          throw new Exception("Send table partition list request, wrong response")
        }

        val fieldCol = block.column(0)
        for (i <- 0 until fieldCol.size()) {
          partitions :+= fieldCol.getUTF8String(i).toString
        }
      }

      partitions
    } finally {
      client.close()
    }
  }

  // TODO: Port to metadata scan
  // TODO: encapsulate scan operation
  def listTables(database: String, node: Node): Array[String] = {
    val client = new SparkCHClientSelect(
      CHUtil.genQueryId("LT"),
      CHSql.showTables(database),
      node.host,
      node.port
    )
    try {
      var tables = new Array[String](0)

      if (client.hasNext) {
        val block = client.next()
        if (block.numCols != 1) {
          throw new Exception("Send show table request, wrong response")
        }

        val fieldCol = block.column(0)
        for (i <- 0 until fieldCol.size()) {
          tables :+= fieldCol.getUTF8String(i).toString
        }
      }

      tables
    } finally {
      if (client != null) {
        client.close()
      }
    }
  }

  // TODO: Port to metadata scan
  // TODO: encapsulate scan operation
  def listDatabases(node: Node): Array[String] = {
    val client =
      new SparkCHClientSelect(CHUtil.genQueryId("LD"), CHSql.showDatabases(), node.host, node.port)
    try {
      var databases = new Array[String](0)

      if (client.hasNext) {
        val block = client.next()
        if (block.numCols != 1) {
          throw new Exception("Send show databases request, wrong response")
        }

        val fieldCol = block.column(0)
        for (i <- 0 until fieldCol.size()) {
          databases :+= fieldCol.getUTF8String(i).toString
        }
      }

      databases
    } finally {
      if (client != null) {
        client.close()
      }
    }
  }

  def getFields(table: CHTableRef): Array[StructField] = {
    val metadata = new MetadataBuilder().putString("name", table.mappedName).build()

    var fields = new Array[StructField](0)

    var names = new Array[String](0)
    var types = new Array[String](0)

    val client =
      new SparkCHClientSelect(CHUtil.genQueryId("D"), CHSql.desc(table), table.host, table.port)
    try {
      while (client.hasNext) {
        val block = client.next()

        if (block.numCols() < 2) {
          throw new Exception("Send desc table to get schema failed: small column size")
        }

        val fieldCol = block.column(0)
        for (i <- 0 until fieldCol.size()) {
          names :+= fieldCol.getUTF8String(i).toString
        }

        val typeCol = block.column(1)
        for (i <- 0 until typeCol.size()) {
          types :+= typeCol.getUTF8String(i).toString
        }
      }

      if (names.length == 0) {
        throw new Exception("Send desc table to get schema failed: table desc not found")
      }
      for (i <- names.indices) {
        val t = TypeMappingJava.stringToSparkType(types(i))
        val field = Hack.hackStructField(names(i), t, metadata)
        fields :+= field
      }

      fields
    } finally {
      if (client != null) {
        client.close()
      }
    }
  }

  def getRowCount(table: CHTableRef, useSelraw: Boolean = false): Long = {
    val client = new SparkCHClientSelect(
      CHUtil.genQueryId("C"),
      CHSql.count(table, useSelraw),
      table.host,
      table.port
    )
    try {
      if (!client.hasNext) {
        throw new Exception("Send table row count request, not response")
      }
      val block = client.next()
      if (block.numCols() != 1) {
        throw new Exception("Send table row count request, wrong response")
      }

      val count = block.column(0).getLong(0)

      count
    } finally {
      if (client != null) {
        client.close()
      }
    }
  }

  // TODO: Pushdown more.
  def isSupportedExpression(exp: Expression): Boolean = {
    // println("PROBE isSupportedExpression:" + exp.getClass.getName + ", " + exp)
    exp match {
      case _: Literal            => true
      case _: AttributeReference => true
      case cast @ Cast(child, _) =>
        Hack.hackSupportCast(cast).getOrElse(isSupportedExpression(child))
      case _: CreateNamedStruct => true
      // TODO: Don't pushdown IsNotNull maybe better
      case IsNotNull(child) =>
        isSupportedExpression(child)
      case IsNull(child) =>
        isSupportedExpression(child)
      case UnaryMinus(child) =>
        isSupportedExpression(child)
      case Not(child) =>
        isSupportedExpression(child)
      case Abs(child) =>
        isSupportedExpression(child)
      case Add(lhs, rhs) =>
        isSupportedExpression(lhs) && isSupportedExpression(rhs)
      case Subtract(lhs, rhs) =>
        isSupportedExpression(lhs) && isSupportedExpression(rhs)
      case Multiply(lhs, rhs) =>
        isSupportedExpression(lhs) && isSupportedExpression(rhs)
      case Divide(lhs, rhs) =>
        isSupportedExpression(lhs) && isSupportedExpression(rhs)
      case Remainder(lhs, rhs) =>
        isSupportedExpression(lhs) && isSupportedExpression(rhs)
      case GreaterThan(lhs, rhs) =>
        isSupportedExpression(lhs) && isSupportedExpression(rhs)
      case GreaterThanOrEqual(lhs, rhs) =>
        isSupportedExpression(lhs) && isSupportedExpression(rhs)
      case LessThan(lhs, rhs) =>
        isSupportedExpression(lhs) && isSupportedExpression(rhs)
      case LessThanOrEqual(lhs, rhs) =>
        isSupportedExpression(lhs) && isSupportedExpression(rhs)
      case EqualTo(lhs, rhs) =>
        isSupportedExpression(lhs) && isSupportedExpression(rhs)
      case And(lhs, rhs) =>
        isSupportedExpression(lhs) && isSupportedExpression(rhs)
      case Or(lhs, rhs) =>
        isSupportedExpression(lhs) && isSupportedExpression(rhs)
      case In(value, list) =>
        isSupportedExpression(value) && list.forall(isSupportedExpression)
      case ae @ AggregateExpression(_, _, _, _) =>
        isSupportedAggregateExpression(ae)
      case _ => false
    }
  }

  def isSupportedAggregateExpression(ae: AggregateExpression): Boolean = {
    // Should not support any AggregateExpression that has isDistinct = true,
    // because we have to unify results on different partitions.
    (ae.aggregateFunction, ae.isDistinct) match {
      case (_, true)            => false
      case (Average(child), _)  => isSupportedExpression(child)
      case (Count(children), _) => children.forall(isSupportedExpression)
      case (Min(child), _)      => isSupportedExpression(child)
      case (Max(child), _)      => isSupportedExpression(child)
      case (Sum(child), _)      => isSupportedExpression(child)
      case _                    => false
    }
  }

  def genQueryId(prefix: String): String = {
    this.synchronized {
      prefix + UUID.randomUUID.toString
    }
  }

}
