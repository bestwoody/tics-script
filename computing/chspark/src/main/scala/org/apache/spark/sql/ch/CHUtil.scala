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

import java.util.{Objects, UUID}
import java.util.concurrent.ConcurrentHashMap

import com.pingcap.common.{Cluster, Node}
import com.pingcap.theflash.{SparkCHClientInsert, SparkCHClientSelect, TypeMappingJava}
import com.pingcap.tikv.meta.{TiTableInfo, TiTimestamp}
import com.pingcap.common.IOUtil
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Abs, Add, Alias, And, AttributeReference, CaseWhen, Cast, Coalesce, CreateNamedStruct, Divide, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, IfNull, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, Multiply, Not, Or, Remainder, StringLPad, StringRPad, StringTrim, StringTrimLeft, StringTrimRight, Subtract, UnaryMinus}
import org.apache.spark.sql.ch.CHUtil.SharedSparkCHClientInsert.Identity
import org.apache.spark.sql.types._
import org.apache.spark.sql.{CHContext, DataFrame, Row}

import scala.collection.JavaConversions._

object CHUtil {
  case class PrimaryKey(column: StructField, index: Int) {}

  object PrimaryKey {
    def fromSchema(schema: StructType, primaryKeys: Seq[String]): Seq[PrimaryKey] =
      schema.zipWithIndex
        .filter {
          case (col, _) => primaryKeys.map(_.toLowerCase).contains(col.name.toLowerCase)
        }
        .map {
          case (col, i) => PrimaryKey(col, i)
        }
  }

  case class Partitioner(method: Partitioner.Value, keyIndex: Int = -1) {}

  object Partitioner extends Enumeration {
    val Hash, Random = Value

    def fromPrimaryKeys(primaryKeys: Seq[PrimaryKey]): Partitioner =
      if (primaryKeys.length == 1) {
        val primaryKey = primaryKeys.head
        if (primaryKey.column.dataType.isInstanceOf[IntegralType]) {
          Partitioner(Hash, primaryKey.index)
        } else {
          Partitioner(Random)
        }
      } else {
        Partitioner(Random)
      }

    def fromCHTableInfo(tableInfo: TableInfo): Partitioner = tableInfo.engine match {
      case MutableMergeTreeEngine(_, pkList, _) =>
        if (pkList.size == 1 && tableInfo.schema(pkList.head).dataType.isInstanceOf[IntegralType]) {
          Partitioner(Hash, tableInfo.schema.getFieldIndex(pkList.head).get)
        } else {
          Partitioner(Random)
        }
      case LogEngine() => Partitioner(Random)
      case TxnMergeTreeEngine(_, _, _, _) =>
        throw new RuntimeException("Partitioner for TMT engine not supported")
    }

    def fromTiTableInfo(tiTableInfo: TiTableInfo): Partitioner =
      if (tiTableInfo.isPkHandle) {
        val (_, index) = tiTableInfo.getColumns.zipWithIndex.filter {
          case (c, _) => c.isPrimaryKey
        }.head
        Partitioner(Hash, index)
      } else {
        Partitioner(Random)
      }
  }

  def createTable(database: String,
                  table: String,
                  schema: StructType,
                  chEngine: CHEngine,
                  ifNotExists: Boolean,
                  cluster: Cluster): Unit =
    try {
      cluster.nodes.foreach(
        node => createTable(database, table, schema, chEngine, ifNotExists, node)
      )

    } catch {
      // roll back if any exception
      case e: Throwable =>
        cluster.nodes.foreach(node => tryDropTable(database, table, node))
        throw e
    }

  private def createTable(database: String,
                          table: String,
                          schema: StructType,
                          chEngine: CHEngine,
                          ifNotExists: Boolean,
                          node: Node): Unit = {
    var client: SparkCHClientSelect = null
    try {
      val queryString = CHSql.createTableStmt(database, table, schema, chEngine, ifNotExists)
      client = new SparkCHClientSelect(queryString, node.host, node.port)
      while (client.hasNext) {
        client.next()
      }
    } finally {
      IOUtil.closeQuietly(client)
    }
  }

  def createTable(database: String,
                  table: TiTableInfo,
                  chEngine: CHEngine,
                  ifNotExists: Boolean,
                  cluster: Cluster): Unit =
    try {
      cluster.nodes.foreach(
        node => createTable(database, table, chEngine, ifNotExists, node)
      )
    } catch {
      // roll back if any exception
      case e: Throwable =>
        cluster.nodes.foreach(node => tryDropTable(database, table.getName, node))
        throw e
    }

  private def createTable(database: String,
                          table: TiTableInfo,
                          chEngine: CHEngine,
                          ifNotExists: Boolean,
                          node: Node): Unit = {
    var client: SparkCHClientSelect = null
    try {
      val queryString =
        CHSql.createTableStmt(database, table, chEngine, ifNotExists)
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

  def dropTable(database: String, table: String, cluster: Cluster, ifExists: Boolean = true): Unit =
    try {
      cluster.nodes.foreach(node => dropTable(database, table, node, ifExists))
    } catch {
      case e: Throwable =>
        // try best to drop as many as possible
        // TODO: try avoid partially drop table
        cluster.nodes.foreach(node => tryDropTable(database, table, node))
        throw e
    }

  def createDatabase(database: String, cluster: Cluster, ifNotExists: Boolean = true): Unit =
    try {
      cluster.nodes.foreach(node => createDatabase(database, node, ifNotExists))
    } catch {
      case e: Throwable =>
        // try best to drop as many as possible
        // TODO: try avoid partially drop table
        cluster.nodes.foreach(node => tryDropDatabase(database, node))
        throw e
    }

  def dropDatabase(database: String, cluster: Cluster, ifExists: Boolean = true): Unit =
    try {
      cluster.nodes.foreach(node => dropDatabase(database, node, ifExists))
    } catch {
      case e: Throwable => throw e
    }

  private def tryDropDatabase(database: String, node: Node) =
    try {
      dropDatabase(database, node, true)
    } catch {
      case _: Throwable => // ignore
    }

  private def tryDropTable(database: String, table: String, node: Node) =
    try {
      dropTable(database, table, node, true)
    } catch {
      case _: Throwable => // ignore
    }

  private def dropTable(database: String, table: String, node: Node, ifExists: Boolean): Unit = {
    val queryString = CHSql.dropTableStmt(database, table, ifExists)
    var client: SparkCHClientSelect = null
    try {
      client = new SparkCHClientSelect(queryString, node.host, node.port)
      while (client.hasNext) {
        client.next()
      }
    } finally {
      IOUtil.closeQuietly(client)
    }
  }

  private def createDatabase(database: String, node: Node, ifNotExists: Boolean): Unit = {
    val queryString = CHSql.createDatabaseStmt(database, ifNotExists)
    var client: SparkCHClientSelect = null
    try {
      client = new SparkCHClientSelect(queryString, node.host, node.port)
      while (client.hasNext) {
        client.next()
      }
    } finally {
      IOUtil.closeQuietly(client)
    }
  }

  private def dropDatabase(database: String, node: Node, ifExists: Boolean): Unit = {
    val queryString = CHSql.dropDatabaseStmt(database, ifExists)
    var client: SparkCHClientSelect = null
    try {
      client = new SparkCHClientSelect(queryString, node.host, node.port)
      while (client.hasNext) {
        client.next()
      }
    } finally {
      IOUtil.closeQuietly(client)
    }
  }

  def insertDataHash(df: DataFrame,
                     database: String,
                     table: String,
                     keyOffset: Int,
                     fromTiDB: Boolean,
                     clientBatchSize: Int,
                     storageBatchRows: Long,
                     storageBatchBytes: Long,
                     cluster: Cluster): Unit = {
    val nodeNum = cluster.nodes.length
    val hash = (row: Row) =>
      (row.get(keyOffset).asInstanceOf[Number].longValue() % nodeNum).asInstanceOf[Int]
    val rddWithKey = df.rdd.keyBy(hash)
    val schema = df.schema

    rddWithKey.foreachPartition { iter =>
      savePartition(
        database,
        table,
        iter,
        schema,
        fromTiDB,
        clientBatchSize,
        storageBatchRows,
        storageBatchBytes,
        cluster
      )
    }
  }

  def insertDataRandom(df: DataFrame,
                       database: String,
                       table: String,
                       fromTiDB: Boolean,
                       clientBatchSize: Int,
                       storageBatchRows: Long,
                       storageBatchBytes: Long,
                       cluster: Cluster): Unit = {
    val schema = df.schema
    val insertMethod: (SparkCHClientInsert, Row) => Unit =
      if (fromTiDB) { (client: SparkCHClientInsert, row: Row) =>
        client.insertFromTiDB(row)
      } else { (client: SparkCHClientInsert, row: Row) =>
        client.insert(row)
      }

    df.rdd
      .mapPartitionsWithIndex { (index, iterator) =>
        {
          val node = cluster.nodes(index % cluster.nodes.length)
          List(
            savePartition(
              database,
              table,
              iterator,
              schema,
              insertMethod,
              clientBatchSize,
              storageBatchRows,
              storageBatchBytes,
              node.host,
              node.port
            )
          ).iterator
        }
      }
      .collect()
  }

  /**
   * An intra-JVM per-CH-node shared CH insert client whose life-cycle is managed by an internal reference count.
   * One should ALWAYS create/destroy an instance through methods of its companion object.
   * @param identity
   */
  case class SharedSparkCHClientInsert(private val identity: Identity) extends Logging {
    private var client: SparkCHClientInsert = _
    private var refCount = 0
    // Our shared CH insert client doesn't support retry (consider a successful task's data may be still in client's cache,
    // this task won't be retried once this client's following insertion fails).
    // Using this flag to prevent any task retrying.
    // Note that there is a side-effect that, as it doesn't exist an accountable point inside a task's life-cycle to clear this flag
    // (i.e. the task doesn't know if it is the first nor last one within this JVM), once it is invalid, it will be forever,
    // until the executor is restarted. That saying one should restart the application (i.e. Spark Shell) once this flag is invalid.
    // TODO: consider using a timer to clear this flag after a while since last touch.
    private var valid = true

    /**
     * Acquire this shared client by increasing the ref count.
     * Create and prepare the internal client on first acquirement.
     * Set invalid on any exception.
     */
    private def acquire(): Unit = this.synchronized {
      assertValid()
      if (refCount == 0) {
        try {
          client =
            new SparkCHClientInsert(identity.insertStmt, identity.node.host, identity.node.port)
          client.setClientBatch(identity.clientBatchSize)
          client.setStorageBatchRows(identity.storageBatchRows)
          client.setStorageBatchBytes(identity.storageBatchBytes)
          client.insertPrefix()
        } catch {
          case t: Throwable => invalidate(t)
        }
      }
      refCount += 1
    }

    /**
     * Release this shared client by decreasing the ref count.
     * Send EOF and close the internal client on last releasing.
     * Set invalid on any exception.
     * @return whether this client should be freed
     */
    private def release(): Boolean = this.synchronized {
      assertValid()
      refCount -= 1
      if (refCount == 0) {
        try {
          client.insertSuffix()
          close()
        } catch {
          case t: Throwable => invalidate(t)
        }
      }
      refCount == 0
    }

    /**
     * Fully synchronized insertion as the internal client is not thread-safe.
     * Set invalid on any exception.
     * @param row
     */
    private def insert(row: Row): Unit = this.synchronized {
      assertValid()
      try {
        if (identity.fromTiDB) {
          client.insertFromTiDB(row)
        } else {
          client.insert(row)
        }
      } catch {
        case t: Throwable => invalidate(t)
      }
    }

    private def assertValid(): Unit = if (!valid) {
      throw new RuntimeException(
        s"$this is invalid due to error in other task. Check log for previous error detail."
      )
    }

    private def invalidate(t: Throwable): Unit = {
      valid = false
      logError(s"$this hit error: ", t)
      close()
      throw t
    }

    private def close(): Unit =
      IOUtil.closeQuietly(client)

    override def toString: String =
      s"SharedSparkCHClientInsert(identity = $identity, refCount = $refCount, valid = $valid)"
  }

  /**
   * Entry point of using shared CH insert clients.
   */
  object SharedSparkCHClientInsert extends Logging {

    /**
     * Signature of a shared CH insert client.
     * @param insertStmt
     * @param fromTiDB
     * @param clientBatchSize
     * @param storageBatchRows
     * @param storageBatchBytes
     * @param node
     */
    case class Identity(insertStmt: String,
                        fromTiDB: Boolean,
                        clientBatchSize: Int,
                        storageBatchRows: Long,
                        storageBatchBytes: Long,
                        node: Node) {
      override def hashCode(): Int =
        Objects
          .hash(insertStmt, node) + fromTiDB.hashCode() + clientBatchSize
          .hashCode() + storageBatchRows.hashCode() + storageBatchBytes.hashCode()

      override def equals(obj: scala.Any): Boolean = obj match {
        case other: Identity =>
          Objects.equals(other.insertStmt, this.insertStmt) &&
            other.fromTiDB == this.fromTiDB &&
            other.clientBatchSize == this.clientBatchSize &&
            other.storageBatchRows == this.storageBatchRows &&
            other.storageBatchBytes == this.storageBatchBytes &&
            Objects.equals(other.node, this.node)
        case _ =>
          false
      }

      override def toString: String =
        s"Identity(insertStmt = $insertStmt, fromTiDB = $fromTiDB, clientBatchSize = $clientBatchSize, storageBatchRows = $storageBatchRows, storageBatchBytes = $storageBatchBytes, node = $node)"
    }

    // For a new job, this value equals to the last job's stage id (or -1), as Spark increments stage id for every job.
    // It will be then set to the new job's stage id by the very first task (in this executor/JVM) of this job.
    // We use this chance to do an only-once (of all tasks of this job in this executor/JVM)
    // reset on the clients to clear previous errors so they won't block next run.
    // The reset tasks (in this executor/JVM) of this job will see this value as the new job's stage id and do nothing.
    private var currentStageId = -1

    // Global client registry, using concurrent map to allow inter-client level concurrent insertion.
    private val clients = new ConcurrentHashMap[Identity, SharedSparkCHClientInsert]()

    /**
     * Only-once (of all tasks of this job in this executor/JVM) reset on the clients
     * to clear previous errors so they won't block next run, by comparing and setting currentStageId.
     * @param stageId
     */
    def reset(stageId: Int): Unit = synchronized {
      if (currentStageId == stageId) {
        logDebug(
          s"No need to reset SharedSparkCHClientInsert: last stage id = $currentStageId, current stage id = $stageId"
        )
      } else {
        logDebug(
          s"Resetting SharedSparkCHClientInsert: last stage id = $currentStageId, current stage id = $stageId"
        )
        clients.foreach(_._2.close())
        clients.clear()
        currentStageId = stageId
      }
    }

    /**
     * Acquire a shared CH insert client, create one if none.
     * And increase the ref count.
     * No concurrency.
     * @param identity
     */
    def acquire(identity: Identity): Unit = this.synchronized {
      clients.putIfAbsent(identity, SharedSparkCHClientInsert(identity))
      clients(identity).acquire()
      logDebug(s"Acquired SharedSparkCHClientInsert: ${clients.get(identity)}")
    }

    /**
     * Release a shared CH insert client, remove it if ref count is zero.
     * No concurrency.
     * @param identity
     */
    def release(identity: Identity): Unit = this.synchronized {
      logDebug(s"Releasing SharedSparkCHClientInsert: ${clients.get(identity)}")
      if (clients(identity).release()) {
        clients.remove(identity)
      }
    }

    /**
     * Insertion allowing inter-client level concurrency while intra-client insertion is still synchronized.
     * @param identity
     * @param row
     */
    def insert(identity: Identity, row: Row): Unit =
      clients(identity).insert(row)
  }

  // Do a one to one partition insertion
  def savePartition(database: String,
                    table: String,
                    iterator: Iterator[(Int, Row)],
                    schema: StructType,
                    fromTiDB: Boolean,
                    clientBatchSize: Int,
                    storageBatchRows: Long,
                    storageBatchBytes: Long,
                    cluster: Cluster): Int = {
    val stageId = TaskContext.get().stageId()
    SharedSparkCHClientInsert.reset(stageId)

    val insertStmt = CHSql.insertStmt(database, table)
    // TODO: could do sampling here to estimate a proper clientBatchSize/storageBatchRows/storageBatchBytes.
    val identities =
      cluster.nodes.map(
        Identity(insertStmt, fromTiDB, clientBatchSize, storageBatchRows, storageBatchBytes, _)
      )
    var totalCount = 0
    identities.foreach(SharedSparkCHClientInsert.acquire)
    while (iterator.hasNext) {
      val res = iterator.next()
      val idx = res._1
      val row = res._2
      SharedSparkCHClientInsert.insert(identities(idx), row)
      totalCount += 1
    }
    identities.foreach(SharedSparkCHClientInsert.release)
    totalCount
  }

  // Do a one to one partition insertion
  def savePartition(database: String,
                    table: String,
                    iterator: Iterator[Row],
                    schema: StructType,
                    insertMethod: (SparkCHClientInsert, Row) => Unit,
                    clientBatchSize: Int,
                    storageBatchRows: Long,
                    storageBatchBytes: Long,
                    host: String,
                    port: Int): Int = {
    var client: SparkCHClientInsert = null
    try {
      // TODO: could do sampling here to estimate a proper clientBatchSize/storageBatchRows/storageBatchBytes.
      client = new SparkCHClientInsert(CHSql.insertStmt(database, table), host, port)
      client.setClientBatch(clientBatchSize)
      client.setStorageBatchRows(storageBatchRows)
      client.setStorageBatchBytes(storageBatchBytes)
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
      IOUtil.closeQuietly(client)
    }
  }

  def getPartitionList(table: CHTableRef): Array[String] = {
    val client = new SparkCHClientSelect(
      CHUtil.genQueryId("P"),
      CHSql.partitionList(table),
      table.node.host,
      table.node.port
    )
    try {
      var partitions = new Array[String](0)

      while (client.hasNext) {
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
      IOUtil.closeQuietly(client)
    }
  }

  def getTableEngine(table: CHTableRef): String = {
    val client = new SparkCHClientSelect(
      CHUtil.genQueryId("E"),
      CHSql.tableEngine(table),
      table.node.host,
      table.node.port
    )
    try {
      if (!client.hasNext) {
        throw new Exception("Send table engine request, no response")
      }
      val block = client.next()
      if (block.numCols() != 1) {
        throw new Exception("Send table engine request, wrong response")
      }

      val engineUTFStr = block.column(0).getUTF8String(0)

      if (engineUTFStr == null) {
        throw new Exception("engine is null")
      }
      val engine = engineUTFStr.toString

      // Consume all data.
      while (client.hasNext) {
        client.next()
      }

      engine
    } finally {
      IOUtil.closeQuietly(client)
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

      while (client.hasNext) {
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
      IOUtil.closeQuietly(client)
    }
  }

  // TODO: Port to metadata scan
  // TODO: encapsulate scan operation
  def listDatabases(node: Node): Array[String] = {
    val client =
      new SparkCHClientSelect(CHUtil.genQueryId("LD"), CHSql.showDatabases(), node.host, node.port)
    try {
      var databases = new Array[String](0)

      while (client.hasNext) {
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
      IOUtil.closeQuietly(client)
    }
  }

  def truncateTable(tableName: TableIdentifier, cluster: Cluster): Unit =
    cluster.nodes.foreach(node => truncateTable(tableName, node))

  def truncateTable(tableName: TableIdentifier, node: Node): Unit = {
    val queryString = CHSql.truncateTable(tableName.database.get, tableName.table)
    var client: SparkCHClientSelect = null
    try {
      client = new SparkCHClientSelect(queryString, node.host, node.port)
      while (client.hasNext) {
        client.next()
      }
    } finally {
      IOUtil.closeQuietly(client)
    }
  }

  def getFields(table: CHTableRef): Array[StructField] = {
    var fields = new Array[StructField](0)

    var names = new Array[String](0)
    var types = new Array[String](0)

    val client =
      new SparkCHClientSelect(
        CHUtil.genQueryId("D"),
        CHSql.desc(table),
        table.node.host,
        table.node.port
      )
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
        val field = StructField(names(i), t.dataType, t.nullable)
        fields :+= field
      }

      fields
    } finally {
      IOUtil.closeQuietly(client)
    }
  }

  def getShowCreateTable(table: CHTableRef): String = {
    val client = new SparkCHClientSelect(
      CHUtil.genQueryId("SCT"),
      CHSql.showCreateTable(table),
      table.node.host,
      table.node.port
    )
    try {
      if (!client.hasNext) {
        throw new Exception("Send show create table request, not response")
      }
      val block = client.next()
      if (block.numCols() != 1) {
        throw new Exception("Send show create table request, wrong response")
      }

      val showCreateTable = block.column(0).getUTF8String(0).toString

      // Consume all data.
      while (client.hasNext) {
        client.next()
      }

      showCreateTable
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
      table.node.host,
      table.node.port
    )
    try {
      if (!client.hasNext) {
        throw new Exception("Send table row count request, no response")
      }
      val block = client.next()
      if (block.numCols() != 1) {
        throw new Exception("Send table row count request, wrong response")
      }

      val count = block.column(0).getLong(0)

      // Consume all data.
      while (client.hasNext) {
        client.next()
      }

      count
    } finally {
      IOUtil.closeQuietly(client)
    }
  }

  // TODO: Pushdown more.
  def isSupportedExpression(exp: Expression): Boolean =
    // println("PROBE isSupportedExpression:" + exp.getClass.getName + ", " + exp)
    exp match {
      case _: Literal            => true
      case _: AttributeReference => true
      case Cast(child, _, _)     => isSupportedExpression(child)
      case _: CreateNamedStruct  => false
      case _ @Alias(child, _) =>
        isSupportedExpression(child)
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
        !rhs.canonicalized.equals(Literal(0)) && isSupportedExpression(lhs) && isSupportedExpression(
          rhs
        )
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
      case ce @ CaseWhen(_, _) =>
        isSupportedCaseWhenExpression(ce)
      case IfNull(lhs, rhs, _) =>
        isSupportedExpression(lhs) && isSupportedExpression(rhs)
      case Coalesce(children) =>
        children.forall(isSupportedExpression)
      case ae: AggregateExpression =>
        isSupportedAggregateExpression(ae)
      case StringTrim(src, trimStr) =>
        isSupportedExpression(src) && trimStr.forall(isSupportedExpression)
      case StringTrimLeft(src, trimStr) =>
        isSupportedExpression(src) && trimStr.forall(isSupportedExpression)
      case StringTrimRight(src, trimStr) =>
        isSupportedExpression(src) && trimStr.forall(isSupportedExpression)
      case StringLPad(str, len, pad) =>
        isSupportedExpression(str) && isSupportedExpression(len) && isSupportedExpression(pad)
      case StringRPad(str, len, pad) =>
        isSupportedExpression(str) && isSupportedExpression(len) && isSupportedExpression(pad)
      case _ => false
    }

  def isSupportedCaseWhenExpression(ce: CaseWhen): Boolean =
    !ce.branches.exists {
      case (branch, value) => !isSupportedExpression(branch) || !isSupportedExpression(value)
    } && (ce.elseValue.isEmpty || isSupportedExpression(ce.elseValue.get))

  def isSupportedAggregateExpression(ae: AggregateExpression): Boolean =
    // Should not support any AggregateExpression that has isDistinct = true,
    // because we have to unify results on different partitions.
    ae.aggregateFunction match {
      case _ if ae.isDistinct => false
      case Sum(child) =>
        child.dataType match {
          case DecimalType() => false
          case _             => isSupportedExpression(child)
        }
      case Average(_) =>
        throw new UnsupportedOperationException(s"Unexpected ${ae.toString} found.")
      case PromotedSum(_) | Count(_) | Min(_) | Max(_) =>
        ae.aggregateFunction.children.forall(isSupportedExpression)
      case _ => false
    }

  def genQueryId(prefix: String): String =
    this.synchronized {
      prefix + UUID.randomUUID.toString
    }

}
