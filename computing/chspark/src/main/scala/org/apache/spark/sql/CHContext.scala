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

import com.pingcap.common.{Cluster, Node}
import com.pingcap.theflash.SparkCHClientInsert
import com.pingcap.tispark.TiSessionCache
import com.pingcap.tispark.listener.CacheInvalidateListener
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.ch.CHUtil.{Partitioner, PrimaryKey}
import org.apache.spark.sql.ch._
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._

class CHContext(val sparkSession: SparkSession) extends Serializable with Logging {
  lazy val sqlContext: SQLContext = sparkSession.sqlContext

  val tiContext: TiContext = new TiContext(sparkSession)

  /**
   * Concrete CH catalog.
   */
  lazy val chConcreteCatalog: CHSessionCatalog =
    new CHConcreteSessionCatalog(this)(new CHDirectExternalCatalog(this))

  /**
   * Legacy catalog.
   */
  lazy val legacyCatalog: SessionCatalog = sqlContext.sessionState.catalog

  /**
   * Root catalog, always be composite catalog.
   */
  lazy val chCatalog: CHSessionCatalog = new CHCompositeSessionCatalog(this)

  val tiSession = tiContext.tiSession

  CacheInvalidateListener
    .initCacheListener(sparkSession.sparkContext, tiSession.getRegionManager)
  tiSession.injectCallBackFunc(CacheInvalidateListener.getInstance())

  lazy val cluster: Cluster = {
    val clusterStr = sparkSession.conf.get(CHConfigConst.CLUSTER_ADDRESSES, "")
    if (clusterStr.isEmpty) {
      Cluster.getDefault
    } else {
      val nodes = clusterStr
        .split(",")
        .map(nodeStr => {
          val nodeParts = nodeStr.split(":")
          if (nodeParts.length != 2) {
            throw new IllegalArgumentException(s"wrong format for cluster configuration $nodeStr")
          }
          val host = nodeParts(0)
          val port = Integer.parseInt(nodeParts(1))
          Node(host, port)
        })
      new Cluster(nodes)
    }
  }

  // TODO: Needs to hook in catalog after 2.3 port
  def listDatabases(): Array[String] =
    CHUtil.listDatabases(cluster.nodes.head)

  // TODO: Needs to hook in catalog after 2.3 port
  def listTables(database: String): Array[String] =
    CHUtil.listTables(database, cluster.nodes.head)

  def dropTable(database: String, table: String, ifExists: Boolean = true): Unit =
    CHUtil.dropTable(database, table, cluster, ifExists)

  def dropDatabase(database: String, ifExists: Boolean = true): Unit =
    CHUtil.dropDatabase(database, cluster, ifExists)

  def createDatabase(database: String, ifNotExists: Boolean = true): Unit =
    CHUtil.createDatabase(database, cluster, ifNotExists)

  def createTableFromTiDB(
    database: String,
    table: String,
    partitionNum: Option[Int] = Some(CHCatalogConst.DEFAULT_PARTITION_NUM),
    bucketNum: Int = CHCatalogConst.DEFAULT_BUCKET_NUM,
    batchRows: Long = SparkCHClientInsert.STORAGE_BATCH_INSERT_COUNT_ROWS,
    batchBytes: Long = SparkCHClientInsert.STORAGE_BATCH_INSERT_COUNT_BYTES
  ): Unit = {
    val tableInfo = tiContext.meta.getTable(database, table)
    if (tableInfo.isEmpty) {
      throw new IllegalArgumentException(s"Table $table not exists")
    }
    val df = tiContext.getDataFrame(database, table)
    // Tables created through CHContext APIs are now fixed as using MMT.
    val mmt = MutableMergeTreeEngine(
      partitionNum,
      tableInfo.get.getColumns.filter(_.isPrimaryKey).map(_.getName),
      bucketNum
    )
    CHUtil.createTable(database, tableInfo.get, mmt, false, cluster)

    Partitioner.fromTiTableInfo(tableInfo.get) match {
      case Partitioner(Partitioner.Hash, keyIndex) =>
        CHUtil.insertDataHash(
          df,
          database,
          table,
          keyIndex,
          fromTiDB = true,
          sqlContext.conf
            .getConfString(
              CHConfigConst.CLIENT_BATCH_SIZE,
              SparkCHClientInsert.CLIENT_BATCH_INSERT_COUNT.toString
            )
            .toInt,
          batchRows,
          batchBytes,
          cluster
        )
      case Partitioner(Partitioner.Random, _) =>
        CHUtil.insertDataRandom(
          df,
          database,
          table,
          fromTiDB = true,
          sqlContext.conf
            .getConfString(
              CHConfigConst.CLIENT_BATCH_SIZE,
              SparkCHClientInsert.CLIENT_BATCH_INSERT_COUNT.toString
            )
            .toInt,
          batchRows,
          batchBytes,
          cluster
        )
    }
  }

  def createTableFromDataFrame(
    database: String,
    table: String,
    primaryKeys: Array[String],
    df: DataFrame,
    partitionNum: Option[Int] = Some(CHCatalogConst.DEFAULT_PARTITION_NUM),
    bucketNum: Int = CHCatalogConst.DEFAULT_BUCKET_NUM,
    batchRows: Long = SparkCHClientInsert.STORAGE_BATCH_INSERT_COUNT_ROWS,
    batchBytes: Long = SparkCHClientInsert.STORAGE_BATCH_INSERT_COUNT_BYTES
  ): Unit = {
    // Tables created through CHContext APIs are now fixed as using MMT.
    val mmt = MutableMergeTreeEngine(partitionNum, primaryKeys, bucketNum)
    CHUtil.createTable(database, table, df.schema, mmt, false, cluster)

    Partitioner.fromPrimaryKeys(PrimaryKey.fromSchema(df.schema, primaryKeys)) match {
      case Partitioner(Partitioner.Hash, keyIndex) =>
        CHUtil.insertDataHash(
          df,
          database,
          table,
          keyIndex,
          false,
          sqlContext.conf
            .getConfString(
              CHConfigConst.CLIENT_BATCH_SIZE,
              SparkCHClientInsert.CLIENT_BATCH_INSERT_COUNT.toString
            )
            .toInt,
          batchRows,
          batchBytes,
          cluster
        )
      case Partitioner(Partitioner.Random, _) =>
        CHUtil.insertDataRandom(
          df,
          database,
          table,
          fromTiDB = false,
          sqlContext.conf
            .getConfString(
              CHConfigConst.CLIENT_BATCH_SIZE,
              SparkCHClientInsert.CLIENT_BATCH_INSERT_COUNT.toString
            )
            .toInt,
          batchRows,
          batchBytes,
          cluster
        )
    }
  }

  def mapCHTable(database: String = null,
                 table: String,
                 partitionsPerSplit: Int = CHConfigConst.DEFAULT_PARTITIONS_PER_SPLIT): Unit = {
    val tableRef = new CHTableRef(cluster.nodes.head, database, table)
    val rel = CHRelation(Array(tableRef), partitionsPerSplit)(sqlContext, this)
    sqlContext.baseRelationToDataFrame(rel).createTempView(tableRef.mappedName)
  }

  def mapCHClusterTable(
    database: String = null,
    table: String,
    partitionsPerSplit: Int = CHConfigConst.DEFAULT_PARTITIONS_PER_SPLIT
  ): Unit = {
    val tableRefList: Array[CHTableRef] =
      cluster.nodes.map(node => new CHTableRef(node, database, table))
    val rel = CHRelation(tableRefList, partitionsPerSplit)(sqlContext, this)
    sqlContext.baseRelationToDataFrame(rel).createTempView(tableRefList.head.mappedName)
  }

  def mapCHClusterTableSimple(
    database: String = null,
    table: String,
    partitionsPerSplit: Int = CHConfigConst.DEFAULT_PARTITIONS_PER_SPLIT
  ): Unit = {
    val tableRefList: Array[CHTableRef] =
      cluster.nodes.map(node => new CHTableRef(node, database, table))
    val rel = CHRelation(tableRefList, partitionsPerSplit)(sqlContext, this)
    sqlContext.baseRelationToDataFrame(rel).createTempView(tableRefList.head.mappedName)
  }

  def sql(sqlText: String): DataFrame =
    sqlContext.sql(sqlText)

  import java.sql.{Connection, DriverManager}

  def updateSample(df: DataFrame, table: String, primaryKeys: Array[String]): Unit = {
    val schema = df.schema
    df.foreachPartition { iterator =>
      {
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://127.0.0.1:4000/test?rewriteBatchedStatements=true"
        val username = "root"
        val password = ""
        var conn: Connection = null
        try {
          // make the connection
          Class.forName(driver)
          conn = DriverManager.getConnection(url, username, password)
          val pkMap = primaryKeys.map(_.toLowerCase()).toSet

          // create the statement, and run the select query
          val sql = new StringBuilder
          sql.append(s"UPDATE `$table` SET ")
          val values = schema.fields.filter(f => !pkMap.contains(f.name))
          val keys = schema.fields.filter(f => pkMap.contains(f.name))
          val fieldOffsetMap = schema.fields.map(f => f.name).zipWithIndex.toMap
          sql.append(values.map(f => s"`${f.name}` = ?").mkString(","))
          sql.append(" WHERE ")
          sql.append(keys.map(f => s"`${f.name}` = ?").mkString(" AND "))

          val ps = conn.prepareStatement(sql.toString())
          val setter = (row: Row, statementOffset: Int, rowOffset: Int, dataType: DataType) => {
            dataType match {
              case ShortType =>
                if (row.isNullAt(rowOffset)) {
                  ps.setNull(statementOffset, java.sql.Types.SMALLINT)
                } else {
                  ps.setShort(statementOffset, row.getShort(rowOffset))
                }
              case IntegerType =>
                if (row.isNullAt(rowOffset)) {
                  ps.setNull(statementOffset, java.sql.Types.INTEGER)
                } else {
                  ps.setInt(statementOffset, row.getInt(rowOffset))
                }
              case LongType =>
                if (row.isNullAt(rowOffset)) {
                  ps.setNull(statementOffset, java.sql.Types.BIGINT)
                } else {
                  ps.setLong(statementOffset, row.getLong(rowOffset))
                }
              case FloatType =>
                if (row.isNullAt(rowOffset)) {
                  ps.setNull(statementOffset, java.sql.Types.FLOAT)
                } else {
                  ps.setFloat(statementOffset, row.getFloat(rowOffset))
                }
              case DoubleType =>
                if (row.isNullAt(rowOffset)) {
                  ps.setNull(statementOffset, java.sql.Types.DOUBLE)
                } else {
                  ps.setDouble(statementOffset, row.getDouble(rowOffset))
                }
              case StringType =>
                if (row.isNullAt(rowOffset)) {
                  ps.setNull(statementOffset, java.sql.Types.VARCHAR)
                } else {
                  ps.setString(statementOffset, row.getString(rowOffset))
                }
              case ByteType =>
                if (row.isNullAt(rowOffset)) {
                  ps.setNull(statementOffset, java.sql.Types.TINYINT)
                } else {
                  ps.setByte(statementOffset, row.getByte(rowOffset))
                }
              case BooleanType =>
                if (row.isNullAt(rowOffset)) {
                  ps.setNull(statementOffset, java.sql.Types.BOOLEAN)
                } else {
                  ps.setBoolean(statementOffset, row.getBoolean(rowOffset))
                }
              case BinaryType =>
                if (row.isNullAt(rowOffset)) {
                  ps.setNull(statementOffset, java.sql.Types.BLOB)
                } else {
                  ps.setBytes(statementOffset, row.getAs[Array[Byte]](rowOffset))
                }
              case DateType =>
                if (row.isNullAt(rowOffset)) {
                  ps.setNull(statementOffset, java.sql.Types.DATE)
                } else {
                  ps.setDate(statementOffset, row.getDate(rowOffset))
                }
              case TimestampType =>
                if (row.isNullAt(rowOffset)) {
                  ps.setNull(statementOffset, java.sql.Types.TIMESTAMP)
                } else {
                  ps.setTimestamp(statementOffset, row.getTimestamp(rowOffset))
                }
              case _: DecimalType =>
                if (row.isNullAt(rowOffset)) {
                  ps.setNull(statementOffset, java.sql.Types.DECIMAL)
                } else {
                  ps.setBigDecimal(statementOffset, row.getDecimal(rowOffset))
                }
              case _ => throw new IllegalArgumentException(s"error type $dataType")
            }
          }

          var rowCount = 0
          while (iterator.hasNext) {
            val row = iterator.next()
            var statementOffset = 1
            (values ++ keys).foreach { field =>
              setter(row, statementOffset, fieldOffsetMap(field.name), field.dataType)
              statementOffset += 1
            }
            rowCount += 1
            ps.addBatch()
            if (rowCount % 500 == 0) {
              ps.executeBatch()
              rowCount = 0
            }
          }
          if (rowCount != 0) {
            ps.executeBatch()
          }
        } finally {
          if (conn != null) {
            conn.close()
          }
        }
      }
    }
  }
}
