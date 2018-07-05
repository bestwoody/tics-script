package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.ch.{CHRelation, CHTableRef}

class CHTestContext(sparkSession: SparkSession) extends CHContext(sparkSession) {
  def mapCHDatabase(database: String = "default", partitionsPerSplit: Int = 16): Unit = {
    val conf: SparkConf = sparkSession.sparkContext.conf

    val tableList = listTables(database)

    val tableRefList: Array[Array[CHTableRef]] =
      tableList.map { table =>
        cluster.nodes.map(node => new CHTableRef(node.host, node.port, database, table))
      }
    val rel = tableRefList.map { new CHRelation(_, partitionsPerSplit)(sqlContext, conf) }
    for (i <- rel.indices) {
      sqlContext
        .baseRelationToDataFrame(rel(i))
        .createOrReplaceTempView(tableRefList(i).head.mappedName)
    }
  }
}
