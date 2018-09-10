package org.apache.spark.sql

import org.apache.spark.sql.ch.{CHConfigConst, CHRelation, CHTableRef}

class CHTestContext(sparkSession: SparkSession) extends CHContext(sparkSession) {
  def mapCHDatabase(database: String = "default",
                    partitionsPerSplit: Int = CHConfigConst.DEFAULT_PARTITIONS_PER_SPLIT): Unit = {
    val tableList = listTables(database)

    val tableRefList: Array[Array[CHTableRef]] =
      tableList.map { table =>
        cluster.nodes.map(node => new CHTableRef(node, database, table))
      }
    val rel = tableRefList.map { new CHRelation(_, partitionsPerSplit)(sqlContext) }
    for (i <- rel.indices) {
      sqlContext
        .baseRelationToDataFrame(rel(i))
        .createOrReplaceTempView(tableRefList(i).head.mappedName)
    }
  }
}
