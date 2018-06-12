package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.ch.{CHRelation, CHTableRef}

class CHTestContext(sparkSession: SparkSession) extends CHContext(sparkSession) {
  def mapCHDatabase(addresses: Seq[(String, Int)] = Seq(("127.0.0.1", 9000)),
                    database: String = null,
                    tables: List[String] = null,
                    partitions: Int = 16,
                    decoders: Int = 1,
                    encoders: Int = 0): Unit = {
    val conf: SparkConf = sparkSession.sparkContext.conf

    val tableList = database match {
      case "default" => List("full_data_type_table")
      case _ if tables == null => throw new RuntimeException("Unable to identify tables")
      case _ => tables
    }
    val tableRefList: List[Seq[CHTableRef]] =
      tableList.map{ table => addresses.map(addr => new CHTableRef(addr._1, addr._2, database, table)) }
    val rel = tableRefList.map{ new CHRelation(_, partitions)(sqlContext, conf) }
    for (i <- rel.indices) {
      sqlContext.baseRelationToDataFrame(rel(i)).createOrReplaceTempView(tableRefList(i).head.mappedName)
    }
  }
}
