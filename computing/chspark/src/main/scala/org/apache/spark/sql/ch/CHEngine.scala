package org.apache.spark.sql.ch

import com.pingcap.tikv.meta.TiTableInfo
import org.apache.spark.sql.catalyst.catalog.{CHCatalogConst, CatalogTable}

import scala.collection.JavaConversions._

object CHEngine extends Enumeration {
  val MutableMergeTree, MergeTree, _Unknown = Value

  def withNameSafe(str: String): Value = values.find(_.toString == str).getOrElse(_Unknown)

  def fromCatalogTable(tableDesc: CatalogTable): CHEngine =
    CHEngine.withNameSafe(tableDesc.properties(CHCatalogConst.TAB_META_ENGINE)) match {
      case CHEngine.MutableMergeTree =>
        val partitionNum =
          tableDesc.properties.get(CHCatalogConst.TAB_META_PARTITION_NUM).map(_.toInt)
        val pkList = tableDesc.schema
          .filter(_.metadata.getBoolean(CHCatalogConst.COL_META_PRIMARY_KEY))
          .map(_.name)
        val bucketNum = tableDesc.properties(CHCatalogConst.TAB_META_BUCKET_NUM).toInt
        new MutableMergeTree(partitionNum, pkList, bucketNum)
      case other => throw new RuntimeException(s"Invalid CH engine $other")
    }

  def fromTiTableInfo(tiTable: TiTableInfo, properties: Map[String, String]): CHEngine =
    CHEngine.withNameSafe(properties(CHCatalogConst.TAB_META_ENGINE)) match {
      case CHEngine.MutableMergeTree =>
        val partitionNum =
          properties.get(CHCatalogConst.TAB_META_PARTITION_NUM).map(_.toInt)
        val pkList = tiTable.getColumns.filter(_.isPrimaryKey).map(_.getName)
        val bucketNum = properties(CHCatalogConst.TAB_META_BUCKET_NUM).toInt
        new MutableMergeTree(partitionNum, pkList, bucketNum)
      case other => throw new RuntimeException(s"Invalid CH engine $other")
    }
}

abstract class CHEngine(val name: CHEngine.Value) {
  def sql: String
}

case class MutableMergeTree(partitionNum: Option[Int], pkList: Seq[String], bucketNum: Int)
    extends CHEngine(CHEngine.MutableMergeTree) {
  override def sql: String =
    s"MutableMergeTree(${partitionNum.map(_.toString + ", ").getOrElse("")}$bucketNum)"
}
