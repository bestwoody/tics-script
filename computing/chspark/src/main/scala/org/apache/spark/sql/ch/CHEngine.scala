package org.apache.spark.sql.ch

import com.pingcap.tikv.meta.TiTableInfo
import org.apache.spark.sql.catalyst.catalog.{CHCatalogConst, CatalogTable}
import org.apache.spark.sql.types.{Metadata, MetadataBuilder, StructField}

import scala.collection.JavaConversions._
import scala.collection.mutable

object CHEngine extends Enumeration {
  val MutableMergeTree, MergeTree, Log, _Unknown = Value

  def withNameSafe(str: String): Value = values.find(_.toString == str).getOrElse(_Unknown)

  def fromCatalogTable(tableDesc: CatalogTable): CHEngine =
    CHEngine.withNameSafe(tableDesc.properties(CHCatalogConst.TAB_META_ENGINE)) match {
      case CHEngine.MutableMergeTree =>
        val partitionNum =
          tableDesc.properties.get(CHCatalogConst.TAB_META_PARTITION_NUM).map(_.toInt)
        val pkList = tableDesc.schema
          .filter(
            f =>
              f.metadata.contains(CHCatalogConst.COL_META_PRIMARY_KEY) && f.metadata
                .getBoolean(CHCatalogConst.COL_META_PRIMARY_KEY)
          )
          .map(_.name)
        val bucketNum = tableDesc.properties(CHCatalogConst.TAB_META_BUCKET_NUM).toInt
        new MutableMergeTree(partitionNum, pkList, bucketNum)
      case CHEngine.Log => LogEngine()
      case other        => throw new RuntimeException(s"Invalid CH engine $other")
    }

  def fromTiTableInfo(tiTable: TiTableInfo, properties: Map[String, String]): CHEngine =
    CHEngine.withNameSafe(properties(CHCatalogConst.TAB_META_ENGINE)) match {
      case CHEngine.MutableMergeTree =>
        val partitionNum =
          properties.get(CHCatalogConst.TAB_META_PARTITION_NUM).map(_.toInt)
        val pkList = tiTable.getColumns.filter(_.isPrimaryKey).map(_.getName)
        val bucketNum = properties(CHCatalogConst.TAB_META_BUCKET_NUM).toInt
        new MutableMergeTree(partitionNum, pkList, bucketNum)
      case CHEngine.Log => LogEngine()
      case other        => throw new RuntimeException(s"Invalid CH engine $other")
    }

  def fromCreateStatement(stmt: String): CHEngine =
    CHEngine.withNameSafe(CHUtil.getTableEngine(stmt)) match {
      case CHEngine.MutableMergeTree =>
        val partitionNum = CHUtil.getPartitionNum(stmt).map(_.toInt)
        val pkList = CHUtil.getPrimaryKeys(stmt)
        val bucketNum = CHUtil.getBucketNum(stmt).toInt
        new MutableMergeTree(partitionNum, pkList, bucketNum)
      case CHEngine.Log => LogEngine()
      case other        => throw new RuntimeException(s"Invalid CH engine $other")
    }
}

abstract class CHEngine(val name: CHEngine.Value) {
  def sql: String

  def toProperties: mutable.Map[String, String]

  def mapFields(fields: Array[StructField]): Array[StructField]
}

case class LogEngine() extends CHEngine(CHEngine.Log) {
  override def sql: String = "Log"

  override def toProperties: mutable.Map[String, String] = mutable.Map[String, String](
    (CHCatalogConst.TAB_META_ENGINE, name.toString)
  )

  override def mapFields(fields: Array[StructField]): Array[StructField] = fields
}

case class MutableMergeTree(partitionNum: Option[Int], pkList: Seq[String], bucketNum: Int)
    extends CHEngine(CHEngine.MutableMergeTree) {
  override def sql: String =
    s"MutableMergeTree(${partitionNum.map(_.toString + ", ").getOrElse("")}$bucketNum)"

  override def toProperties: mutable.Map[String, String] = {
    val properties = mutable.Map[String, String](
      (CHCatalogConst.TAB_META_ENGINE, name.toString),
      (CHCatalogConst.TAB_META_BUCKET_NUM, bucketNum.toString)
    )
    partitionNum.foreach(p => properties.put(CHCatalogConst.TAB_META_PARTITION_NUM, p.toString))
    properties
  }

  override def mapFields(fields: Array[StructField]): Array[StructField] = {
    val buildMetadata = (name: String, metadata: Metadata) => {
      val builder = new MetadataBuilder()
        .withMetadata(metadata)
      if (pkList.contains(name)) {
        builder.putBoolean(CHCatalogConst.COL_META_PRIMARY_KEY, true)
      }
      builder.build()
    }

    fields.map(field => field.copy(metadata = buildMetadata(field.name, field.metadata)))
  }
}
