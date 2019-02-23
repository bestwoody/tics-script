package org.apache.spark.sql.ch

import com.pingcap.tikv.meta.TiTableInfo
import org.apache.spark.sql.catalyst.catalog.{CHCatalogConst, CatalogTable}
import org.apache.spark.sql.types.{Metadata, MetadataBuilder, StructField}

import scala.collection.JavaConversions._
import scala.collection.mutable

object CHEngine extends Enumeration {
  val MutableMergeTree, TxnMergeTree, MergeTree, Log = Value

  private def getCHEngine(name: String): CHEngine =
    CHEngine.withName(name) match {
      case CHEngine.MutableMergeTree =>
        MutableMergeTreeEngine(None, Seq.empty[String], -1)
      case CHEngine.TxnMergeTree =>
        TxnMergeTreeEngine(None, Seq.empty[String], -1, "")
      case CHEngine.Log =>
        LogEngine()
    }

  def fromCatalogTable(tableDesc: CatalogTable): CHEngine = {
    val chEngine = getCHEngine(tableDesc.properties(CHCatalogConst.TAB_META_ENGINE))
    chEngine.fromCatalogTable(tableDesc)
    chEngine
  }

  def fromTiTableInfo(tiTable: TiTableInfo, properties: Map[String, String]): CHEngine = {
    val chEngine = getCHEngine(properties(CHCatalogConst.TAB_META_ENGINE))
    chEngine.fromTiTableInfo(tiTable, properties)
    chEngine
  }

  def fromCreateStatement(stmt: String): CHEngine = {
    val chEngine = getCHEngine(getTableEngine(stmt))
    chEngine.fromCreateStatement(stmt)
    chEngine
  }

  def getTableEngine(stmt: String): String = {
    val start: Int = stmt.lastIndexOf("ENGINE = ") + 9
    var end: Int = stmt.indexOf("(", start)
    if (end == -1) {
      end = stmt.length
    }
    stmt.substring(start, end)
  }

  def getTableEngineArgs(stmt: String): Seq[String] = {
    var args = Seq.empty[String]

    val engineStart: Int = stmt.lastIndexOf("ENGINE = ") + 9
    var start: Int = stmt.indexOf("(", engineStart)
    if (start == -1) {
      return args
    }
    start += 1
    val end = stmt.lastIndexOf(")")
    while (start < end) {
      val c = stmt(start)
      if (c.isLetterOrDigit || c == '_') {
        var sep = stmt.indexOf(",", start)
        if (sep == -1) {
          sep = end
        }
        args = args :+ stmt.substring(start, sep)
        start = sep + 2
      } else if (c == '(') {
        val rightParen = stmt.indexOf(")", start)
        args = args :+ stmt.substring(start + 1, rightParen)
        start = rightParen + 3
      } else if (c == '\'' || c == '`') {
        val quoteChar = c
        var i = start + 1
        while (i < end && stmt(i) != quoteChar) {
          if (stmt(i) == '\\') {
            i += 2
          } else {
            i += 1
          }
        }
        if (i == end) {
          throw new RuntimeException("Bad create statement format")
        }
        val rightQuote = i
        args = args :+ stmt.substring(start + 1, rightQuote)
        start = rightQuote + 3
      }
    }
    args
  }
}

abstract class CHEngine(val name: CHEngine.Value) {
  def sql: String

  def toProperties: mutable.Map[String, String]

  def fromCatalogTable(tableDesc: CatalogTable): Unit

  def fromTiTableInfo(tiTable: TiTableInfo, properties: Map[String, String]): Unit

  def fromCreateStatement(stmt: String): Unit

  def mapFields(fields: Array[StructField]): Array[StructField]
}

case class LogEngine() extends CHEngine(CHEngine.Log) {
  override def sql: String = "Log"

  override def toProperties: mutable.Map[String, String] = mutable.Map[String, String](
    (CHCatalogConst.TAB_META_ENGINE, name.toString)
  )

  override def fromCatalogTable(tableDesc: CatalogTable): Unit = ()

  override def fromTiTableInfo(tiTable: TiTableInfo, properties: Map[String, String]): Unit = ()

  override def fromCreateStatement(stmt: String): Unit = ()

  override def mapFields(fields: Array[StructField]): Array[StructField] = fields
}

case class MutableMergeTreeEngine(var partitionNum: Option[Int],
                                  var pkList: Seq[String],
                                  var bucketNum: Int)
    extends CHEngine(CHEngine.MutableMergeTree) {
  private val MMT_TAB_META_PARTITION_NUM = "mmt_partition_num"
  private val MMT_TAB_META_BUCKET_NUM = "mmt_bucket_num"

  override def sql: String =
    s"MutableMergeTree(${partitionNum.map(_.toString + ", ").getOrElse("")}$bucketNum)"

  override def toProperties: mutable.Map[String, String] = {
    val properties = mutable.Map[String, String](
      (CHCatalogConst.TAB_META_ENGINE, name.toString),
      (MMT_TAB_META_BUCKET_NUM, bucketNum.toString)
    )
    partitionNum.foreach(p => properties.put(MMT_TAB_META_PARTITION_NUM, p.toString))
    properties
  }

  override def fromCatalogTable(tableDesc: CatalogTable): Unit = {
    partitionNum = tableDesc.properties.get(MMT_TAB_META_PARTITION_NUM).map(_.toInt)
    pkList = tableDesc.schema
      .filter(
        f =>
          f.metadata.contains(CHCatalogConst.COL_META_PRIMARY_KEY) && f.metadata
            .getBoolean(CHCatalogConst.COL_META_PRIMARY_KEY)
      )
      .map(_.name)
    bucketNum = tableDesc.properties(MMT_TAB_META_BUCKET_NUM).toInt
  }

  override def fromTiTableInfo(tiTable: TiTableInfo, properties: Map[String, String]): Unit = {
    partitionNum = properties.get(MMT_TAB_META_PARTITION_NUM).map(_.toInt)
    pkList = tiTable.getColumns.filter(_.isPrimaryKey).map(_.getName)
    bucketNum = properties(MMT_TAB_META_BUCKET_NUM).toInt
  }

  override def fromCreateStatement(stmt: String): Unit = {
    val args = CHEngine.getTableEngineArgs(stmt)
    var i = 0
    if (args.size == 3) {
      partitionNum = Some(args.head.toInt)
      i += 1
    } else {
      partitionNum = None
    }
    pkList = args(i).split(" *, *").map(_.replaceAll("`", "").trim)
    i += 1
    bucketNum = args(i).toInt
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

case class TxnMergeTreeEngine(var partitionNum: Option[Int],
                              var pkList: Seq[String],
                              var bucketNum: Int,
                              var tableInfo: String)
    extends CHEngine(CHEngine.TxnMergeTree) {
  private val TMT_TAB_META_PARTITION_NUM = "tmt_partition_num"
  private val TMT_TAB_META_BUCKET_NUM = "tmt_bucket_num"
  private val TMT_TAB_META_TABLE_INFO = "tmt_table_info"

  override def sql: String =
    s"TxnMergeTree(${partitionNum.map(_.toString + ", ").getOrElse("")}$bucketNum, '$tableInfo')"

  override def toProperties: mutable.Map[String, String] = {
    val properties = mutable.Map[String, String](
      (CHCatalogConst.TAB_META_ENGINE, name.toString),
      (TMT_TAB_META_BUCKET_NUM, bucketNum.toString),
      (TMT_TAB_META_TABLE_INFO, tableInfo)
    )
    partitionNum.foreach(p => properties.put(TMT_TAB_META_PARTITION_NUM, p.toString))
    properties
  }

  override def fromCatalogTable(tableDesc: CatalogTable): Unit = {
    partitionNum = tableDesc.properties.get(TMT_TAB_META_PARTITION_NUM).map(_.toInt)
    pkList = tableDesc.schema
      .filter(
        f =>
          f.metadata.contains(CHCatalogConst.COL_META_PRIMARY_KEY) && f.metadata
            .getBoolean(CHCatalogConst.COL_META_PRIMARY_KEY)
      )
      .map(_.name)
    bucketNum = tableDesc.properties(TMT_TAB_META_BUCKET_NUM).toInt
    tableInfo = tableDesc.properties(TMT_TAB_META_TABLE_INFO)
  }

  override def fromTiTableInfo(tiTable: TiTableInfo, properties: Map[String, String]): Unit = {
    partitionNum = properties.get(TMT_TAB_META_PARTITION_NUM).map(_.toInt)
    pkList = tiTable.getColumns.filter(_.isPrimaryKey).map(_.getName)
    bucketNum = properties(TMT_TAB_META_BUCKET_NUM).toInt
    tableInfo = properties(TMT_TAB_META_TABLE_INFO)
  }

  override def fromCreateStatement(stmt: String): Unit = {
    val args = CHEngine.getTableEngineArgs(stmt)
    var i = 0
    if (args.size == 4) {
      partitionNum = Some(args.head.toInt)
      i += 1
    } else {
      partitionNum = None
    }
    pkList = args(i).split(" *, *").map(_.replaceAll("`", "").trim)
    i += 1
    bucketNum = args(i).toInt
    i += 1
    tableInfo = args(i)
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
