package org.apache.spark.sql.ch

import java.net.InetAddress

import com.pingcap.tikv.TiSession
import com.pingcap.tikv.expression.visitor.{MetaResolver, PrunedPartitionBuilder}
import com.pingcap.tikv.meta.TiTableInfo
import com.pingcap.tispark.BasicExpression
import org.apache.spark.sql.catalyst.catalog.{CHCatalogConst, CatalogTable}
import org.apache.spark.sql.types.{Metadata, MetadataBuilder, StructField}
import org.json4s.JsonAST.{JField, JInt, JObject}

import scala.collection.JavaConversions._
import scala.collection.mutable
import org.json4s._
import org.json4s.jackson.JsonMethods._

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

  /**
   * Engine-specific logic to generate CH physical plans (later to CH partitions) that are used in CHScanRDD.
   * @param chRelation
   * @param chLogicalPlan
   * @param tiSession
   * @return
   */
  def getPhysicalPlans(chRelation: CHRelation,
                       chLogicalPlan: CHLogicalPlan,
                       tiSession: TiSession): Array[CHPhysicalPlan]
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

  override def getPhysicalPlans(chRelation: CHRelation,
                                chLogicalPlan: CHLogicalPlan,
                                tiSession: TiSession): Array[CHPhysicalPlan] =
    chRelation.tables.map(
      table =>
        CHPhysicalPlan(
          table,
          CHSql.query(table, chLogicalPlan, null, chRelation.useSelraw),
          None,
          None
      )
    )
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

  override def getPhysicalPlans(chRelation: CHRelation,
                                chLogicalPlan: CHLogicalPlan,
                                tiSession: TiSession): Array[CHPhysicalPlan] =
    chRelation.tables.flatMap(table => {
      val partitionList = CHUtil.getPartitionList(table)
      partitionList
        .grouped(chRelation.partitionsPerSplit)
        .map(
          partitions =>
            CHPhysicalPlan(
              table,
              CHSql.query(table, chLogicalPlan, partitions, chRelation.useSelraw),
              None,
              None
          )
        )
    })
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

  private lazy val tableID: Long = {
    val parsed = parse(tableInfo)
    val ids: List[BigInt] = for {
      JObject(child) <- parsed
      JField("table_info", JObject(tableInfo)) <- child
      JField("id", JInt(id)) <- tableInfo
    } yield id
    if (ids.length != 1) {
      throw new Exception("Wrong table info json format " + tableInfo)
    }
    ids.head.longValue()
  }

  private def getPhysicalPlansByTableID(tableID: Long,
                                        tableName: String,
                                        chRelation: CHRelation,
                                        chLogicalPlan: CHLogicalPlan,
                                        tiSession: TiSession): Array[CHPhysicalPlan] = {
    // TODO: Doesn't support multiple TiFlash instances on single node with separate ports.
    // We'll need a decent way to identify a TiFlash instance, i.e. store ID or so.
    // Now hack by finding by host name only.
    val regionMap = CHUtil.getTableLearnerPeerRegionMap(tableID, tiSession)
    val hostRegionMap = regionMap.map(p => (InetAddress.getByName(p._1).getHostAddress, p._2))
    val hostTableMap =
      chRelation.tables.map(t => (InetAddress.getByName(t.node.host).getHostAddress, t)).toMap
    // Filter out (node, regions)s that don't have the table. Regions in such nodes would be all empty.
    val tableRegionMap =
      hostRegionMap.filter(p => hostTableMap.contains(p._1)).map(p => (hostTableMap(p._1), p._2))
    tableRegionMap
      .flatMap(p => {
        val regionList = p._2
        val table = CHTableRef(p._1.node, p._1.database, tableName)
        val query = CHSql.query(table, chLogicalPlan, null, chRelation.useSelraw)
        regionList
          .grouped(chRelation.partitionsPerSplit)
          .map(regionGroup => CHPhysicalPlan(table, query, chRelation.ts, Some(regionGroup)))
      })
      .toArray
  }

  override def getPhysicalPlans(chRelation: CHRelation,
                                chLogicalPlan: CHLogicalPlan,
                                tiSession: TiSession): Array[CHPhysicalPlan] = {
    val tiTableInfo =
      tiSession.getCatalog.getTable(chRelation.tables(0).database, chRelation.tables(0).table)

    if (tiTableInfo.isPartitionEnabled) {
      type TiExpression = com.pingcap.tikv.expression.Expression
      val tiFilters: Seq[TiExpression] = chLogicalPlan.chFilter.predicates.collect {
        case BasicExpression(expr) => expr
      }
      MetaResolver.resolve(tiFilters, tiTableInfo)
      val prunedPartBuilder = new PrunedPartitionBuilder()
      val prunedParts = prunedPartBuilder.prune(tiTableInfo, tiFilters)
      prunedParts
        .flatMap(partition => {
          val tableName = tiTableInfo.getName + "_" + partition.getId
          getPhysicalPlansByTableID(
            partition.getId,
            tableName,
            chRelation,
            chLogicalPlan,
            tiSession
          )
        })
        .toArray
    } else {
      getPhysicalPlansByTableID(tableID, tiTableInfo.getName, chRelation, chLogicalPlan, tiSession)
    }
  }
}
