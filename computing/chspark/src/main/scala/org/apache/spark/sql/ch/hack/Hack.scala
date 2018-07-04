package org.apache.spark.sql.ch.hack

import com.pingcap.ch.columns.CHColumn
import com.pingcap.ch.datatypes.{CHType, CHTypeString}
import com.pingcap.ch.datatypes.CHTypeNumber.CHTypeInt32
import com.pingcap.theflash.DataTypeAndNullable
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object Hack {
  final val TIDB_DATE_PREFIX = "_tidb_date_"
  private final val SECONDS_PER_DAY = 60 * 60 * 24L
  private final val MILLIS_PER_DAY = SECONDS_PER_DAY * 1000L

  def hackColumnName(name: String, dataType: DataType): Option[String] = {
    if (dataType == DateType) {
      Some(s"${(TIDB_DATE_PREFIX + name).toLowerCase()}")
    } else {
      Option.empty
    }
  }

  def hackColumnDef(name: String, dataType: DataType, nullable: Boolean): Option[String] = {
    var chType: CHType = null
    if (dataType == DateType) {
      if (nullable) {
        chType = CHTypeInt32.nullableInstance
      } else {
        chType = CHTypeInt32.instance
      }
    } else {
      return Option.empty
    }
    Some(s"`${hackColumnName(name, dataType).get}` ${chType.name()}")
  }

  def hackSparkColumnData(name: String,
                          chType: CHType,
                          row: Row,
                          index: Int,
                          col: CHColumn): Boolean = {
    if (name.startsWith(TIDB_DATE_PREFIX) && (chType == CHTypeInt32.instance || chType == CHTypeInt32.nullableInstance)) {
      val dt = row.getDate(index)
      col.insertInt((dt.getTime / MILLIS_PER_DAY).asInstanceOf[Int])
      true
    } else {
      false
    }
  }

  def hackStructField(name: String,
                      chType: DataTypeAndNullable,
                      metadata: Metadata): Option[StructField] = {
    if (name.startsWith(TIDB_DATE_PREFIX) && (chType.dataType == IntegerType)) {
      return Some(
        new CHStructField(
          name.replaceFirst(TIDB_DATE_PREFIX, ""),
          DateType,
          chType.nullable,
          metadata
        )
      )
    }
    Option.empty
  }

  def hackAttributeReference(attr: AttributeReference): Option[String] = attr match {
    case CHAttributeReference(name, _, _, _, _, _, _) => Some(TIDB_DATE_PREFIX + name)
    case _                                            => Option.empty
  }

  def hackSupportCast(cast: Cast): Option[Boolean] = cast match {
    // Any casting to date is not supported as CH date is too narrow.
    case Cast(_, DateType) => Some(false)
    // Any casting tidb date to string or timestamp is not supported as tidb date is represented as Int32.
    case Cast(CHAttributeReference(_, dataType, _, _, _, _, _), targetType) =>
      (dataType, targetType) match {
        case (DateType, StringType)    => Some(false)
        case (DateType, TimestampType) => Some(false)
      }
    case _ => None
  }
}
