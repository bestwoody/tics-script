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
  def hackColumnName(name: String, dataType: DataType): Option[String] =
    Option.empty

  def hackColumnDef(name: String, dataType: DataType, nullable: Boolean): Option[String] = {
    var chType: CHType = null
    return Option.empty
  }

  def hackSparkColumnData(name: String,
                          chType: CHType,
                          row: Row,
                          index: Int,
                          col: CHColumn): Boolean =
    false

  def hackStructField(name: String,
                      chType: DataTypeAndNullable,
                      metadata: Metadata): Option[StructField] =
    Option.empty

  def hackAttributeReference(attr: AttributeReference): Option[String] = attr match {
    case _ => Option.empty
  }

  def hackSupportCast(cast: Cast): Option[Boolean] = cast match {
    case _ => None
  }
}
