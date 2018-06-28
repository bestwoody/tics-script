package org.apache.spark.sql.ch.hack

import com.pingcap.theflash.DataTypeAndNullable
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast}
import org.apache.spark.sql.types._

object Hack {
  final val TIDB_DATE_PREFIX = "_tidb_date_"

  def hackStructField(name: String, chType: DataTypeAndNullable, metadata: Metadata): StructField = {
    if (name.startsWith(TIDB_DATE_PREFIX) && (chType.dataType == IntegerType)) {
      return new CHStructField(name.replaceFirst(TIDB_DATE_PREFIX, ""), DateType, chType.nullable, metadata)
    }
    StructField(name, chType.dataType, chType.nullable, metadata)
  }

  def hackAttributeReference(attr: AttributeReference): String = attr match {
    case CHAttributeReference(name, _, _, _, _, _, _) => TIDB_DATE_PREFIX + name
    case _ => attr.name
  }

  def hackSupportCast(cast: Cast): Option[Boolean] = cast match {
    // Any casting to date is not supported as CH date is too narrow.
    case Cast(_, DateType) => Some(false)
    // Any casting tidb date to string or timestamp is not supported as tidb date is represented as Int32.
    case Cast(CHAttributeReference(_, dataType, _, _, _, _, _), targetType) => (dataType, targetType) match {
      case (DateType, StringType) => Some(false)
      case (DateType, TimestampType) => Some(false)
    }
    case _ => None
  }
}
