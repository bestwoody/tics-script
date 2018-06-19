package org.apache.spark.sql.ch.hack

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.{StructField, StructType}

class CHStructType(fields: Array[StructField]) extends StructType(fields) {
  override protected[sql] def toAttributes: Seq[AttributeReference] =
    map(f => f match {
      case df: CHStructField => new CHAttributeReference(df.name, df.dataType, df.nullable, df.metadata)
      case _ => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()
      }
    )
}
