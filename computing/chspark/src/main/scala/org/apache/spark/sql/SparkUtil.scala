package org.apache.spark.sql

import org.apache.spark.sql.types.{StructField, StructType}

object SparkUtil {
  def setNullableStateOfColumns(df: DataFrame, nullable: Boolean, cns: Array[String]): DataFrame = {
    val schema = df.schema
    // modify [[StructField] with name `cn`
    val newSchema = StructType(schema.map {
      case StructField(c, t, _, m) if cns.contains(c) => StructField(c, t, nullable = nullable, m)
      case y: StructField                             => y
    })
    // apply new schema
    df.sqlContext.createDataFrame(df.rdd, newSchema)
  }
}
