package org.apache.spark.sql.util

import com.pingcap.ch.CHBlock
import com.pingcap.theflash.TypeMappingJava
import org.apache.spark.sql.types.{StructType, _}

import scala.collection.JavaConverters._

object TypeMapping {

  def chSchemaToSparkSchema(chBlock: CHBlock): StructType =
    StructType(chBlock.columns.asScala.map { f =>
      val dt = TypeMappingJava.chTypetoSparkType(f.dataType)
      StructField(f.name, dt.dataType, dt.nullable)
    })

}
