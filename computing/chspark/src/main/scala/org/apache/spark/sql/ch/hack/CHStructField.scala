package org.apache.spark.sql.ch.hack

import org.apache.spark.sql.types.{DataType, DateType, Metadata, StructField}

class CHStructField(name: String,
    dataType: DataType,
    nullable: Boolean = true,
    metadata: Metadata = Metadata.empty) extends StructField(name, dataType, nullable, metadata) {
}
