package org.apache.spark.sql.execution.command

import org.apache.spark.sql.CHContext
import org.apache.spark.sql.catalyst.catalog.CHSessionCatalog

trait CHCommand {
  val chContext: CHContext
  val chCatalog: CHSessionCatalog = chContext.chCatalog
}
