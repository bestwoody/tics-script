package org.apache.spark.sql.execution.command

import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.{CHContext, Row, SparkSession}

class CHShowDatabasesCommand(val chContext: CHContext, databasePattern: Option[String])
    extends ShowDatabasesCommand(databasePattern)
    with CHCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val databases =
      // Not leveraging catalog-specific db pattern, at least Hive and Spark behave different than each other.
      databasePattern
        .map(StringUtils.filterPattern(chCatalog.listDatabases(), _))
        .getOrElse(chCatalog.listDatabases())
    databases.map { d =>
      Row(d)
    }
  }
}

case class CHSetDatabaseCommand(chContext: CHContext, databaseName: String)
    extends RunnableCommand
    with CHCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    chCatalog.setCurrentDatabase(databaseName)
    Seq.empty[Row]
  }
}
