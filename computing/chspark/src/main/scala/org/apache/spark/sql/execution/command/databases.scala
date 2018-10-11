package org.apache.spark.sql.execution.command

import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.{CHContext, Row, SparkSession}

case class CHShowDatabasesCommand(chContext: CHContext, delegate: ShowDatabasesCommand)
    extends CHDelegateCommand(delegate) {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val databases =
      // Not leveraging catalog-specific db pattern, at least Hive and Spark behave different than each other.
      delegate.databasePattern
        .map(StringUtils.filterPattern(chCatalog.listDatabases(), _))
        .getOrElse(chCatalog.listDatabases())
    databases.map { d =>
      Row(d)
    }
  }
}

case class CHSetDatabaseCommand(chContext: CHContext, delegate: SetDatabaseCommand)
    extends CHDelegateCommand(delegate) {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    chCatalog.setCurrentDatabase(delegate.databaseName)
    Seq.empty[Row]
  }
}
