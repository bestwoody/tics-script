package org.apache.spark.sql.extensions

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

case class CreateFlashDatabase(databaseName: String, ifNotExists: Boolean) extends LogicalPlan {
  override def children: Seq[LogicalPlan] = Seq.empty[LogicalPlan]
  override def output: Seq[Attribute] = Seq.empty
  override lazy val resolved: Boolean = false
}

case class CreateFlashTable(tableDesc: CatalogTable,
                            query: Option[LogicalPlan],
                            ignoreIfExists: Boolean)
    extends LogicalPlan {
  override def children: Seq[LogicalPlan] = query.toSeq
  override def output: Seq[Attribute] = Seq.empty
  override lazy val resolved: Boolean = false
}

case class CreateFlashTableFromTiDB(tiTable: TableIdentifier,
                                    properties: Map[String, String],
                                    ifNotExists: Boolean)
    extends LogicalPlan {
  override def children: Seq[LogicalPlan] = Seq.empty[LogicalPlan]
  override def output: Seq[Attribute] = Seq.empty
  override lazy val resolved: Boolean = false
}

case class LoadDataFromTiDB(tiTable: TableIdentifier, isOverwrite: Boolean) extends LogicalPlan {
  override def children: Seq[LogicalPlan] = Seq.empty[LogicalPlan]
  override def output: Seq[Attribute] = Seq.empty
  override lazy val resolved: Boolean = false
}
