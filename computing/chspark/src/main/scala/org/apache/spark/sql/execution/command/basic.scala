package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{CHContext, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CHSessionCatalog
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Base class of CH commands. Can be directly derived by CH-only command classes.
 */
abstract class CHCommand extends RunnableCommand {
  val chContext: CHContext
  val chCatalog: CHSessionCatalog = chContext.chCatalog
}

/**
 * Base class of CH commands that are derived from Spark commands.
 * It is made into a delegate pattern as:
 * 1. Spark catalyst heavily assumes a command class to be a case class;
 * 2. Case class inheritance is obsolete in Scala.
 * One can inherit this class and override methods casually as most methods are already delegated.
 *
 * @param delegate
 * @param chContext
 */
abstract class CHDelegateCommand(delegate: RunnableCommand) extends CHCommand {
  override def output: Seq[Attribute] = delegate.output

  override def children: Seq[LogicalPlan] = delegate.children

  override def run(sparkSession: SparkSession): Seq[Row] = delegate.run(sparkSession)
}
