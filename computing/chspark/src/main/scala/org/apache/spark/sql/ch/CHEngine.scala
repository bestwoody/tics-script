package org.apache.spark.sql.ch

object CHEngine extends Enumeration {
  val MutableMergeTree, MergeTree, _Unknown = Value

  def withNameSafe(str: String): Value = values.find(_.toString == str).getOrElse(_Unknown)
}

abstract class CHEngine(val name: CHEngine.Value) {}

case class MutableMergeTree(partitionNum: Option[Int], pkList: Seq[String], bucketNum: Int)
    extends CHEngine(CHEngine.MutableMergeTree) {}
