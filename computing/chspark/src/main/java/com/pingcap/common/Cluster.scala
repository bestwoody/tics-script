package com.pingcap.common

import org.apache.spark.sql.ch.CHTableRef
import java.util.Objects

case class Node(host: String, port: Int) {
  override def hashCode(): Int = Objects.hash(host) + port.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: Node =>
      Objects.equals(other.host, this.host) && other.port == this.port
    case _ =>
      false
  }

  override def toString: String = s"Node($host:$port)"
}

case class Cluster(nodes: Array[Node]) {}

object Cluster {
  def getDefault: Cluster = {
    Cluster(Array(Node("127.0.0.1", 9000)))
  }

  def ofCHTableRefs(tables: Array[CHTableRef]): Cluster = {
    Cluster(tables.map(_.node))
  }
}
