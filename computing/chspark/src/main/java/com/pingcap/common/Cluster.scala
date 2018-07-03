package com.pingcap.common

case class Node(val host: String, val port: Int) {}

case class Cluster(val nodes: Array[Node]) {}

object Cluster {
  def getDefault(): Cluster = {
    return new Cluster(Array(new Node("127.0.0.1", 9000)))
  }
}