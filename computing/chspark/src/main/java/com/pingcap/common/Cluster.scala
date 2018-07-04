package com.pingcap.common

case class Node(host: String, port: Int) {}

case class Cluster(nodes: Array[Node]) {}

object Cluster {
  def getDefault: Cluster = {
    Cluster(Array(Node("127.0.0.1", 9000)))
  }
}