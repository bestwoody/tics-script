/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.ch


object CHRaw {
  def main(args: Array[String]) {
    if (args.size < 3) {
      println("usage: <bin> query-sql partitions conc [host] [port]")
      return;
    }

    val query: String = args(0)
    val partitions: Int = args(1).toInt
    val conc: Int = args(2).toInt

    val host: String = if (args.size >= 4) {
      args(3)
    } else {
      "127.0.0.1"
    }

    val port: Int = if (args.size >= 5) {
      args(4).toInt
    } else {
      9006
    }

    val workers = new Array[Thread](partitions);

    var totalRows: Long = 0

    def addRows(n: Long): Unit = {
      this.synchronized {
        totalRows += n
      }
    }

    for (i <- 0 until partitions) {
      workers(i) = new Thread {
        println("#" + i + " start")
        val resp = CHExecutorPool.get(query, host, port, "", conc, false)
        var block = resp.executor.next
        while (block != null) {
          val columns = block.decoded.block.getFieldVectors
          val rows: Long = if (columns.isEmpty) { 0 } else { columns.get(0).getAccessor().getValueCount }
          addRows(rows)
          block.close
          block = resp.executor.next
        }
        CHExecutorPool.close(resp)
        println("#" + i + " finish")
      }
    }

    workers.foreach(_.join)
    println("All finish, total rows: " + totalRows)
  }
}
