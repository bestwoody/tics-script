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

import java.text.SimpleDateFormat
import java.util.Date

import scala.util.Random

object CHRawScala {
  def main(args: Array[String]) {
    if (args.size < 3) {
      println("usage: <bin> query-sql partitions decoders encoders [verb=0|1|2] [host] [port]")
      return;
    }

    val query: String = args(0)
    val partitions: Int = args(1).toInt
    val decoders: Int = args(2).toInt
    val encoders: Int = args(3).toInt

    val verb: Int = if (args.size >= 5) args(4).toInt else 2

    val host: String = if (args.size >= 6) args(5) else "127.0.0.1"
    val port: Int = if (args.size >= 7) args(6).toInt else 9006

    val rid = Random.nextInt
    val qid = "chraw-" + (if (rid < 0) -rid else rid)

    val workers = new Array[Thread](partitions);

    var totalRows: Long = 0
    var totalBlocks: Long = 0
    var totalBytes: Long = 0

    def addRows(n: Long, cb: Long): Unit = {
      this.synchronized {
        totalRows += n
        totalBlocks += 1
        totalBytes += cb
      }
    }

    if (verb >= 0) {
      println("Starting, partitions: " + partitions + ", decoders: " + decoders + ", encoders: " + encoders)
    }
    val startTime = new Date()

    for (i <- 0 until partitions) {
      workers(i) = new Thread {
        override def run {
          val resp = new CHExecutorParall(qid, query, host, port, "", decoders, encoders, partitions, i, false)
          if (verb >= 1) {
            println("#" + i + " start")
          }
          var n: Int = 0
          var block = resp.next
          while (block != null) {
            if (verb >= 2) {
              println("#" + i + "@" + n + " block")
            }
            val columns = block.decoded.block.getFieldVectors
            val rows: Long = if (columns.isEmpty) 0 else { columns.get(0).getValueCount }
            addRows(rows, block.dataSize)
            block.close
            block = resp.next
            n += 1
          }
          resp.close
          if (verb >= 1) {
            println("#" + i + " finish")
          }
        }
      }
      workers(i).start
    }

    workers.foreach(_.join)

    val endTime = new Date()
    val elapsed = endTime.getTime - startTime.getTime
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("mm:ss")

    if (verb >= 0) {
      println("All finish, total rows: " + totalRows + ", blocks: " + totalBlocks + ", bytes: " + totalBytes)
      println("Elapsed: " + dateFormat.format(elapsed) + ", bytes/s: " + (totalBytes * 1000 / elapsed))
    }
  }
}
