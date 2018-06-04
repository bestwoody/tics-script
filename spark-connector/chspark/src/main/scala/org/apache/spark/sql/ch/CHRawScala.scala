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
import java.util.concurrent.atomic.AtomicLong

import com.pingcap.theflash.SparkCHClientSelect

import scala.util.Random

object CHRawScala {
  def main(args: Array[String]) {
    if (args.size < 2) {
      println("usage: <bin> query-sql partitions [verb=0|1|2] [host] [port]")
      return
    }

    val query: String = args(0)
    val partitions: Int = args(1).toInt

    val verb: Int = if (args.size >= 3) args(2).toInt else 2

    val host: String = if (args.size >= 4) args(3) else "127.0.0.1"
    val port: Int = if (args.size >= 5) args(4).toInt else 9000

    val rid = Random.nextInt
    val qid = "chraw-" + (if (rid < 0) -rid else rid)

    val workers = new Array[Thread](partitions);

    var totalRows: AtomicLong = new AtomicLong(0)
    var totalBlocks: AtomicLong = new AtomicLong(0)
    var totalBytes: AtomicLong = new AtomicLong(0)

    def addRows(n: Long, cb: Long): Unit = {
      totalRows.addAndGet(n)
      totalBlocks.incrementAndGet()
      totalBytes.addAndGet(cb)
    }

    if (verb >= 0) {
      println("Starting, partitions: " + partitions)
    }
    val startTime = new Date()

    for (i <- 0 until partitions) {
      workers(i) = new Thread {
        override def run {
          val client = new SparkCHClientSelect(qid, query, host, port, partitions, i)
          if (verb >= 1) {
            println("#" + i + " start")
          }
          var n: Int = 0
          while (client.hasNext) {
            if (verb >= 2) {
              println("#" + i + "@" + n + " block")
            }
            val block = client.next
            val rows = block.chBlock().rowCount()
            addRows(rows, block.chBlock().byteCount())
            block.chBlock().free()
            n += 1
          }
          client.close()
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
      println("Elapsed: " + dateFormat.format(elapsed) + ", mb/s: " + (totalBytes.get().toDouble / 1000 / elapsed))
    }
  }
}
