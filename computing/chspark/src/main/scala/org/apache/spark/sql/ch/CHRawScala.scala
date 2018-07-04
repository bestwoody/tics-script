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
    if (args.length < 1) {
      println("usage: <bin> query-sql [verb=0|1] [host] [port]")
      return
    }

    val query: String = args(0)
    val verb: Int = if (args.length >= 2) args(1).toInt else 2

    val host: String = if (args.length >= 3) args(2) else "127.0.0.1"
    val port: Int = if (args.length >= 4) args(3).toInt else 9000

    val rid = Random.nextInt
    val qid = "chraw-" + (if (rid < 0) -rid else rid)

    var totalRows: Long = 0
    var totalBlocks: Long = 0
    var totalBytes: Long = 0

    def addRows(n: Long, cb: Long): Unit = {
      totalRows += n
      totalBlocks += 1
      totalBytes += cb
    }

    if (verb >= 0) {
      println("Starting.")
    }
    val startTime = new Date()

    val client = new SparkCHClientSelect(qid, query, host, port)
    var n: Int = 0
    while (client.hasNext) {
      if (verb >= 1) {
        println(s"$n block")
      }
      val block = client.next
      val rows = block.chBlock().rowCount()
      addRows(rows, block.chBlock().byteCount())
      block.chBlock().free()
      n += 1
    }
    client.close()

    val endTime = new Date()
    val elapsed = endTime.getTime - startTime.getTime
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("mm:ss")

    if (verb >= 0) {
      println(
        "All finish, total rows: " + totalRows + ", blocks: " + totalBlocks + ", bytes: " + totalBytes
      )
      println(
        "Elapsed: " + dateFormat
          .format(elapsed) + ", mb/s: " + (totalBytes.toDouble / 1000 / elapsed)
      )
    }
  }
}
