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

object CHRawReader {
  def splitQueryForPartitioning(query: String): (String, String) = {
    val lq = query.toLowerCase()
    val i1 = lq.indexOf("where")
    val i2 = lq.lastIndexOf("where")
    if (i1 < 0) {
      (query, "")
    } else if (i1 == i2) {
      (query.substring(0, i1), " " + query.substring(i1))
    } else {
      throw new Exception("Unsupport two 'where' in a query")
    }
  }

  // TODO: can be more balance
  def getPartitionsSql(partitions: Array[String], threads: Int): Array[String] = {
    if (partitions.size < threads) {
      throw new Exception("Too many threads: " + threads + ", partitions: " + partitions.size)
    }
    val partitionsPerThread = partitions.size / threads
    var threadPartitions: Array[Array[String]] = new Array[Array[String]](threads)
    var i = 0
    while (i + 1 < threads) {
      threadPartitions(i) = new Array[String](0)
      var n = 0
      while (n < partitionsPerThread) {
        threadPartitions(i) :+= partitions(i * partitionsPerThread + n)
        n += 1
      }
      i += 1
    }
    threadPartitions(i) = new Array[String](0)
    var n = i * partitionsPerThread
    while (n < partitions.size) {
      threadPartitions(i) :+= partitions(n)
      n += 1
    }

    threadPartitions.map(_.mkString("(", ",", ")"))
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      println(
        "usage: <bin> database table [threads=4] [query-sql] [verb=2] [host=127.0.0.1] [port=9000]"
      )
      return
    }

    val database: String = args(0)
    val table: String = args(1)
    val threads: Int = if (args.length >= 3 && !args(2).isEmpty) args(2).toInt else 9000
    var sql: String = if (args.length >= 4 && !args(3).isEmpty) { args(3) } else {
      "SELECT * FROM " + database + "." + table
    }
    val query = splitQueryForPartitioning(sql)
    val verb: Int = if (args.length >= 5 && !args(4).isEmpty) args(4).toInt else 2
    val host: String = if (args.length >= 6 && !args(5).isEmpty) args(5) else "127.0.0.1"
    val port: Int = if (args.length >= 7 && !args(6).isEmpty) args(6).toInt else 9000

    val tableRef: CHTableRef = new CHTableRef(host, port, database, table)
    val partitions = CHUtil.getPartitionList(tableRef).map("'" + _ + "'")
    if (partitions.isEmpty) {
      throw new Exception("Unable to get partition list of " + database + "." + table)
    }

    var queries = getPartitionsSql(partitions, threads).map(query._1 + " PARTITION " + _ + query._2)

    var totalRows: Long = 0
    var totalBlocks: Long = 0
    var totalBytes: Long = 0

    object locker;
    def addRows(n: Long, cb: Long): Unit =
      locker.synchronized {
        totalRows += n
        totalBlocks += 1
        totalBytes += cb
      }

    if (verb > 0) {
      println(
        "=> Config\n"
          + "Database: " + database + "\n"
          + "Table: " + table + "\n"
          + "Threads: " + threads + "\n"
          + "---\n"
          + "Verb: " + verb + "\n"
          + "Host: " + host + "\n"
          + "Port: " + port + "\n"
      )
      println("=> Starting")
    }
    val startTime = new Date()

    val workers = new Array[Thread](threads)
    for (i <- 0 until threads) {
      workers(i) = new Thread {
        override def run {
          if (verb > 0) {
            println("Thread #" + i + " [" + new Date() + "] start, query: " + queries(i))
          }
          val client = new SparkCHClientSelect(queries(i), host, port)
          var n: Int = 0
          while (client.hasNext) {
            if (verb > 1) {
              println("Thread #" + i + " block " + n)
            }
            val block = client.next
            val rows = block.chBlock().rowCount()
            addRows(rows, block.chBlock().byteCount())
            block.chBlock().free()
            n += 1
          }
          client.close()
          if (verb > 0) {
            println("Thread #" + i + " [" + new Date() + "] done")
          }
        }
      }
    }

    workers.foreach(_.start)
    workers.foreach(_.join)

    val endTime = new Date()
    val elapsed = endTime.getTime - startTime.getTime
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("mm:ss")

    if (verb > 0) {
      println(
        "\n=> Finished\n"
          + "Elapsed: " + dateFormat.format(elapsed) + "\n"
          + "Total rows: " + totalRows + "\n"
          + "Total blocks: " + totalBlocks + "\n"
          + "Total bytes: " + totalBytes + "\n"
          + "---\n"
          + "Rows/s: " + (totalRows * 1000 / elapsed).toLong + "\n"
          + "MB/s: " + totalBytes.toDouble / 1000 / elapsed
      )
    }
  }
}
