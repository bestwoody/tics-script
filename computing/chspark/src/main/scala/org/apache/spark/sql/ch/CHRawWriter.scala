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

import com.pingcap.theflash.SparkCHClientSelect
import com.pingcap.theflash.SparkCHClientInsert

import com.pingcap.common.SimpleRow
import com.pingcap.ch.datatypes.CHType
import com.pingcap.ch.datatypes.CHTypeNumber
import com.pingcap.ch.datatypes.CHTypeString

private class CHRandRowGenerator(val dataTypes: Array[CHType],
                                 val threadIndex: Int = -1,
                                 val useSameValue: Boolean = false) {

  private var byteBase: Byte = if (threadIndex < 0) 0 else (threadIndex * 10).toByte
  private var shortBase: Short = if (threadIndex < 0) 0 else (threadIndex * 3000).toShort
  private var integerBase: Integer = if (threadIndex < 0) 0 else (threadIndex * 1000000)
  private var longBase: Long = if (threadIndex < 0) 0 else (threadIndex * 100000000).toLong
  private val stringBase: String = "the quick brown fox jumps over the lazy dog"

  def bytesOfRow(): Int = {
    var n = 0
    for (i: Int <- 0 until dataTypes.length) {
      val chType = dataTypes(i)
      if (chType == CHTypeNumber.CHTypeInt8.instance) {
        n += 1
      } else if (chType == CHTypeNumber.CHTypeInt16.instance) {
        n += 2
      } else if (chType == CHTypeNumber.CHTypeInt32.instance) {
        n += 4
      } else if (chType == CHTypeNumber.CHTypeInt64.instance) {
        n += 8
      } else if (chType == CHTypeString.instance) {
        n += stringBase.length
      } else {
        throw new UnsupportedOperationException("Unsupported data type: " + chType.name())
      }
    }
    n
  }

  def getRow(): SimpleRow = {
    var fields = new Array[Object](dataTypes.length)
    for (i: Int <- 0 until dataTypes.length) {
      val chType = dataTypes(i)
      if (chType == CHTypeNumber.CHTypeInt8.instance) {
        fields(i) = byteBase.asInstanceOf[Object];
        if (!useSameValue) {
          byteBase = (byteBase + 1).toByte
        }
      } else if (chType == CHTypeNumber.CHTypeInt16.instance) {
        fields(i) = shortBase.asInstanceOf[Object];
        if (!useSameValue) {
          shortBase = (shortBase + 1).toShort
        }
      } else if (chType == CHTypeNumber.CHTypeInt32.instance) {
        fields(i) = integerBase.asInstanceOf[Object];
        if (!useSameValue) {
          integerBase += 1
        }
      } else if (chType == CHTypeNumber.CHTypeInt64.instance) {
        fields(i) = longBase.asInstanceOf[Object];
        if (!useSameValue) {
          longBase += 1
        }
      } else if (chType == CHTypeString.instance) {
        fields(i) = stringBase
      } else {
        throw new UnsupportedOperationException("Unsupported data type: " + chType.name())
      }
    }
    new SimpleRow(fields)
  }
}

private class CHRowsInserter(val database: String,
                             val table: String,
                             val host: String,
                             val port: Int,
                             val rows: Long,
                             val clientBatch: Int,
                             val storageBatchRows: Int,
                             val storageBatchBytes: Int,
                             val sameValue: Boolean,
                             val threadIndex: Int,
                             val verb: Int) {
  var client = new SparkCHClientInsert(CHSql.insertStmt(database, table), host, port)
  client.setClientBatch(clientBatch)
  client.setStorageBatchRows(storageBatchRows)
  client.setStorageBatchBytes(storageBatchBytes)
  client.insertPrefix()

  var dataTypes = client.dataTypes()
  if (dataTypes == null) {
    client.insertSuffix()
    client.close()
    client = null
    throw new Exception("Fetch schema of " + database + "." + table + " failed")
  }

  var rand = new CHRandRowGenerator(dataTypes, 0, sameValue)

  def run() {
    if (verb > 0) {
      println("Thread #" + threadIndex + " [" + new Date() + "] start")
    }
    var n: Int = 0
    while (n < rows) {
      client.insert(rand.getRow())
      n += 1
    }
    client.insertSuffix()
    client.close()
    client = null
    if (verb > 0) {
      println("Thread #" + threadIndex + " [" + new Date() + "] done, inserted rows: " + rows)
    }
  }
}

object CHRawWriter {
  def main(args: Array[String]) {
    if (args.length < 3) {
      println(
        "usage: <bin> database table sending-batch-rows-count writing-batch-rows writing-batch-bytes insert-rows-count threads [pre-create-table-sql=''] [use-same-value=false] [verb=0|1|2] [host] [port]"
      )
      return
    }

    val database: String = args(0)
    val table: String = args(1)
    val clientBatch: Int = args(2).toInt
    val storageBatchRows: Int = args(3).toInt
    val storageBatchBytes: Int = args(4).toInt
    val rows: Long = args(5).toLong
    val threads: Int = args(6).toInt
    val createSql: String =
      if (args.length >= 8 && !args(7).isEmpty) args(7).split("\\s+").mkString(" ") else ""

    val sameValue: Boolean = if (args.length >= 9 && !args(8).isEmpty) args(8).toBoolean else false
    val verb: Int = if (args.length >= 10 && !args(9).isEmpty) args(9).toInt else 2
    val host: String = if (args.length >= 11 && !args(10).isEmpty) args(10) else "127.0.0.1"
    val port: Int = if (args.length >= 12 && !args(11).isEmpty) args(11).toInt else 9000

    if (createSql.length != 0) {
      val clientSelect = new SparkCHClientSelect(createSql, host, port)
      while (clientSelect.hasNext()) {
        clientSelect.next()
      }
      clientSelect.close()
    }

    if (threads <= 0) {
      return
    }

    if (verb > 0) {
      if (verb > 1) {
        println(
          "=> Config\n"
            + "Database: " + database + "\n"
            + "Table: " + table + "\n"
            + "Sending batch size: " + clientBatch + "\n"
            + "Writing batch rows: " + storageBatchRows + "\n"
            + "Writing batch bytes: " + storageBatchBytes + "\n"
            + "Total insert rows: " + rows + "\n"
            + "Threads: " + threads + "\n"
            + "---\n"
            + "Use same value: " + sameValue + "\n"
            + "Verb: " + verb + "\n"
            + "Host: " + host + "\n"
            + "Port: " + port + "\n"
            + "---\nSchema:\n" + createSql + "\n"
        )
      }
      println("=> Starting")
    }

    var inserters = new Array[CHRowsInserter](threads)
    val rowsPerThread = rows / threads
    var dataTypes: Array[CHType] = null

    for (i <- 0 until threads) {
      var threadRows = rowsPerThread
      if (i == threads - 1) {
        threadRows = rows - rowsPerThread * (threads - 1)
      }
      var inserter =
        new CHRowsInserter(
          database,
          table,
          host,
          port,
          threadRows,
          clientBatch,
          storageBatchRows,
          storageBatchBytes,
          sameValue,
          i,
          verb
        )
      dataTypes = inserter.dataTypes
      inserters(i) = inserter
    }

    val startTime = new Date()

    val workers = new Array[Thread](threads)
    for (i <- 0 until threads) {
      workers(i) = new Thread {
        override def run {
          inserters(i).run()
        }
      }
      workers(i).start
    }
    workers.foreach(_.join)

    val endTime = new Date()
    val elapsed = endTime.getTime - startTime.getTime
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("mm:ss")
    val rowBytes: Long = new CHRandRowGenerator(dataTypes).bytesOfRow()
    val totalBytes: Long = rows * rowBytes

    if (verb > 0) {
      println(
        "\n=> Finished\n"
          + "Elapsed: " + dateFormat.format(elapsed) + "\n"
          + "Total rows: " + rows + "\n"
          + "One row bytes: " + rowBytes + "\n"
          + "Total bytes: " + totalBytes + "\n"
          + "---\n"
          + "Rows/s: " + (rows * 1000 / elapsed).toLong + "\n"
          + "MB/s: " + totalBytes.toDouble / 1000 / elapsed
      )
    }
  }
}
