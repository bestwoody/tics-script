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

package org.apache.spark.sql.ch;

import com.pingcap.theflash.codegene.{ArrowColumnBatch, ArrowColumnVector, ColumnVector}
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.execution.arrow.ArrowUtils

import scala.actors.threadpool.{BlockingQueue, LinkedBlockingQueue}
import scala.collection.JavaConverters._


// TODO: May need rpc retry.
class CHExecutorParall(
  val qid: String,
  val query: String,
  val host: String,
  val port: Int,
  val table: String,
  val threads: Int,
  val encoders: Int = 0,
  val clientCount: Int = 1,
  val clientIndex: Int = 0,
  val encode: Boolean = true) {

  class Result(schema: Schema, table: String, val decoded: CHExecutor.Result) {
    val error = decoded.error
    val isEmpty = decoded.isEmpty
    val block = decoded.block

    val batch: ArrowColumnBatch = if (isEmpty || error != null || !encode) {
      null
    } else {
      val columns = block.getFieldVectors.asScala.map { vector =>
        new ArrowColumnVector(vector).asInstanceOf[ColumnVector]
      }.toArray

      assert(block.getFieldVectors.asScala.map(_.getValueCount).distinct.lengthCompare(1) == 0,
        "Each column should have same row count!")
      val rowCount = block.getFieldVectors.asScala.head.getValueCount

      val arrBatch = new ArrowColumnBatch(
        ArrowUtils.fromArrowSchema(block.getSchema), columns, rowCount)
      arrBatch.setNumRows(rowCount)
      arrBatch
    }

    def close(): Unit = if (batch != null) {
      batch.close()
    } else {
      decoded.close()
    }
  }

  private var inputBlocks: Long = 0
  private var outputBlocks: Long = 0
  private var totalBlocks: Long = -1

  private val decodings: BlockingQueue[CHExecutor.Package] = new LinkedBlockingQueue[CHExecutor.Package](threads)
  private val decodeds: BlockingQueue[Result] = new LinkedBlockingQueue[Result](threads)
  private val executor = new CHExecutor(qid, query, host, port, encoders, clientCount, clientIndex)

  startDecode()
  startFetch()

  // TODO: throws
  def close(): Unit = executor.close

  def getSchema(): Schema = executor.getSchema

  // TODO: throws InterruptedException, CHExecutor.CHExecutorException
  def next(): Result = {
    var got: Boolean = false
    var block: Result = null
    while (!got) {
      block = getNext
      if (block == null || !block.isEmpty) {
        got = true
      }
    }
    block
  }

  private def getNext(): Result = {
    this.synchronized {
      val hasNext = (totalBlocks < 0 || outputBlocks < totalBlocks)
      if (hasNext) {
        val decoded = decodeds.take
        if (decoded.error != null) {
            throw new CHExecutor.CHExecutorException(decoded.error)
        } else if (decoded.isEmpty) {
          decoded
        } else {
          outputBlocks += 1
          decoded
        }
      } else {
        null
      }
    }
  }

  // TODO: May need multi threads fetcher
  private def startFetch(): Unit = {
    val fetcher = new Thread {
      override def run {
        try {
          while (executor.hasNext) {
            val block = executor.safeNext
            if (!block.isLast) {
              this.synchronized {
                inputBlocks += 1
              }
              decodings.put(block)
            } else {
              this.synchronized {
                totalBlocks = inputBlocks
              }
              // End marks for decoders
              for (i <- 0 until threads) {
                decodings.put(block)
              }
            }
          }
        } catch {
          case _: InterruptedException => {}
          case e: Any => throw e
        }
      }
    }
    fetcher.start
  }

  // TODO: Reorder blocks may faster, in some cases
  private def startDecode(): Array[Thread] = {
    val decoders = new Array[Thread](threads);
    for (i <- 0 until threads) {
      decoders(i) = new Thread {
        override def run {
          try {
            var isLast = false
            while (!isLast) {
              val decoded = executor.safeDecode(decodings.take)
              isLast = decoded.isLast
              // Pass over the empty block to `getNext`, to offer the chance to check if stream is finished
              // Note that the empty block doesn't count
              decodeds.put(new Result(executor.getSchema, table, decoded))
            }
          } catch {
            case _: InterruptedException => {}
            case e: Any => throw e
          }
        }
      }
      decoders(i).start
    }
    decoders
  }
}
