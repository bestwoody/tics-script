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

import java.io.IOException;

import scala.actors.threadpool.BlockingQueue
import scala.actors.threadpool.LinkedBlockingQueue

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.VectorSchemaRoot;

import org.apache.spark.sql.Row


// TODO: May need rpc retry.
class CHExecutorParal(
  val query: String,
  val host: String,
  val port: Int,
  val table: String,
  val threads: Int) {

  class Result(schema: Schema, table: String, decoded: CHExecutor.Result) {
    val error = decoded.error
    val isEmpty = decoded.isEmpty

    val encoded: Iterator[Row] = if (isEmpty || error != null) {
      null
    } else {
      ArrowConverter.toRows(schema, table, decoded)
    }
  }

  private var finished: Boolean = false
  private val decodings: BlockingQueue[CHExecutor.Package] = new LinkedBlockingQueue[CHExecutor.Package](32)
  private val decodeds: BlockingQueue[Result] = new LinkedBlockingQueue[Result](32)
  private val executor = new CHExecutor(query, host, port)

  // TODO: throws
  def close(): Unit = executor.close

  def getSchema(): Schema = executor.getSchema

  def hasNext(): Boolean = {
    return !finished;
  }

  // TODO: throws InterruptedException, CHExecutor.CHExecutorException
  def next(): Result = {
      val decoded = decodeds.take
      // TODO: BUG: The end block may come earlier than normal block
      if (decoded.isEmpty) {
          finishAll
          null
      } else if (decoded.error != null) {
          finishAll
          throw new CHExecutor.CHExecutorException(decoded.error)
      } else {
        decoded
      }
  }

  private def finishAll(): Unit = {
    finished = true
    decoders.foreach(_.interrupt)
  }

  // TODO: May need multi threads fetcher
  private def startFetch(): Unit = {
    val fetcher = new Thread {
      override def run {
        try {
          while (!finished && executor.hasNext) {
            decodings.put(executor.safeNext)
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
  private def startDecode(threads: Int): Array[Thread] = {
    val decoders = new Array[Thread](threads);
    for (i <- 0 until threads) {
      decoders(i) = new Thread {
        override def run {
          var decoding: CHExecutor.Package = null
          var decoded: CHExecutor.Result = null
          try {
            while (!finished && (decoded == null || !decoded.isLast)) {
              decoding = decodings.take
              decoded = executor.safeDecode(decoding)
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

  val decoders = startDecode(threads)
  startFetch()
}
