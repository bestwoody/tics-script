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

import scala.collection.mutable.Map

// TODO: May have bug when a same query executing in the same time
object CHExecutorPool {
  class Executor(val executor: CHExecutorParall, val key: String) {
    var count = 1;
    def ref(): Executor = {
      this.synchronized {
        count += 1
        this
      }
    }
    def deref(): Unit = {
      this.synchronized {
        count -= 1
        if (count == 0) {
          executor.close
        }
      }
    }
  }

  val instances: Map[String, Executor] = Map()

  private def getKey(qid: String, query: String, host: String, port: Int, table: String, threads: Int): String = {
    qid + query + host + port + table
  }

  def get(qid: String, query: String, host: String, port: Int, table: String, threads: Int, encode: Boolean = true): Executor = {
    this.synchronized {
      val key = getKey(qid, query, host, port, table, threads)
      if (instances.contains(key)) {
        instances(key).ref
      } else {
        val executor = new CHExecutorParall(query, host, port, table, threads, encode)
        val handle = new Executor(executor, key)
        instances += (key -> handle)
        handle
      }
    }
  }

  def close(handle: Executor): Unit = {
    this.synchronized {
      if (!instances.contains(handle.key)) {
        throw new Exception("Key not found: " + handle.key)
      }
      handle.deref
      if (handle.count == 0) {
        // TODO: Schedule instance clear, but not now, should wait until all RDD's are done
      }
    }
  }
}
