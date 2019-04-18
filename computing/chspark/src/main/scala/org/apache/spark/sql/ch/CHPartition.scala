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

import com.pingcap.theflash.SparkCHClientSelect
import com.pingcap.tikv.TiConfiguration
import com.pingcap.tikv.meta.TiTimestamp
import com.pingcap.tikv.region.TiRegion
import com.pingcap.tispark.TiSessionCache
import org.apache.spark.Partition

case class CHPhysicalPlan(table: CHTableRef,
                          query: String,
                          ts: Option[TiTimestamp],
                          regions: Option[Array[TiRegion]]) {
  override def toString: String =
    s"{${table.node}, query='$query', ts=${ts.map(_.getVersion).orNull}, regions=${regions.map(_.mkString("[", ",", "]"))}}"

  def createCHClient(tiConf: TiConfiguration): SparkCHClientSelect = new SparkCHClientSelect(
    query,
    table.node.host,
    table.node.port,
    TiSessionCache.getSession(tiConf),
    ts.orNull,
    regions.orNull
  )
}

case class CHPartition(index: Int, chPhysicalPlan: CHPhysicalPlan) extends Partition {}
