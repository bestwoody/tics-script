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

object CHConfigConst {
  val CATALOG_POLICY: String = "spark.flash.catalog_policy"
  val ENABLE_PUSHDOWN_AGG: String = "spark.storage.plan.pushdown.agg"
  val ENABLE_SELRAW: String = "spark.storage.selraw"
  val PARTITIONS_PER_SPLIT: String = "spark.storage.partitionsPerSplit"
  val DEFAULT_PARTITIONS_PER_SPLIT = 1
  val CLUSTER_ADDRESSES: String = "spark.flash.addresses"
  val STORAGE_BATCH_ROWS: String = "spark.flash.storage.batch.rows"
  val STORAGE_BATCH_BYTES: String = "spark.flash.storage.batch.bytes"
  val CLIENT_BATCH_SIZE: String = "spark.flash.client.batch.size"
  val _IN_TEST: String = "spark.flash.intest"
}
