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
  val ENABLE_PUSHDOWN_AGG: String = "spark.ch.plan.pushdown.agg"
  val ENABLE_SINGLE_NODE_OPT: String = "spark.ch.plan.single.node.opt"
  val ENABLE_SELRAW: String = "spark.ch.storage.selraw"
  val ENABLE_SELRAW_TABLE_INFO: String = "spark.ch.storage.tableinfo.selraw"
}
