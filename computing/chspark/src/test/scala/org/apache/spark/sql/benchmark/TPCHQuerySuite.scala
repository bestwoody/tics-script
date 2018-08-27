/*
 *
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
 *
 */

package org.apache.spark.sql.benchmark

import org.apache.spark.sql.BaseClickHouseSuite
import org.apache.spark.sql.catalyst.util.resourceToString

import scala.collection.mutable

class TPCHQuerySuite extends BaseClickHouseSuite {
  val tpchQueries = Seq(
    "q1",
    "q2",
    "q3",
    "q4",
    "q5",
    "q6",
    "q7",
    "q8",
    "q9",
    "q10",
    "q11",
    "q12",
    "q13",
    "q14",
    "q15",
    "q16",
    "q17",
    "q18",
    "q19",
    "q20",
    "q21", // May cause OOM if data set is large
    "q22"
  )

  private lazy val chSparkRes = {
    val result = mutable.Map[String, List[List[Any]]]()
    // We do not use statistic information here due to conflict of netty versions when physical plan has broadcast nodes.
    ch.mapCHDatabase(database = tpchDBName)
    tpchQueries.foreach { name =>
      val queryString = resourceToString(
        s"tpch-sql/$name.sql",
        classLoader = Thread.currentThread().getContextClassLoader
      )
      result(name) = querySpark(queryString)
      println(s"CHSpark finished $name")
    }
    result
  }

  private lazy val jdbcRes = {
    val result = mutable.Map[String, List[List[Any]]]()
    createOrReplaceTempView(tpchDBName, "lineitem", "")
    createOrReplaceTempView(tpchDBName, "orders", "")
    createOrReplaceTempView(tpchDBName, "customer", "")
    createOrReplaceTempView(tpchDBName, "nation", "")
    createOrReplaceTempView(tpchDBName, "customer", "")
    createOrReplaceTempView(tpchDBName, "part", "")
    createOrReplaceTempView(tpchDBName, "partsupp", "")
    createOrReplaceTempView(tpchDBName, "region", "")
    createOrReplaceTempView(tpchDBName, "supplier", "")
    tpchQueries.foreach { name =>
      val queryString = resourceToString(
        s"tpch-sql/$name.sql",
        classLoader = Thread.currentThread().getContextClassLoader
      )
      result(name) = querySpark(queryString)
      println(s"Spark JDBC finished $name")
    }
    result
  }

  tpchQueries.foreach { name =>
    test(name) {
      // We need to make sure `clickHouseMapDatabase` happens before JDBC tables mapping,
      // because calling `clickHouseMapDatabase` will only try to `createTempView` in spark,
      // so it will not replace existing tables with the same name, as a consequence,
      // calling JDBC database mapping before `clickHouseMapDatabase` may result in unexpectedly
      // using JDBC views to run CHSpark test.
      // Reversing the order of two will not result in such problem since JDBC database
      // mapping will replace original table views.
      //
      // Note that ClickHouse JDBC has issues concerning SortOrder, and it is not fixed yet.
      assert(compResult(chSparkRes(name), jdbcRes(name), isOrdered = false))
    }
  }
}
