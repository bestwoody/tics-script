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
import org.scalatest.Ignore

import scala.collection.mutable

@Ignore
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

  val tpchTables = Seq(
    "lineitem",
    "orders",
    "customer",
    "nation",
    "customer",
    "partsupp",
    "part",
    "region",
    "supplier"
  )

  private def chSparkRes(q: String) = {
    tpchTables.foreach(spark.sqlContext.dropTempTable)
    // We do not use statistic information here due to conflict of netty versions when physical plan has broadcast nodes.
    setCurrentDatabase(tpchDBName)
    val queryString = resourceToString(
      s"tpch-sql/$q.sql",
      classLoader = Thread.currentThread().getContextClassLoader
    )
    val res = querySpark(queryString)
    println(s"CHSpark finished $q")
    res
  }

  private def jdbcRes(q: String) = {
    tpchTables.foreach(createOrReplaceTempView(tpchDBName, _, ""))
    setCurrentDatabase("default")
    val queryString = resourceToString(
      s"tpch-sql/$q.sql",
      classLoader = Thread.currentThread().getContextClassLoader
    )
    val res = querySpark(queryString)
    println(s"Spark JDBC finished $q")
    res
  }

  tpchQueries.foreach { name =>
    test(name) {
      // Note that ClickHouse JDBC has issues concerning SortOrder, and it is not fixed yet.
      assert(compResult(chSparkRes(name), jdbcRes(name), isOrdered = false))
    }
  }
}
