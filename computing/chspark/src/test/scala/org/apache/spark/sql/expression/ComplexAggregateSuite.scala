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

package org.apache.spark.sql.expression

import org.apache.spark.sql.BaseClickHouseSuite

class ComplexAggregateSuite extends BaseClickHouseSuite {
  private val allCases = Seq[String](
    "select min(tp_int8) from full_data_type_table",
    "select sum(tp_uint8) from full_data_type_table",
    "select sum(tp_float64) from full_data_type_table",
    "select sum(tp_decimal) from full_data_type_table",
    "select avg(tp_int32) from full_data_type_table",
    "select max(tp_int64) from full_data_type_table",
    "select max(tp_uint64) from full_data_type_table"
  )

  allCases.map { _.replace(")", " / tp_int16)") } ++ allCases.map {
    _.replace(")", " / tp_float64)")
  } ++ allCases
    .map { _.replace(")", " + tp_float32 * 2)") } foreach { query =>
    test(query) {
      runTest(query)
    }
  }
}
