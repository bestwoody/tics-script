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

class Count0Suite extends BaseClickHouseSuite {
  private val countCases = Seq[String](
    "select count(id_dt) from full_data_type_table",
    "select count(tp_boolean) from full_data_type_table",
    "select count(tp_date) from full_data_type_table",
    "select count(tp_datetime) from full_data_type_table",
    "select count(tp_float32) from full_data_type_table",
    "select count(tp_float64) from full_data_type_table",
    "select count(tp_decimal) from full_data_type_table",
    "select count(tp_uint8) from full_data_type_table",
    "select count(tp_uint16) from full_data_type_table",
    "select count(tp_uint32) from full_data_type_table",
    "select count(tp_uint64) from full_data_type_table",
    "select count(tp_int8) from full_data_type_table",
    "select count(tp_int16) from full_data_type_table",
    "select count(tp_int32) from full_data_type_table",
    "select count(tp_int64) from full_data_type_table",
    "select count(tp_string) from full_data_type_table",
    "select count(tp_int8, tp_uint8) from full_data_type_table"
  )

  // count(distinct_*) cases
  private val allCases = countCases.map { _.replace("count(", "count(distinct ") } ++ countCases

  allCases foreach { query =>
    {
      test(query) {
        runTest(query)
      }
    }
  }

}
