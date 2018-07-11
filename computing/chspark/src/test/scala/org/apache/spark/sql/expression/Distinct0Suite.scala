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

class Distinct0Suite extends BaseClickHouseSuite {
  private val allCases = Seq[String](
    "select distinct(id_dt) from full_data_type_table order by id_dt",
    "select distinct(tp_boolean) from full_data_type_table order by tp_boolean",
    "select distinct(tp_date) from full_data_type_table order by tp_date",
    "select distinct(tp_datetime) from full_data_type_table order by tp_datetime",
    "select distinct(tp_float32) from full_data_type_table order by tp_float32",
    "select distinct(tp_float64) from full_data_type_table order by tp_float64",
    "select distinct(tp_decimal) from full_data_type_table order by tp_decimal",
    "select distinct(tp_uint8) from full_data_type_table order by tp_uint8",
    "select distinct(tp_uint16) from full_data_type_table order by tp_uint16",
    "select distinct(tp_uint32) from full_data_type_table order by tp_uint32",
    "select distinct(tp_uint64) from full_data_type_table order by tp_uint64",
    "select distinct(tp_int8) from full_data_type_table order by tp_int8",
    "select distinct(tp_int16) from full_data_type_table order by tp_int16",
    "select distinct(tp_int32) from full_data_type_table order by tp_int32",
    "select distinct(tp_int64) from full_data_type_table order by tp_int64",
    "select distinct(tp_string) from full_data_type_table order by tp_string"
  )

  allCases foreach { query =>
    {
      test(query) {
        runTest(query)
      }
    }
  }

}
