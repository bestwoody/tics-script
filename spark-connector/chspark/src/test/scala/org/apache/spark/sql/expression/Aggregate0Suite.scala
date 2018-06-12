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

class Aggregate0Suite extends BaseClickHouseSuite {
  private val allCases = Seq[String](
    "select id_dt from full_data_type_table group by (id_dt) order by id_dt",
    "select tp_boolean from full_data_type_table group by (tp_boolean) order by tp_boolean",
    "select tp_date from full_data_type_table group by (tp_date) order by tp_date",
    "select tp_datetime from full_data_type_table group by (tp_datetime) order by tp_datetime",
    "select tp_float32 from full_data_type_table group by (tp_float32) order by tp_float32",
    "select tp_float64 from full_data_type_table group by (tp_float64) order by tp_float64",
    "select tp_uint8 from full_data_type_table group by (tp_uint8) order by tp_uint8",
    "select tp_uint16 from full_data_type_table group by (tp_uint16) order by tp_uint16",
    "select tp_uint32 from full_data_type_table group by (tp_uint32) order by tp_uint32",
    "select tp_uint64 from full_data_type_table group by (tp_uint64) order by tp_uint64",
    "select tp_int8 from full_data_type_table group by (tp_int8) order by tp_int8",
    "select tp_int16 from full_data_type_table group by (tp_int16) order by tp_int16",
    "select tp_int32 from full_data_type_table group by (tp_int32) order by tp_int32",
    "select tp_int64 from full_data_type_table group by (tp_int64) order by tp_int64",
    "select tp_string from full_data_type_table group by (tp_string) order by tp_string",

    "[skip]select 999 + tp_int32 + sum(tp_int32 + 999) from full_data_type_table  group by tp_int32 + 999 order by 1",
    "[skip]select 999 + tp_int32 + sum(tp_int32) from full_data_type_table  group by tp_int32 + 999 order by 1",
    "[skip]select 999 + tp_int32+sum(tp_int32), tp_int32 + 999 + 1 from full_data_type_table  group by tp_int32 + 999 order by 1,2",
    "/*non-order*/select tp_int32 + 999 + 1 from full_data_type_table group by tp_int32 + 999 order by 1",

    "/*non-order*/select tp_int32 + 999, tp_int32 + 999 + sum(tp_int32 + 999) from full_data_type_table  group by tp_int32 + 999 order by 1",
    "/*non-order*/select tp_int32 + 999 + sum(tp_int32) from full_data_type_table  group by tp_int32 + 999 order by 1",
    "/*non-order*/select tp_int32 + 999 + sum(tp_int32), tp_int32 + 999 + 1 from full_data_type_table  group by tp_int32 + 999 order by 1,2"
  )

  allCases foreach { query =>
    {
      test(query) {
        runTest(query, skipClickHouse = true)
      }
    }
  }

}
