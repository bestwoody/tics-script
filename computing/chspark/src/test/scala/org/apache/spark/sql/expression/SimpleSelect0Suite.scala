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

class SimpleSelect0Suite extends BaseClickHouseSuite {
  private val allCases = Seq[String](
    "select id_dt from full_data_type_table order by id_dt limit 20",
    "select tp_boolean from full_data_type_table order by tp_boolean limit 20",
    "select tp_date from full_data_type_table order by tp_date limit 20",
    "select tp_datetime from full_data_type_table order by tp_datetime limit 20",
    "select tp_float32 from full_data_type_table order by tp_float32 limit 20",
    "select tp_float64 from full_data_type_table order by tp_float64 limit 20",
    "select tp_decimal from full_data_type_table order by tp_decimal limit 20",
    "select tp_uint8 from full_data_type_table order by tp_uint8 limit 20",
    "select tp_uint16 from full_data_type_table order by tp_uint16 limit 20",
    "select tp_uint32 from full_data_type_table order by tp_uint32 limit 20",
    "select tp_uint64 from full_data_type_table order by tp_uint64 limit 20",
    "select tp_int8 from full_data_type_table order by tp_int8 limit 20",
    "select tp_int16 from full_data_type_table order by tp_int16 limit 20",
    "select tp_int32 from full_data_type_table order by tp_int32 limit 20",
    "select tp_int64 from full_data_type_table order by tp_int64 limit 20",
    "select tp_string from full_data_type_table order by tp_string limit 20"
  )

  allCases foreach { query =>
    {
      test(query) {
        runTest(query)
      }
    }
  }

}
