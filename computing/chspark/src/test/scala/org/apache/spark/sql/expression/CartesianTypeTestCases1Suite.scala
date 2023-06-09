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

class CartesianTypeTestCases1Suite extends BaseClickHouseSuite {
  private val allCases = Seq[String](
    "select id_dt, id_dt from full_data_type_table where id_dt <> id_dt order by tp_uint32 nulls last limit 20",
    "select tp_boolean, tp_boolean from full_data_type_table where tp_boolean <> tp_boolean order by tp_uint32 nulls last limit 20",
    "select tp_boolean, tp_float32 from full_data_type_table where tp_boolean <> tp_float32 order by tp_uint32 nulls last limit 20",
    "select tp_boolean, tp_float64 from full_data_type_table where tp_boolean <> tp_float64 order by tp_uint32 nulls last limit 20",
    "select tp_boolean, tp_uint8 from full_data_type_table where tp_boolean <> tp_uint8 order by tp_uint32 nulls last limit 20",
    "select tp_boolean, tp_uint16 from full_data_type_table where tp_boolean <> tp_uint16 order by tp_uint32 nulls last limit 20",
    "select tp_boolean, tp_uint32 from full_data_type_table where tp_boolean <> tp_uint32 order by tp_uint32 nulls last limit 20",
    "select tp_boolean, tp_uint64 from full_data_type_table where tp_boolean <> tp_uint64 order by tp_uint32 nulls last limit 20",
    "select tp_boolean, tp_int8 from full_data_type_table where tp_boolean <> tp_int8 order by tp_uint32 nulls last limit 20",
    "select tp_boolean, tp_int16 from full_data_type_table where tp_boolean <> tp_int16 order by tp_uint32 nulls last limit 20",
    "select tp_boolean, tp_int32 from full_data_type_table where tp_boolean <> tp_int32 order by tp_uint32 nulls last limit 20",
    "select tp_boolean, tp_int64 from full_data_type_table where tp_boolean <> tp_int64 order by tp_uint32 nulls last limit 20",
    "select tp_float32, tp_float32 from full_data_type_table where tp_float32 <> tp_float32 order by tp_uint32 nulls last limit 20",
    "select tp_float32, tp_float64 from full_data_type_table where tp_float32 <> tp_float64 order by tp_uint32 nulls last limit 20",
    "select tp_float32, tp_uint8 from full_data_type_table where tp_float32 <> tp_uint8 order by tp_uint32 nulls last limit 20",
    "select tp_float32, tp_uint16 from full_data_type_table where tp_float32 <> tp_uint16 order by tp_uint32 nulls last limit 20",
    "select tp_float32, tp_uint32 from full_data_type_table where tp_float32 <> tp_uint32 order by tp_uint32 nulls last limit 20",
    "select tp_float32, tp_uint64 from full_data_type_table where tp_float32 <> tp_uint64 order by tp_uint32 nulls last limit 20",
    "select tp_float32, tp_int8 from full_data_type_table where tp_float32 <> tp_int8 order by tp_uint32 nulls last limit 20",
    "select tp_float32, tp_int16 from full_data_type_table where tp_float32 <> tp_int16 order by tp_uint32 nulls last limit 20",
    "select tp_float32, tp_int32 from full_data_type_table where tp_float32 <> tp_int32 order by tp_uint32 nulls last limit 20",
    "select tp_float32, tp_int64 from full_data_type_table where tp_float32 <> tp_int64 order by tp_uint32 nulls last limit 20",
    "select tp_float64, tp_float64 from full_data_type_table where tp_float64 <> tp_float64 order by tp_uint32 nulls last limit 20",
    "select tp_float64, tp_uint8 from full_data_type_table where tp_float64 <> tp_uint8 order by tp_uint32 nulls last limit 20",
    "select tp_float64, tp_uint16 from full_data_type_table where tp_float64 <> tp_uint16 order by tp_uint32 nulls last limit 20",
    "select tp_float64, tp_uint32 from full_data_type_table where tp_float64 <> tp_uint32 order by tp_uint32 nulls last limit 20",
    "select tp_float64, tp_uint64 from full_data_type_table where tp_float64 <> tp_uint64 order by tp_uint32 nulls last limit 20",
    "select tp_float64, tp_int8 from full_data_type_table where tp_float64 <> tp_int8 order by tp_uint32 nulls last limit 20",
    "select tp_float64, tp_int16 from full_data_type_table where tp_float64 <> tp_int16 order by tp_uint32 nulls last limit 20",
    "select tp_float64, tp_int32 from full_data_type_table where tp_float64 <> tp_int32 order by tp_uint32 nulls last limit 20",
    "select tp_float64, tp_int64 from full_data_type_table where tp_float64 <> tp_int64 order by tp_uint32 nulls last limit 20",
    "select tp_uint8, tp_uint8 from full_data_type_table where tp_uint8 <> tp_uint8 order by tp_uint32 nulls last limit 20",
    "select tp_uint8, tp_uint16 from full_data_type_table where tp_uint8 <> tp_uint16 order by tp_uint32 nulls last limit 20",
    "select tp_uint8, tp_uint32 from full_data_type_table where tp_uint8 <> tp_uint32 order by tp_uint32 nulls last limit 20",
    "select tp_uint8, tp_uint64 from full_data_type_table where tp_uint8 <> tp_uint64 order by tp_uint32 nulls last limit 20",
    "select tp_uint8, tp_int8 from full_data_type_table where tp_uint8 <> tp_int8 order by tp_uint32 nulls last limit 20",
    "select tp_uint8, tp_int16 from full_data_type_table where tp_uint8 <> tp_int16 order by tp_uint32 nulls last limit 20",
    "select tp_uint8, tp_int32 from full_data_type_table where tp_uint8 <> tp_int32 order by tp_uint32 nulls last limit 20",
    "select tp_uint8, tp_int64 from full_data_type_table where tp_uint8 <> tp_int64 order by tp_uint32 nulls last limit 20",
    "select tp_uint16, tp_uint16 from full_data_type_table where tp_uint16 <> tp_uint16 order by tp_uint32 nulls last limit 20",
    "select tp_uint16, tp_uint32 from full_data_type_table where tp_uint16 <> tp_uint32 order by tp_uint32 nulls last limit 20",
    "select tp_uint16, tp_uint64 from full_data_type_table where tp_uint16 <> tp_uint64 order by tp_uint32 nulls last limit 20",
    "select tp_uint16, tp_int8 from full_data_type_table where tp_uint16 <> tp_int8 order by tp_uint32 nulls last limit 20",
    "select tp_uint16, tp_int16 from full_data_type_table where tp_uint16 <> tp_int16 order by tp_uint32 nulls last limit 20",
    "select tp_uint16, tp_int32 from full_data_type_table where tp_uint16 <> tp_int32 order by tp_uint32 nulls last limit 20",
    "select tp_uint16, tp_int64 from full_data_type_table where tp_uint16 <> tp_int64 order by tp_uint32 nulls last limit 20",
    "select tp_uint32, tp_uint32 from full_data_type_table where tp_uint32 <> tp_uint32 order by tp_uint32 nulls last limit 20",
    "select tp_uint32, tp_uint64 from full_data_type_table where tp_uint32 <> tp_uint64 order by tp_uint32 nulls last limit 20",
    "select tp_uint32, tp_int8 from full_data_type_table where tp_uint32 <> tp_int8 order by tp_uint32 nulls last limit 20",
    "select tp_uint32, tp_int16 from full_data_type_table where tp_uint32 <> tp_int16 order by tp_uint32 nulls last limit 20",
    "select tp_uint32, tp_int32 from full_data_type_table where tp_uint32 <> tp_int32 order by tp_uint32 nulls last limit 20",
    "select tp_uint32, tp_int64 from full_data_type_table where tp_uint32 <> tp_int64 order by tp_uint32 nulls last limit 20",
    "select tp_uint64, tp_uint64 from full_data_type_table where tp_uint64 <> tp_uint64 order by tp_uint32 nulls last limit 20",
    "select tp_uint64, tp_int8 from full_data_type_table where tp_uint64 <> tp_int8 order by tp_uint32 nulls last limit 20",
    "select tp_uint64, tp_int16 from full_data_type_table where tp_uint64 <> tp_int16 order by tp_uint32 nulls last limit 20",
    "select tp_uint64, tp_int32 from full_data_type_table where tp_uint64 <> tp_int32 order by tp_uint32 nulls last limit 20",
    "select tp_uint64, tp_int64 from full_data_type_table where tp_uint64 <> tp_int64 order by tp_uint32 nulls last limit 20",
    "select tp_int8, tp_int8 from full_data_type_table where tp_int8 <> tp_int8 order by tp_uint32 nulls last limit 20",
    "select tp_int8, tp_int16 from full_data_type_table where tp_int8 <> tp_int16 order by tp_uint32 nulls last limit 20",
    "select tp_int8, tp_int32 from full_data_type_table where tp_int8 <> tp_int32 order by tp_uint32 nulls last limit 20",
    "select tp_int8, tp_int64 from full_data_type_table where tp_int8 <> tp_int64 order by tp_uint32 nulls last limit 20",
    "select tp_int16, tp_int16 from full_data_type_table where tp_int16 <> tp_int16 order by tp_uint32 nulls last limit 20",
    "select tp_int16, tp_int32 from full_data_type_table where tp_int16 <> tp_int32 order by tp_uint32 nulls last limit 20",
    "select tp_int16, tp_int64 from full_data_type_table where tp_int16 <> tp_int64 order by tp_uint32 nulls last limit 20",
    "select tp_int32, tp_int32 from full_data_type_table where tp_int32 <> tp_int32 order by tp_uint32 nulls last limit 20",
    "select tp_int32, tp_int64 from full_data_type_table where tp_int32 <> tp_int64 order by tp_uint32 nulls last limit 20",
    "select tp_int64, tp_int64 from full_data_type_table where tp_int64 <> tp_int64 order by tp_uint32 nulls last limit 20"
  )

  allCases foreach { query =>
    {
      test(query) {
        runTest(query)
      }
    }
  }

}
