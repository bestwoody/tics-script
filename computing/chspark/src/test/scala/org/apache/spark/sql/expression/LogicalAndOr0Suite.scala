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

class LogicalAndOr0Suite extends BaseClickHouseSuite {
  private val allCases = Seq[String](
    "select tp_float32, tp_float32 from full_data_type_table where tp_float32 = tp_float32 and tp_float32 > 0",
    "select tp_float32, tp_float64 from full_data_type_table where tp_float32 = tp_float64 and tp_float32 > 0",
    "select tp_float32, tp_uint8 from full_data_type_table where tp_float32 = tp_uint8 and tp_float32 > 0",
    "select tp_float32, tp_uint16 from full_data_type_table where tp_float32 = tp_uint16 and tp_float32 > 0",
    "select tp_float32, tp_uint32 from full_data_type_table where tp_float32 = tp_uint32 and tp_float32 > 0",
    "select tp_float32, tp_uint64 from full_data_type_table where tp_float32 = tp_uint64 and tp_float32 > 0",
    "select tp_float32, tp_int8 from full_data_type_table where tp_float32 = tp_int8 and tp_float32 > 0",
    "select tp_float32, tp_int16 from full_data_type_table where tp_float32 = tp_int16 and tp_float32 > 0",
    "select tp_float32, tp_int32 from full_data_type_table where tp_float32 = tp_int32 and tp_float32 > 0",
    "select tp_float32, tp_int64 from full_data_type_table where tp_float32 = tp_int64 and tp_float32 > 0",
    "select tp_float64, tp_float32 from full_data_type_table where tp_float64 = tp_float32 and tp_float64 > 0",
    "select tp_float64, tp_float64 from full_data_type_table where tp_float64 = tp_float64 and tp_float64 > 0",
    "select tp_float64, tp_uint8 from full_data_type_table where tp_float64 = tp_uint8 and tp_float64 > 0",
    "select tp_float64, tp_uint16 from full_data_type_table where tp_float64 = tp_uint16 and tp_float64 > 0",
    "select tp_float64, tp_uint32 from full_data_type_table where tp_float64 = tp_uint32 and tp_float64 > 0",
    "select tp_float64, tp_uint64 from full_data_type_table where tp_float64 = tp_uint64 and tp_float64 > 0",
    "select tp_float64, tp_int8 from full_data_type_table where tp_float64 = tp_int8 and tp_float64 > 0",
    "select tp_float64, tp_int16 from full_data_type_table where tp_float64 = tp_int16 and tp_float64 > 0",
    "select tp_float64, tp_int32 from full_data_type_table where tp_float64 = tp_int32 and tp_float64 > 0",
    "select tp_float64, tp_int64 from full_data_type_table where tp_float64 = tp_int64 and tp_float64 > 0",
    "select tp_uint8, tp_float32 from full_data_type_table where tp_uint8 = tp_float32 and tp_uint8 > 0",
    "select tp_uint8, tp_float64 from full_data_type_table where tp_uint8 = tp_float64 and tp_uint8 > 0",
    "select tp_uint8, tp_uint8 from full_data_type_table where tp_uint8 = tp_uint8 and tp_uint8 > 0",
    "select tp_uint8, tp_uint16 from full_data_type_table where tp_uint8 = tp_uint16 and tp_uint8 > 0",
    "select tp_uint8, tp_uint32 from full_data_type_table where tp_uint8 = tp_uint32 and tp_uint8 > 0",
    "select tp_uint8, tp_uint64 from full_data_type_table where tp_uint8 = tp_uint64 and tp_uint8 > 0",
    "select tp_uint8, tp_int8 from full_data_type_table where tp_uint8 = tp_int8 and tp_uint8 > 0",
    "select tp_uint8, tp_int16 from full_data_type_table where tp_uint8 = tp_int16 and tp_uint8 > 0",
    "select tp_uint8, tp_int32 from full_data_type_table where tp_uint8 = tp_int32 and tp_uint8 > 0",
    "select tp_uint8, tp_int64 from full_data_type_table where tp_uint8 = tp_int64 and tp_uint8 > 0",
    "select tp_uint16, tp_float32 from full_data_type_table where tp_uint16 = tp_float32 and tp_uint16 > 0",
    "select tp_uint16, tp_float64 from full_data_type_table where tp_uint16 = tp_float64 and tp_uint16 > 0",
    "select tp_uint16, tp_uint8 from full_data_type_table where tp_uint16 = tp_uint8 and tp_uint16 > 0",
    "select tp_uint16, tp_uint16 from full_data_type_table where tp_uint16 = tp_uint16 and tp_uint16 > 0",
    "select tp_uint16, tp_uint32 from full_data_type_table where tp_uint16 = tp_uint32 and tp_uint16 > 0",
    "select tp_uint16, tp_uint64 from full_data_type_table where tp_uint16 = tp_uint64 and tp_uint16 > 0",
    "select tp_uint16, tp_int8 from full_data_type_table where tp_uint16 = tp_int8 and tp_uint16 > 0",
    "select tp_uint16, tp_int16 from full_data_type_table where tp_uint16 = tp_int16 and tp_uint16 > 0",
    "select tp_uint16, tp_int32 from full_data_type_table where tp_uint16 = tp_int32 and tp_uint16 > 0",
    "select tp_uint16, tp_int64 from full_data_type_table where tp_uint16 = tp_int64 and tp_uint16 > 0",
    "select tp_uint32, tp_float32 from full_data_type_table where tp_uint32 = tp_float32 and tp_uint32 > 0",
    "select tp_uint32, tp_float64 from full_data_type_table where tp_uint32 = tp_float64 and tp_uint32 > 0",
    "select tp_uint32, tp_uint8 from full_data_type_table where tp_uint32 = tp_uint8 and tp_uint32 > 0",
    "select tp_uint32, tp_uint16 from full_data_type_table where tp_uint32 = tp_uint16 and tp_uint32 > 0",
    "select tp_uint32, tp_uint32 from full_data_type_table where tp_uint32 = tp_uint32 and tp_uint32 > 0",
    "select tp_uint32, tp_uint64 from full_data_type_table where tp_uint32 = tp_uint64 and tp_uint32 > 0",
    "select tp_uint32, tp_int8 from full_data_type_table where tp_uint32 = tp_int8 and tp_uint32 > 0",
    "select tp_uint32, tp_int16 from full_data_type_table where tp_uint32 = tp_int16 and tp_uint32 > 0",
    "select tp_uint32, tp_int32 from full_data_type_table where tp_uint32 = tp_int32 and tp_uint32 > 0",
    "select tp_uint32, tp_int64 from full_data_type_table where tp_uint32 = tp_int64 and tp_uint32 > 0",
    "select tp_uint64, tp_float32 from full_data_type_table where tp_uint64 = tp_float32 and tp_uint64 > 0",
    "select tp_uint64, tp_float64 from full_data_type_table where tp_uint64 = tp_float64 and tp_uint64 > 0",
    "select tp_uint64, tp_uint8 from full_data_type_table where tp_uint64 = tp_uint8 and tp_uint64 > 0",
    "select tp_uint64, tp_uint16 from full_data_type_table where tp_uint64 = tp_uint16 and tp_uint64 > 0",
    "select tp_uint64, tp_uint32 from full_data_type_table where tp_uint64 = tp_uint32 and tp_uint64 > 0",
    "select tp_uint64, tp_uint64 from full_data_type_table where tp_uint64 = tp_uint64 and tp_uint64 > 0",
    "select tp_uint64, tp_int8 from full_data_type_table where tp_uint64 = tp_int8 and tp_uint64 > 0",
    "select tp_uint64, tp_int16 from full_data_type_table where tp_uint64 = tp_int16 and tp_uint64 > 0",
    "select tp_uint64, tp_int32 from full_data_type_table where tp_uint64 = tp_int32 and tp_uint64 > 0",
    "select tp_uint64, tp_int64 from full_data_type_table where tp_uint64 = tp_int64 and tp_uint64 > 0",
    "select tp_int8, tp_float32 from full_data_type_table where tp_int8 = tp_float32 and tp_int8 > 0",
    "select tp_int8, tp_float64 from full_data_type_table where tp_int8 = tp_float64 and tp_int8 > 0",
    "select tp_int8, tp_uint8 from full_data_type_table where tp_int8 = tp_uint8 and tp_int8 > 0",
    "select tp_int8, tp_uint16 from full_data_type_table where tp_int8 = tp_uint16 and tp_int8 > 0",
    "select tp_int8, tp_uint32 from full_data_type_table where tp_int8 = tp_uint32 and tp_int8 > 0",
    "select tp_int8, tp_uint64 from full_data_type_table where tp_int8 = tp_uint64 and tp_int8 > 0",
    "select tp_int8, tp_int8 from full_data_type_table where tp_int8 = tp_int8 and tp_int8 > 0",
    "select tp_int8, tp_int16 from full_data_type_table where tp_int8 = tp_int16 and tp_int8 > 0",
    "select tp_int8, tp_int32 from full_data_type_table where tp_int8 = tp_int32 and tp_int8 > 0",
    "select tp_int8, tp_int64 from full_data_type_table where tp_int8 = tp_int64 and tp_int8 > 0",
    "select tp_int16, tp_float32 from full_data_type_table where tp_int16 = tp_float32 and tp_int16 > 0",
    "select tp_int16, tp_float64 from full_data_type_table where tp_int16 = tp_float64 and tp_int16 > 0",
    "select tp_int16, tp_uint8 from full_data_type_table where tp_int16 = tp_uint8 and tp_int16 > 0",
    "select tp_int16, tp_uint16 from full_data_type_table where tp_int16 = tp_uint16 and tp_int16 > 0",
    "select tp_int16, tp_uint32 from full_data_type_table where tp_int16 = tp_uint32 and tp_int16 > 0",
    "select tp_int16, tp_uint64 from full_data_type_table where tp_int16 = tp_uint64 and tp_int16 > 0",
    "select tp_int16, tp_int8 from full_data_type_table where tp_int16 = tp_int8 and tp_int16 > 0",
    "select tp_int16, tp_int16 from full_data_type_table where tp_int16 = tp_int16 and tp_int16 > 0",
    "select tp_int16, tp_int32 from full_data_type_table where tp_int16 = tp_int32 and tp_int16 > 0",
    "select tp_int16, tp_int64 from full_data_type_table where tp_int16 = tp_int64 and tp_int16 > 0",
    "select tp_int32, tp_float32 from full_data_type_table where tp_int32 = tp_float32 and tp_int32 > 0",
    "select tp_int32, tp_float64 from full_data_type_table where tp_int32 = tp_float64 and tp_int32 > 0",
    "select tp_int32, tp_uint8 from full_data_type_table where tp_int32 = tp_uint8 and tp_int32 > 0",
    "select tp_int32, tp_uint16 from full_data_type_table where tp_int32 = tp_uint16 and tp_int32 > 0",
    "select tp_int32, tp_uint32 from full_data_type_table where tp_int32 = tp_uint32 and tp_int32 > 0",
    "select tp_int32, tp_uint64 from full_data_type_table where tp_int32 = tp_uint64 and tp_int32 > 0",
    "select tp_int32, tp_int8 from full_data_type_table where tp_int32 = tp_int8 and tp_int32 > 0",
    "select tp_int32, tp_int16 from full_data_type_table where tp_int32 = tp_int16 and tp_int32 > 0",
    "select tp_int32, tp_int32 from full_data_type_table where tp_int32 = tp_int32 and tp_int32 > 0",
    "select tp_int32, tp_int64 from full_data_type_table where tp_int32 = tp_int64 and tp_int32 > 0",
    "select tp_int64, tp_float32 from full_data_type_table where tp_int64 = tp_float32 and tp_int64 > 0",
    "select tp_int64, tp_float64 from full_data_type_table where tp_int64 = tp_float64 and tp_int64 > 0",
    "select tp_int64, tp_uint8 from full_data_type_table where tp_int64 = tp_uint8 and tp_int64 > 0",
    "select tp_int64, tp_uint16 from full_data_type_table where tp_int64 = tp_uint16 and tp_int64 > 0",
    "select tp_int64, tp_uint32 from full_data_type_table where tp_int64 = tp_uint32 and tp_int64 > 0",
    "select tp_int64, tp_uint64 from full_data_type_table where tp_int64 = tp_uint64 and tp_int64 > 0",
    "select tp_int64, tp_int8 from full_data_type_table where tp_int64 = tp_int8 and tp_int64 > 0",
    "select tp_int64, tp_int16 from full_data_type_table where tp_int64 = tp_int16 and tp_int64 > 0",
    "select tp_int64, tp_int32 from full_data_type_table where tp_int64 = tp_int32 and tp_int64 > 0",
    "select tp_int64, tp_int64 from full_data_type_table where tp_int64 = tp_int64 and tp_int64 > 0"
  ).map(_.concat(" order by id_dt"))

  allCases foreach { query =>
    {
      test(query) {
        runTest(query)
      }
    }
  }

}
