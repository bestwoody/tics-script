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

class ArithmeticTest2Suite extends BaseClickHouseSuite {
  private val divideCases = Seq[String](
    "select tp_uint16 from full_data_type_table where tp_int32 / tp_int8 > 0 order by id_dt",
    "select tp_uint16 from full_data_type_table where tp_int64 / tp_int32 >= 0 order by id_dt",
    "select tp_uint16 from full_data_type_table where tp_int64 / tp_float32 <= 0 order by id_dt",
    "select tp_uint16 from full_data_type_table where tp_uint64 / tp_float64 < 0 order by id_dt",
    "select tp_uint16 from full_data_type_table where tp_int32 / tp_float32 != 0 order by id_dt",
    "select tp_uint16 from full_data_type_table where tp_int32 / tp_float64 > 0 order by id_dt",
    "select tp_uint16 from full_data_type_table where tp_float32 / tp_uint16 > 0 order by id_dt",
    "select tp_uint16 from full_data_type_table where tp_float64 / tp_uint8 > 0 order by id_dt",
    "select tp_uint16 from full_data_type_table where tp_uint64 / tp_int8 > 0 order by id_dt",
    "select tp_uint16 from full_data_type_table where tp_uint32 / tp_uint8 < 0 order by id_dt",
    "select tp_uint16 from full_data_type_table where tp_float64 / tp_int32 > 0 order by id_dt",
    "select tp_uint16 from full_data_type_table where tp_float64 / tp_uint16 < 0 order by id_dt",
    "select tp_uint16 from full_data_type_table where tp_float32 / tp_float64 > 0 order by id_dt",
    "select tp_uint16 from full_data_type_table where tp_float64 / tp_float32 > 0 order by id_dt"
  )
  private val multiCases = divideCases map { _.replace("/", "*") }
  private val minusCases = divideCases map { _.replace("/", "-") }
  private val addCases = divideCases map { _.replace("/", "+") }
  private val allCases = addCases ++ minusCases ++ multiCases ++ divideCases

  allCases foreach { query =>
    {
      test(query) {
        runTest(query)
      }
    }
  }
}
