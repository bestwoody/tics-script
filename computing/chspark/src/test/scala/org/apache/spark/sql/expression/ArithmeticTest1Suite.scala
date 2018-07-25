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

class ArithmeticTest1Suite extends BaseClickHouseSuite {
  private val allCases = Seq[String](
    "select tp_uint8 % 18446744073709551615 from full_data_type_table order by tp_uint8 nulls last limit 10",
    "select tp_uint8 % 9223372036854775807 from full_data_type_table order by tp_uint8 nulls last limit 10",
    "select tp_uint8 % -9223372036854775808 from full_data_type_table order by tp_uint8 nulls last limit 10",
    "select tp_uint8 % 127 from full_data_type_table order by tp_uint8 nulls last limit 10",
    "select tp_uint8 % 2 from full_data_type_table order by tp_uint8 nulls last limit 10",
    "select tp_uint8 % 0 from full_data_type_table order by tp_uint8 nulls last limit 10",
    "select tp_uint16 % 18446744073709551615 from full_data_type_table order by tp_uint16 nulls last limit 10",
    "select tp_uint16 % 9223372036854775807 from full_data_type_table order by tp_uint16 nulls last limit 10",
    "select tp_uint16 % -9223372036854775808 from full_data_type_table order by tp_uint16 nulls last limit 10",
    "select tp_uint16 % 127 from full_data_type_table order by tp_uint16 nulls last limit 10",
    "select tp_uint16 % 2 from full_data_type_table order by tp_uint16 nulls last limit 10",
    "select tp_uint16 % 0 from full_data_type_table order by tp_uint16 nulls last limit 10",
    "select tp_uint32 % 18446744073709551615 from full_data_type_table order by tp_uint32 nulls last limit 10",
    "select tp_uint32 % 9223372036854775807 from full_data_type_table order by tp_uint32 nulls last limit 10",
    "select tp_uint32 % -9223372036854775808 from full_data_type_table order by tp_uint32 nulls last limit 10",
    "select tp_uint32 % 127 from full_data_type_table order by tp_uint32 nulls last limit 10",
    "select tp_uint32 % 2 from full_data_type_table order by tp_uint32 nulls last limit 10",
    "[skip]select tp_uint32 % 0 from full_data_type_table order by tp_uint32 nulls last limit 10",
    "select tp_uint64 % 18446744073709551615 from full_data_type_table order by tp_uint64 nulls last limit 10",
    "select tp_uint64 % 9223372036854775807 from full_data_type_table order by tp_uint64 nulls last limit 10",
    "select tp_uint64 % -9223372036854775808 from full_data_type_table order by tp_uint64 nulls last limit 10",
    "select tp_uint64 % 127 from full_data_type_table order by tp_uint64 nulls last limit 10",
    "select tp_uint64 % 2 from full_data_type_table order by tp_uint64 nulls last limit 10",
    "[skip]select tp_uint64 % 0 from full_data_type_table order by tp_uint64 nulls last limit 10",
    "select tp_int8 % 18446744073709551615 from full_data_type_table order by tp_int8 nulls last limit 10",
    "select tp_int8 % 9223372036854775807 from full_data_type_table order by tp_int8 nulls last limit 10",
    "select tp_int8 % -9223372036854775808 from full_data_type_table order by tp_int8 nulls last limit 10",
    "select tp_int8 % 127 from full_data_type_table order by tp_int8 nulls last limit 10",
    "select tp_int8 % 2 from full_data_type_table order by tp_int8 nulls last limit 10",
    "select tp_int8 % 0 from full_data_type_table order by tp_int8 nulls last limit 10",
    "select tp_int16 % 18446744073709551615 from full_data_type_table order by tp_int16 nulls last limit 10",
    "select tp_int16 % 9223372036854775807 from full_data_type_table order by tp_int16 nulls last limit 10",
    "select tp_int16 % -9223372036854775808 from full_data_type_table order by tp_int16 nulls last limit 10",
    "select tp_int16 % 127 from full_data_type_table order by tp_int16 nulls last limit 10",
    "select tp_int16 % 2 from full_data_type_table order by tp_int16 nulls last limit 10",
    "select tp_int16 % 0 from full_data_type_table order by tp_int16 nulls last limit 10",
    "select tp_int32 % 18446744073709551615 from full_data_type_table order by tp_int32 nulls last limit 10",
    "select tp_int32 % 9223372036854775807 from full_data_type_table order by tp_int32 nulls last limit 10",
    "select tp_int32 % -9223372036854775808 from full_data_type_table order by tp_int32 nulls last limit 10",
    "select tp_int32 % 127 from full_data_type_table order by tp_int32 nulls last limit 10",
    "select tp_int32 % 2 from full_data_type_table order by tp_int32 nulls last limit 10",
    "select tp_int32 % 0 from full_data_type_table order by tp_int32 nulls last limit 10",
    "select tp_int64 % 18446744073709551615 from full_data_type_table order by tp_int64 nulls last limit 10",
    "select tp_int64 % 9223372036854775807 from full_data_type_table order by tp_int64 nulls last limit 10",
    "select tp_int64 % -9223372036854775808 from full_data_type_table order by tp_int64 nulls last limit 10",
    "select tp_int64 % 127 from full_data_type_table order by tp_int64 nulls last limit 10",
    "select tp_int64 % 2 from full_data_type_table order by tp_int64 nulls last limit 10",
    "select tp_int64 % 0 from full_data_type_table order by tp_int64 nulls last limit 10"
  )

  allCases foreach { query =>
    {
      test(query) {
        runTest(query)
      }
    }
  }

}
