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

class PlaceHolderTest1Suite extends BaseClickHouseSuite {
  private val allCases = Seq[String](
    "select count(1) from full_data_type_table where id_dt < null",
    "select count(1) from full_data_type_table where id_dt < '2017-11-02'",
    "select count(1) from full_data_type_table where id_dt < '2008-01-20 03:00:03'",
    "select count(1) from full_data_type_table where id_dt < 'Ki0GlftUwBB0h2tfOoKn'",
    "select count(1) from full_data_type_table where id_dt < 18446744073709551615",
    "select count(1) from full_data_type_table where id_dt < -18446744073709551616",
    "select count(1) from full_data_type_table where id_dt < 9223372036854775807",
    "select count(1) from full_data_type_table where id_dt < -9223372036854775808",
    "select count(1) from full_data_type_table where id_dt < 127",
    "select count(1) from full_data_type_table where id_dt < 2",
    "select count(1) from full_data_type_table where id_dt < 0",
    "select count(1) from full_data_type_table where tp_boolean < null",
    "select count(1) from full_data_type_table where tp_boolean < 18446744073709551615",
    "select count(1) from full_data_type_table where tp_boolean < -18446744073709551616",
    "select count(1) from full_data_type_table where tp_boolean < 9223372036854775807",
    "select count(1) from full_data_type_table where tp_boolean < -9223372036854775808",
    "select count(1) from full_data_type_table where tp_boolean < 127",
    "select count(1) from full_data_type_table where tp_boolean < 2",
    "select count(1) from full_data_type_table where tp_boolean < 0",
    "select count(1) from full_data_type_table where tp_date < null",
    "select count(1) from full_data_type_table where tp_date < '2017-11-02'",
    "select count(1) from full_data_type_table where tp_datetime < null",
    "select count(1) from full_data_type_table where tp_datetime < '2008-01-20 03:00:03'",
    "select count(1) from full_data_type_table where tp_float32 < null",
    "select count(1) from full_data_type_table where tp_float32 < 18446744073709551615",
    "select count(1) from full_data_type_table where tp_float32 < -18446744073709551616",
    "select count(1) from full_data_type_table where tp_float32 < 9223372036854775807",
    "select count(1) from full_data_type_table where tp_float32 < -9223372036854775808",
    "select count(1) from full_data_type_table where tp_float32 < 127",
    "select count(1) from full_data_type_table where tp_float32 < 2",
    "select count(1) from full_data_type_table where tp_float32 < 0",
    "select count(1) from full_data_type_table where tp_float64 < null",
    "select count(1) from full_data_type_table where tp_float64 < 18446744073709551615",
    "select count(1) from full_data_type_table where tp_float64 < -18446744073709551616",
    "select count(1) from full_data_type_table where tp_float64 < 9223372036854775807",
    "select count(1) from full_data_type_table where tp_float64 < -9223372036854775808",
    "select count(1) from full_data_type_table where tp_float64 < 127",
    "select count(1) from full_data_type_table where tp_float64 < 2",
    "select count(1) from full_data_type_table where tp_float64 < 0",
    "select count(1) from full_data_type_table where tp_uint8 < null",
    "select count(1) from full_data_type_table where tp_uint8 < 18446744073709551615",
    "select count(1) from full_data_type_table where tp_uint8 < -18446744073709551616",
    "select count(1) from full_data_type_table where tp_uint8 < 9223372036854775807",
    "select count(1) from full_data_type_table where tp_uint8 < -9223372036854775808",
    "select count(1) from full_data_type_table where tp_uint8 < 127",
    "select count(1) from full_data_type_table where tp_uint8 < 2",
    "select count(1) from full_data_type_table where tp_uint8 < 0",
    "select count(1) from full_data_type_table where tp_uint16 < null",
    "select count(1) from full_data_type_table where tp_uint16 < 18446744073709551615",
    "select count(1) from full_data_type_table where tp_uint16 < -18446744073709551616",
    "select count(1) from full_data_type_table where tp_uint16 < 9223372036854775807",
    "select count(1) from full_data_type_table where tp_uint16 < -9223372036854775808",
    "select count(1) from full_data_type_table where tp_uint16 < 127",
    "select count(1) from full_data_type_table where tp_uint16 < 2",
    "select count(1) from full_data_type_table where tp_uint16 < 0",
    "select count(1) from full_data_type_table where tp_uint32 < null",
    "select count(1) from full_data_type_table where tp_uint32 < 18446744073709551615",
    "select count(1) from full_data_type_table where tp_uint32 < -18446744073709551616",
    "select count(1) from full_data_type_table where tp_uint32 < 9223372036854775807",
    "select count(1) from full_data_type_table where tp_uint32 < -9223372036854775808",
    "select count(1) from full_data_type_table where tp_uint32 < 127",
    "select count(1) from full_data_type_table where tp_uint32 < 2",
    "select count(1) from full_data_type_table where tp_uint32 < 0",
    "select count(1) from full_data_type_table where tp_uint64 < null",
    "select count(1) from full_data_type_table where tp_uint64 < 18446744073709551615",
    "select count(1) from full_data_type_table where tp_uint64 < -18446744073709551616",
    "select count(1) from full_data_type_table where tp_uint64 < 9223372036854775807",
    "select count(1) from full_data_type_table where tp_uint64 < -9223372036854775808",
    "select count(1) from full_data_type_table where tp_uint64 < 127",
    "select count(1) from full_data_type_table where tp_uint64 < 2",
    "select count(1) from full_data_type_table where tp_uint64 < 0",
    "select count(1) from full_data_type_table where tp_int8 < null",
    "select count(1) from full_data_type_table where tp_int8 < 18446744073709551615",
    "select count(1) from full_data_type_table where tp_int8 < -18446744073709551616",
    "select count(1) from full_data_type_table where tp_int8 < 9223372036854775807",
    "select count(1) from full_data_type_table where tp_int8 < -9223372036854775808",
    "select count(1) from full_data_type_table where tp_int8 < 127",
    "select count(1) from full_data_type_table where tp_int8 < 2",
    "select count(1) from full_data_type_table where tp_int8 < 0",
    "select count(1) from full_data_type_table where tp_int16 < null",
    "select count(1) from full_data_type_table where tp_int16 < 18446744073709551615",
    "select count(1) from full_data_type_table where tp_int16 < -18446744073709551616",
    "select count(1) from full_data_type_table where tp_int16 < 9223372036854775807",
    "select count(1) from full_data_type_table where tp_int16 < -9223372036854775808",
    "select count(1) from full_data_type_table where tp_int16 < 127",
    "select count(1) from full_data_type_table where tp_int16 < 2",
    "select count(1) from full_data_type_table where tp_int16 < 0",
    "select count(1) from full_data_type_table where tp_int32 < null",
    "select count(1) from full_data_type_table where tp_int32 < 18446744073709551615",
    "select count(1) from full_data_type_table where tp_int32 < -18446744073709551616",
    "select count(1) from full_data_type_table where tp_int32 < 9223372036854775807",
    "select count(1) from full_data_type_table where tp_int32 < -9223372036854775808",
    "select count(1) from full_data_type_table where tp_int32 < 127",
    "select count(1) from full_data_type_table where tp_int32 < 2",
    "select count(1) from full_data_type_table where tp_int32 < 0",
    "select count(1) from full_data_type_table where tp_int64 < null",
    "select count(1) from full_data_type_table where tp_int64 < 'Ki0GlftUwBB0h2tfOoKn'",
    "select count(1) from full_data_type_table where tp_int64 < 18446744073709551615",
    "select count(1) from full_data_type_table where tp_int64 < -18446744073709551616",
    "select count(1) from full_data_type_table where tp_int64 < 9223372036854775807",
    "select count(1) from full_data_type_table where tp_int64 < -9223372036854775808",
    "select count(1) from full_data_type_table where tp_int64 < 127",
    "select count(1) from full_data_type_table where tp_int64 < 2",
    "select count(1) from full_data_type_table where tp_int64 < 0",
    "select count(1) from full_data_type_table where tp_string < null",
    "select count(1) from full_data_type_table where tp_string < 'Ki0GlftUwBB0h2tfOoKn'"
  )

  allCases foreach { query =>
    {
      test(query) {
        runTest(query)
      }
    }
  }

}
