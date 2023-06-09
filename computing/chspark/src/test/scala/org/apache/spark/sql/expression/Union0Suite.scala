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

class Union0Suite extends BaseClickHouseSuite {
  // Skip this test suite since ClickHouse JDBC cannot handle tests with union all.

  private val allCases = Seq[String](
    "[skip](select tp_float64 from full_data_type_table where tp_float64 < 0) union all (select tp_float64 from full_data_type_table where tp_float64 > 0)",
    "[skip](select tp_uint8 from full_data_type_table where tp_uint8 < 0) union all (select tp_uint8 from full_data_type_table where tp_uint8 > 0)",
    "[skip](select id_dt from full_data_type_table where id_dt < 0) union all (select id_dt from full_data_type_table where id_dt > 0)",
    "[skip](select tp_uint32 from full_data_type_table where tp_uint32 < 0) union all (select tp_uint32 from full_data_type_table where tp_uint32 > 0)",
    "[skip](select tp_uint64 from full_data_type_table where tp_uint64 < 0) union all (select tp_uint64 from full_data_type_table where tp_uint64 > 0)",
    "[skip](select tp_int16 from full_data_type_table where tp_int16 < 0) union all (select tp_int16 from full_data_type_table where tp_int16 > 0)",
    "[skip](select tp_float32 from full_data_type_table where tp_float32 < 0) union all (select tp_float32 from full_data_type_table where tp_float32 > 0)",
    "[skip](select tp_int8 from full_data_type_table where tp_int8 < 0) union all (select tp_int8 from full_data_type_table where tp_int8 > 0)"
  )

  allCases foreach { query =>
    {
      test(query) {
        runTest(query)
      }
    }
  }

}
