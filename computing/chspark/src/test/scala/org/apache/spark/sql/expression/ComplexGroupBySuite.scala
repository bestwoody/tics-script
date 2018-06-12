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

class ComplexGroupBySuite extends BaseClickHouseSuite {
  private val allCases = Seq[String](
    "select tp_int32 + 1 from full_data_type_table  group by (tp_int32 + 1)",
    "select tp_float32 * 2 from full_data_type_table  group by (tp_float32 * 2)",
    "select tp_float32 - 2 from full_data_type_table  group by (tp_float32 - 2)",
    "select tp_float32 / 2 from full_data_type_table  group by (tp_float32 / 2)",
    "select tp_int32 + tp_int32 from full_data_type_table group by (tp_int32 + tp_int32)",
    "select tp_int32 + tp_int64 from full_data_type_table group by (tp_int32 + tp_int64)",
    "select tp_float32 + tp_float32 from full_data_type_table group by (tp_float32 + tp_float32)",
    "select tp_float64 + tp_float32 from full_data_type_table group by (tp_float64 + tp_float32)",
    "select tp_float64 + tp_float64 from full_data_type_table group by (tp_float64 + tp_float64)",
    "select tp_int32 + tp_float32 from full_data_type_table group by (tp_int32 + tp_float32)",
    "select tp_int32 + tp_float32 - tp_float64 / 5 + tp_int64 / tp_int32 from full_data_type_table group by (tp_int32 + tp_float32 - tp_float64 / 5 + tp_int64 / tp_int32)"
  )

  allCases foreach { query =>
    test(query) {
      runTest(query)
    }
  }
}
