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

class Between0Suite extends BaseClickHouseSuite {
  private val allCases = Seq[String](
    "select tp_date from full_data_type_table where tp_date between date '2007-01-01' and date '2008-01-01'",
    "select tp_datetime from full_data_type_table where tp_datetime between '2007-01-01 00:00:00' and '2008-01-01 00:00:00'",
    "select tp_float32 from full_data_type_table where tp_float32 between -0.379347 and 0.660279",
    "select tp_float64 from full_data_type_table where tp_float64 between -3.9811620520 and 5.7086851260",
    "select tp_uint8 from full_data_type_table where tp_uint8 between 49 and 250",
    "select tp_uint16 from full_data_type_table where tp_uint16 between 27661 and 61318",
    "select tp_uint32 from full_data_type_table where tp_uint32 between 1519221222 and 3872657088",
    "select tp_uint64 from full_data_type_table where tp_uint64 between 8420541050786697579 and 10620405007566100753",
    "select tp_int8 from full_data_type_table where tp_int8 between -124 and -1",
    "select tp_int16 from full_data_type_table where tp_int16 between -17089 and 21949",
    "select tp_int32 from full_data_type_table where tp_int32 between -1504412665 and 976916010",
    "select tp_int64 from full_data_type_table where tp_int64 between -5140205782857209733 and 5095954694365419572"
  )

  allCases foreach { query =>
    {
      test(query) {
        runTest(query)
      }
    }
  }

}
