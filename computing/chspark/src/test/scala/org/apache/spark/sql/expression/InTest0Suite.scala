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

class InTest0Suite extends BaseClickHouseSuite {
  private val allCases = Seq[String](
    "select tp_int32 from full_data_type_table  where tp_int32 in (2333, 601508558, 4294967296, 4294967295)",
    "select tp_int64 from full_data_type_table  where tp_int64 in (122222, -2902580959275580308, 9223372036854775807, 9223372036854775808)",
    "select tp_string from full_data_type_table  where tp_string in ('nova', '2nIGRwH4cYCroORc958d')",
    "select tp_decimal from full_data_type_table  where tp_decimal in (70394684189334124418.3210174888, 70394684189334124418.3210174889, 41693577077730693327.2727249575)",
    "select tp_float64 from full_data_type_table  where tp_float64 in (-3.0770481530,5.3030606820,-5.1807925690)",
    "select tp_float32 from full_data_type_table  where tp_float32 in (0.516204,-0.645650,0.477112)",
    "select tp_datetime from full_data_type_table  where tp_datetime in ('2007-12-18 17:38:22','2017-09-07 11:11:11','2002-10-21 07:18:29')",
    "select tp_date from full_data_type_table  where tp_date in (date '2004-04-03', date '2043-11-28')"
  )

  allCases foreach { query =>
    {
      test(query) {
        runTest(query)
      }
    }
  }

}
