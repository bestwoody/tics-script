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

class OtherTestSuite extends BaseClickHouseSuite {
  private val cases = Seq[String](
    "select id_dt from full_data_type_table where tp_date is null",
    "select id_dt from full_data_type_table where tp_date is not null",
    "select id_dt from full_data_type_table where tp_decimal is not null",
    "select tp_string from full_data_type_table where tp_string like 'g%' order by id_dt",
    "select tp_string from full_data_type_table where tp_string like '%H%' order by id_dt"
  )

  cases foreach { query =>
    {
      test(query) {
        runTest(query)
      }
    }
  }
}
