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

class Having0Suite extends BaseClickHouseSuite {
  private val allCases = Seq[String](
    "select tp_int32%100 a, count(*) from full_data_type_table group by (tp_int32%100) having sum(tp_int32%100) > 100 order by a",
    "select tp_uint64%100 a, count(*) from full_data_type_table group by (tp_uint64%100) having sum(tp_uint64%100) < 100 order by a"
  )

  allCases foreach { query =>
    {
      test(query) {
        runTest(query)
      }
    }
  }

}
