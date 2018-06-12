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

class Join0Suite extends BaseClickHouseSuite {
  // seems clickhouse cannot handle cross join correctly?

  private val allCases = Seq[String](
    "select a.id_dt, tp_int32, tp_int64 from (select id_dt, tp_int32 from full_data_type_table order by tp_int32 limit 20) a cross join (select id_dt, tp_int64 from full_data_type_table order by tp_int64 limit 20) b order by (id_dt, tp_int32, tp_int64)",
    "select * from (select id_dt, tp_int32 from full_data_type_table order by tp_int32 limit 20) a cross join (select id_dt, tp_int64 from full_data_type_table order by tp_int64 limit 20) b order by (tp_int32, tp_int64)"
  )

  allCases foreach { query =>
    {
      test(query) {
        runTest(query, skipJDBC = true)
      }
    }
  }

}
