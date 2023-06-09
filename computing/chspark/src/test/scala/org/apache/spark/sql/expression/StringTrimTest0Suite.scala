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

class StringTrimTest0Suite extends BaseClickHouseSuite {
  private val allCases = Seq[String](
    "select trim(tp_string) from full_data_type_table",
    "select trim(' i3wm', tp_string) from full_data_type_table",
    "select ltrim(tp_string) from full_data_type_table",
    "select ltrim(' arch', tp_string) from full_data_type_table",
    "select rtrim(tp_string) from full_data_type_table",
    "select rtrim('linux', tp_string) from full_data_type_table",
    "select trim(tp_utf8) from full_data_type_table",
    "select trim('分布式', tp_utf8) from full_data_type_table",
    "select ltrim(tp_utf8) from full_data_type_table",
    "select ltrim('一致性协议', tp_utf8) from full_data_type_table",
    "select rtrim(tp_utf8) from full_data_type_table",
    "select rtrim('事务', tp_utf8) from full_data_type_table"
  )

  allCases foreach { query =>
    {
      test(query) {
        runTest(query)
      }
    }
  }

}
