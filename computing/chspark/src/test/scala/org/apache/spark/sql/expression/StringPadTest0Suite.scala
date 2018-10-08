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

class StringPadTest0Suite extends BaseClickHouseSuite {
  private val allCases = Seq[String](
    "select lpad('hello', 12, 'abc')",
    "select rpad('hello', 12, 'abc')",
    "select lpad(tp_string, 50, 'abc') from full_data_type_table",
    "select rpad(tp_string, 50, 'abc') from full_data_type_table",
    "select lpad(tp_string, 100, 'ab?') from full_data_type_table",
    "select rpad(tp_string, 100, 'ab?') from full_data_type_table",
    "select lpad(tp_string, 10, 'abc') from full_data_type_table",
    "select rpad(tp_string, 10, 'abc') from full_data_type_table",
    "select lpad(tp_string, 50, '') from full_data_type_table",
    "select rpad(tp_string, 50, '') from full_data_type_table",
    "select lpad('你好', 12, 'Pingcap')",
    "select rpad('你好', 12, 'Pingcap')",
    "select lpad(tp_utf8, 50, '晨凯') from full_data_type_table",
    "select rpad(tp_utf8, 50, '北京') from full_data_type_table",
    "select lpad(tp_utf8, 100, '上海') from full_data_type_table",
    "select rpad(tp_utf8, 100, 'ab?') from full_data_type_table",
    "select lpad(tp_utf8, 10, 'abc') from full_data_type_table",
    "select rpad(tp_utf8, 10, 'abc') from full_data_type_table",
    "select lpad(tp_utf8, 50, '') from full_data_type_table",
    "select rpad(tp_utf8, 50, '') from full_data_type_table"
  )

  allCases foreach { query =>
    {
      test(query) {
        runTest(query)
      }
    }
  }

}
