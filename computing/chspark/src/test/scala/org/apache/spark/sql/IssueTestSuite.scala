/*
 *
 * Copyright 2018 PingCAP, Inc.
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

package org.apache.spark.sql

class IssueTestSuite extends BaseClickHouseSuite {
  test("test decimals") {
    // will delete this test after decimal tests are finished.
    runTest("select tp_decimal from full_data_type_table")
  }

  test("#413 Count distinct has an incorrect plan") {
    runTest("select sum(distinct tp_int8) from full_data_type_table")
    runTest("select count(distinct tp_int8) from full_data_type_table")
    runTest("select avg(distinct tp_int8) from full_data_type_table")
  }

  test("#235 Comparison incorrect when TimeStamp cast to Date in predicates") {
    runTest(
      "SELECT cast(tp_datetime as date) cast_datetime, date(tp_datetime) date_datetime, tp_datetime FROM full_data_type_table WHERE date(tp_datetime) > date('2009-01-02')"
    )
  }
}
