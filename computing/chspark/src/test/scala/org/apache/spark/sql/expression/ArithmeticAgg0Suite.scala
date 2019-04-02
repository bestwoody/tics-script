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
import org.scalatest.Ignore

@Ignore
class ArithmeticAgg0Suite extends BaseClickHouseSuite {
  private val allCases = Seq[String](
    "select min(id_dt) from full_data_type_table",
    "select max(id_dt) from full_data_type_table",
    "select min(tp_boolean) from full_data_type_table",
    "select max(tp_boolean) from full_data_type_table",
    "select min(tp_date) from full_data_type_table",
    "select max(tp_date) from full_data_type_table",
    "select min(tp_datetime) from full_data_type_table",
    "select max(tp_datetime) from full_data_type_table",
    "select min(tp_float32) from full_data_type_table",
    "select max(tp_float32) from full_data_type_table",
    "select sum(tp_float32) from full_data_type_table",
    "select avg(tp_float32) from full_data_type_table",
    "select abs(tp_float32) from full_data_type_table",
    "select min(tp_float64) from full_data_type_table",
    "select max(tp_float64) from full_data_type_table",
    "select sum(tp_float64) from full_data_type_table",
    "select avg(tp_float64) from full_data_type_table",
    "select abs(tp_float64) from full_data_type_table",
    "select min(tp_decimal) from full_data_type_table",
    "select max(tp_decimal) from full_data_type_table",
    "select sum(tp_decimal) from full_data_type_table",
    "select avg(tp_decimal) from full_data_type_table",
    "select abs(tp_decimal) from full_data_type_table",
    "select min(tp_uint8) from full_data_type_table",
    "select max(tp_uint8) from full_data_type_table",
    "select sum(tp_uint8) from full_data_type_table",
    "select avg(tp_uint8) from full_data_type_table",
    "select abs(tp_uint8) from full_data_type_table",
    "select min(tp_uint16) from full_data_type_table",
    "select max(tp_uint16) from full_data_type_table",
    "select sum(tp_uint16) from full_data_type_table",
    "select avg(tp_uint16) from full_data_type_table",
    "select abs(tp_uint16) from full_data_type_table",
    "select min(tp_uint32) from full_data_type_table",
    "select max(tp_uint32) from full_data_type_table",
    "select sum(tp_uint32) from full_data_type_table",
    "select avg(tp_uint32) from full_data_type_table",
    "select abs(tp_uint32) from full_data_type_table",
    "select min(tp_uint64) from full_data_type_table",
    "select max(tp_uint64) from full_data_type_table",
    "[skip]select sum(tp_uint64) from full_data_type_table",
    "[skip]select avg(tp_uint64) from full_data_type_table",
    "select abs(tp_uint64) from full_data_type_table",
    "select min(tp_int8) from full_data_type_table",
    "select max(tp_int8) from full_data_type_table",
    "select sum(tp_int8) from full_data_type_table",
    "select avg(tp_int8) from full_data_type_table",
    "[skip]select abs(tp_int8) from full_data_type_table",
    "select min(tp_int16) from full_data_type_table",
    "select max(tp_int16) from full_data_type_table",
    "select sum(tp_int16) from full_data_type_table",
    "select avg(tp_int16) from full_data_type_table",
    "[skip]select abs(tp_int16) from full_data_type_table",
    "select min(tp_int32) from full_data_type_table",
    "select max(tp_int32) from full_data_type_table",
    "select sum(tp_int32) from full_data_type_table",
    "select avg(tp_int32) from full_data_type_table",
    "[skip]select abs(tp_int32) from full_data_type_table",
    "select min(tp_int64) from full_data_type_table",
    "select max(tp_int64) from full_data_type_table",
    "select sum(tp_int64) from full_data_type_table",
    "select avg(tp_int64) from full_data_type_table",
    "[skip]select abs(tp_int64) from full_data_type_table",
    "select min(tp_string) from full_data_type_table",
    "select max(tp_string) from full_data_type_table"
  )

  allCases foreach { query =>
    {
      test(query) {
        runTest(query, skipJDBC = true)
      }
    }
  }

}
