package org.apache.spark.sql.plan

import org.apache.spark.sql.BaseClickHouseSuite

class PlanTestSuite extends BaseClickHouseSuite {
  test("Complex projection/filter pushdown") {
    explainAndRunTest("select tp_uint16 from full_data_type_table where tp_decimal - tp_uint16 > 0")
    explainAndRunTest(
      "select tp_uint16, tp_decimal - tp_uint16 from full_data_type_table where tp_decimal - tp_uint16 > 0 order by id_dt nulls first"
    )
    explainAndRunTest(
      "select tp_decimal, tp_uint16, tp_decimal - tp_uint16 from full_data_type_table where tp_decimal - tp_uint16 > 0 order by id_dt nulls first"
    )
  }

  test("Complex aggregation pushdown") {
    explainAndRunTest(
      "select tp_boolean, tp_uint32 from full_data_type_table where tp_boolean = tp_uint32 order by tp_uint32 nulls last limit 20"
    )
    explainAndRunTest(
      "select avg(tp_int32 + 2), first(tp_decimal) from full_data_type_table where tp_int16 % 2 == 0 group by (tp_int32 + 2) order by first(id_dt) nulls first"
    )
  }

  test("Duplicate projection pushdown") {
    // TODO: resolve namedStruct
    explainAndRunTest(
      "select count(id_dt), (tp_int16, tp_int32 + 4), (tp_int16, tp_int32 + 4) from full_data_type_table group by (tp_int16, tp_int32 + 4) order by (tp_int16, tp_int32 + 4) nulls first limit 2"
    )
  }
}
