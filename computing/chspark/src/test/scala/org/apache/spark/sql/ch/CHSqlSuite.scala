/*
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
 */

package org.apache.spark.sql.ch

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, CreateNamedStruct, Expression, Literal}
import org.apache.spark.sql.ch.hack.CHAttributeReference
import org.apache.spark.sql.types._

class CHSqlSuite extends SparkFunSuite {
  val booleanLiteral = Literal(true, BooleanType)
  val falseValue = false
  val nullLiteral = Literal(null, null)
  val one = 1.0
  val oneEMINUS4 = 0.0001
  val oneE8 = 100000000.0
  val abc = "abc"
  val a: AttributeReference = 'A.int
  val b: AttributeReference = 'b.int
  val c: AttributeReference = 'C.string
  val hackD: CHAttributeReference = new CHAttributeReference("d", DateType, false, Metadata.empty)
  val t = new CHTableRef(null, 0, "D", "T")

  def testCompileExpression(e: Expression, expected: String): Unit =
    assert(CHSql.compileExpression(e) == expected)

  def testCreateTable(database: String,
                      table: String,
                      schema: StructType,
                      pkList: Array[String],
                      expected: String): Unit =
    assert(CHSql.createTableStmt(database, schema, pkList, table) == expected)

  def testQuery(table: CHTableRef, chLogicalPlan: CHLogicalPlan, expected: String): Unit =
    assert(CHSql.query(table, chLogicalPlan).buildQuery() == expected)

  test("null check expressions") {
    testCompileExpression(a.isNull, "`a` IS NULL")
    testCompileExpression(b.isNotNull, "`b` IS NOT NULL")
    testCompileExpression(!b.isNotNull, "NOT `b` IS NOT NULL")
  }

  test("literal expressions") {
    testCompileExpression(booleanLiteral, "CAST(1 AS UInt8)")
    testCompileExpression(falseValue, "CAST(0 AS UInt8)")
    testCompileExpression(nullLiteral, "NULL")
    testCompileExpression(one, "1.0")
    testCompileExpression(oneEMINUS4, "1.0E-4")
    testCompileExpression(oneE8, "1.0E8")
    testCompileExpression(abc, "'abc'")
    testCompileExpression("\\", "'\\\\'")
    testCompileExpression("\'", "'\\''")
    testCompileExpression("\"a\bb\fc\rd\ne\tf\0g\"", "'\"a\\bb\\fc\\rd\\ne\\tf\\0g\"'")
  }

  test("arithmetic expressions") {
    testCompileExpression(a + b, "(`a` + `b`)")
    testCompileExpression(-(a + b), "(-(`a` + `b`))")
    testCompileExpression(a + b * -a, "(`a` + (`b` * (-`a`)))")
    testCompileExpression(a / b * a, "((`a` / `b`) * `a`)")
    testCompileExpression(a / b * (a - abs(b)), "((`a` / `b`) * (`a` - ABS(`b`)))")
    testCompileExpression(
      a - b * (a / abs(b + a % 100)),
      "(`a` - (`b` * (`a` / ABS((`b` + (`a` % 100))))))"
    )
  }

  test("comparison expressions") {
    testCompileExpression(a === b, "(`a` = `b`)")
    testCompileExpression(a > b, "(`a` > `b`)")
    testCompileExpression(a >= b, "(`a` >= `b`)")
    testCompileExpression(a < b, "(`a` < `b`)")
    testCompileExpression(a <= b, "(`a` <= `b`)")
    testCompileExpression(a in (b, c), "`a` IN (`b`, `c`)")
  }

  test("logical expressions") {
    testCompileExpression(!(a + b <= b), "NOT ((`a` + `b`) <= `b`)")
    testCompileExpression(a + b <= b && a > b, "(((`a` + `b`) <= `b`) AND (`a` > `b`))")
    testCompileExpression(a <= b || a + b > b, "((`a` <= `b`) OR ((`a` + `b`) > `b`))")
    testCompileExpression(a || a && !b || c, "((`a` OR (`a` AND NOT `b`)) OR `c`)")
    testCompileExpression((a || b) && !(b || c), "((`a` OR `b`) AND NOT (`b` OR `c`))")
  }

  test("aggregate expressions") {
    testCompileExpression(avg(a), "AVG(`a`)")
    testCompileExpression(count(a), "COUNT(`a`)")
    testCompileExpression(max(a), "MAX(`a`)")
    testCompileExpression(min(a), "MIN(`a`)")
    testCompileExpression(sum(a), "SUM(`a`)")
  }

  test("cast expressions") {
    testCompileExpression(nullLiteral.cast(IntegerType), "CAST(NULL AS Nullable(Int32))")
    testCompileExpression(booleanLiteral.cast(LongType), "CAST(CAST(1 AS UInt8) AS Int64)")
    testCompileExpression(booleanLiteral.cast(LongType), "CAST(CAST(1 AS UInt8) AS Int64)")

    // Comment it for hack. Will add it back after we support true date type in CH.
    // testCompileExpression(a.cast(DateType), "CAST(`a` AS Nullable(Date))")
    testCompileExpression(b.withNullability(false).cast(TimestampType), "CAST(`b` AS DateTime)")

    testCompileExpression((a + b).cast(FloatType), "CAST((`a` + `b`) AS Nullable(Float32))")
    testCompileExpression(
      (a.withNullability(false) + b).cast(DoubleType),
      "CAST((`a` + `b`) AS Nullable(Float64))"
    )
    testCompileExpression(
      (a + b.withNullability(false)).cast(StringType),
      "CAST((`a` + `b`) AS Nullable(String))"
    )
    testCompileExpression(
      (a.withNullability(false) + b.withNullability(false)).cast(ShortType),
      "CAST((`a` + `b`) AS Int16)"
    )

    testCompileExpression(
      (a.withNullability(false) + b.withNullability(false)).cast(DecimalType.FloatDecimal),
      "CAST((`a` + `b`) AS Decimal(14, 7))"
    )
  }

  test("hack expressions") {
    testCompileExpression(hackD, "`_tidb_date_d`")
    testCompileExpression(hackD > "1990-01-01", "(`_tidb_date_d` > '1990-01-01')")
  }

  test("create table statements") {
    testCreateTable(
      "Test",
      "tesT",
      new StructType(
        Array(
          StructField("I", IntegerType, false),
          StructField("D", DoubleType, true),
          StructField("s", StringType, true),
          StructField("dt", DateType, true),
          StructField("dt_NN", DateType, false)
        )
      ),
      Array("I"),
      "CREATE TABLE `test`.`test` (`i` Int32, `d` Nullable(Float64), `s` Nullable(String), `_tidb_date_dt` Nullable(Int32), `_tidb_date_dt_nn` Int32) ENGINE = MutableMergeTree((`i`), 8192)"
    )
    testCreateTable(
      "Test",
      "tesT",
      new StructType(
        Array(
          StructField("I", IntegerType, false),
          StructField("Dd", DecimalType.bounded(20, 1), false),
          StructField("dS", DecimalType.bounded(10, 0), true)
        )
      ),
      Array("I"),
      "CREATE TABLE `test`.`test` (`i` Int32, `dd` Decimal(20, 1), `ds` Nullable(Decimal(10, 0))) ENGINE = MutableMergeTree((`i`), 8192)"
    )
  }

  test("empty queries") {
    testQuery(
      t,
      CHLogicalPlan(
        Seq.empty,
        Seq.empty,
        Seq.empty,
        Seq.empty,
        Seq.empty,
        None
      ),
      "SELECT  FROM `d`.`t`"
    )
  }

  test("project queries") {
    testQuery(
      t,
      CHLogicalPlan(
        Seq(a, b, c),
        Seq.empty,
        Seq.empty,
        Seq.empty,
        Seq.empty,
        None
      ),
      "SELECT `a`, `b`, `c` FROM `d`.`t`"
    )
  }

  test("filter queries") {
    testQuery(
      t,
      CHLogicalPlan(
        Seq.empty,
        Seq(a, b, nullLiteral.isNotNull),
        Seq.empty,
        Seq.empty,
        Seq.empty,
        None
      ),
      "SELECT  FROM `d`.`t` WHERE ((`a` AND `b`) AND NULL IS NOT NULL)"
    )
  }

  test("aggregate queries") {
    testQuery(
      t,
      CHLogicalPlan(
        Seq(a, b, sum(c)),
        Seq.empty,
        Seq(a, b),
        Seq.empty,
        Seq.empty,
        None
      ),
      "SELECT `a`, `b`, SUM(`c`) FROM `d`.`t` GROUP BY `a`, `b`"
    )
  }

  test("top-n queries") {
    testQuery(
      t,
      CHLogicalPlan(
        Seq.empty,
        Seq.empty,
        Seq.empty,
        Seq.empty,
        Seq(a desc, b asc),
        None
      ),
      "SELECT  FROM `d`.`t` ORDER BY `a` DESC, `b` ASC"
    )
    testQuery(
      t,
      CHLogicalPlan(
        Seq.empty,
        Seq.empty,
        Seq.empty,
        Seq.empty,
        Seq(a asc, CreateNamedStruct(Seq(b.name, b, "col1", a + b, "col2", b + c)) desc, c asc),
        None
      ),
      "SELECT  FROM `d`.`t` ORDER BY `a` ASC, (`b`, (`a` + `b`), (`b` + `c`)) DESC, `c` ASC"
    )
    testQuery(
      t,
      CHLogicalPlan(
        Seq.empty,
        Seq.empty,
        Seq.empty,
        Seq.empty,
        Seq.empty,
        Option(1)
      ),
      "SELECT  FROM `d`.`t` LIMIT 1"
    )
    testQuery(
      t,
      CHLogicalPlan(
        Seq.empty,
        Seq.empty,
        Seq.empty,
        Seq.empty,
        Seq(a desc, b asc),
        Option(1)
      ),
      "SELECT  FROM `d`.`t` ORDER BY `a` DESC, `b` ASC LIMIT 1"
    )
  }
}
