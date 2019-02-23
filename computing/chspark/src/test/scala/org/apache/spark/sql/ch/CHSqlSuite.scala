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

import com.pingcap.common.Node
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Coalesce, CreateNamedStruct, Expression, IfNull, Literal}
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
  val d: AttributeReference = 'd.byte
  val t = new CHTableRef(Node("", 0), "D", "T")

  def testCompileExpression(e: Expression, expected: String): Unit =
    assert(CHSql.compileExpression(e) == expected)

  def testCreateTable(database: String,
                      table: String,
                      schema: StructType,
                      partitionNum: Option[Int],
                      pkList: Array[String],
                      bucketNum: Int,
                      expected: String): Unit =
    assert(
      CHSql.createTableStmt(
        database,
        table,
        schema,
        MutableMergeTreeEngine(partitionNum, pkList, bucketNum),
        false
      ) == expected
    )

  def testQuery(table: CHTableRef, chLogicalPlan: CHLogicalPlan, expected: String): Unit =
    assert(CHSql.query(table, chLogicalPlan).buildQuery() == expected)

  def ignoreQuery(table: CHTableRef, chLogicalPlan: CHLogicalPlan, expected: String): Unit =
    println(s"ignored test sql: $expected")

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
    testCompileExpression(a + b, "CAST((`a` + `b`) AS Nullable(Int32))")
    testCompileExpression(-(a + b), "(-CAST((`a` + `b`) AS Nullable(Int32)))")
    testCompileExpression(
      a + b * -a,
      "CAST((`a` + CAST((`b` * (-`a`)) AS Nullable(Int32))) AS Nullable(Int32))"
    )
    testCompileExpression(
      a / b * a,
      "CAST((CAST((`a` / `b`) AS Nullable(Int32)) * `a`) AS Nullable(Int32))"
    )
    testCompileExpression(
      a / b * (a - abs(b)),
      "CAST((CAST((`a` / `b`) AS Nullable(Int32)) * CAST((`a` - CAST(abs(`b`) AS Nullable(Int32))) AS Nullable(Int32))) AS Nullable(Int32))"
    )
    testCompileExpression(
      a - b * (a / abs(b + a % 100)),
      "CAST((`a` - CAST((`b` * CAST((`a` / CAST(abs(CAST((`b` + CAST((`a` % 100) AS Nullable(Int32))) AS Nullable(Int32))) AS Nullable(Int32))) AS Nullable(Int32))) AS Nullable(Int32))) AS Nullable(Int32))"
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
    testCompileExpression(!(a + b <= b), "NOT (CAST((`a` + `b`) AS Nullable(Int32)) <= `b`)")
    testCompileExpression(
      a + b <= b && a > b,
      "((CAST((`a` + `b`) AS Nullable(Int32)) <= `b`) AND (`a` > `b`))"
    )
    testCompileExpression(
      a <= b || a + b > b,
      "((`a` <= `b`) OR (CAST((`a` + `b`) AS Nullable(Int32)) > `b`))"
    )
    testCompileExpression(a || a && !b || c, "((`a` OR (`a` AND NOT `b`)) OR `c`)")
    testCompileExpression((a || b) && !(b || c), "((`a` OR `b`) AND NOT (`b` OR `c`))")
  }

  test("aggregate expressions") {
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

    testCompileExpression(
      (a + b).cast(FloatType),
      "CAST(CAST((`a` + `b`) AS Nullable(Int32)) AS Nullable(Float32))"
    )
    testCompileExpression(
      (a.withNullability(false) + b).cast(DoubleType),
      "CAST(CAST((`a` + `b`) AS Nullable(Int32)) AS Nullable(Float64))"
    )
    testCompileExpression(
      (a + b.withNullability(false)).cast(StringType),
      "CAST(CAST((`a` + `b`) AS Nullable(Int32)) AS Nullable(String))"
    )
    testCompileExpression(
      (a.withNullability(false) + b.withNullability(false)).cast(ShortType),
      "CAST(CAST((`a` + `b`) AS Int32) AS Int16)"
    )

    testCompileExpression(
      (a.withNullability(false) + b.withNullability(false)).cast(DecimalType.FloatDecimal),
      "CAST(CAST((`a` + `b`) AS Int32) AS Decimal(14, 7))"
    )

    testCompileExpression(a + d, "CAST((`a` + `d`) AS Nullable(Int32))")

    testCompileExpression(
      (a + d) <= (a - d),
      "(CAST((`a` + `d`) AS Nullable(Int32)) <= CAST((`a` - `d`) AS Nullable(Int32)))"
    )

    testCompileExpression(
      (a + d).as("a"),
      "CAST((`a` + `d`) AS Nullable(Int32)) AS `a`"
    )

    testCompileExpression(
      (a + b + c).cast(StringType).as((a + b + c).cast(StringType).sql),
      "CAST(CAST((CAST((`a` + `b`) AS Nullable(Int32)) + `c`) AS Nullable(Int32)) AS Nullable(String)) AS `CAST(((\\`A\\` + \\`b\\`) + \\`C\\`) AS STRING)`"
    )

    // A strange Spark TypeCoercion rule that might change after update Spark version...
    testCompileExpression(
      a.withNullability(false) + d * (a.withNullability(false) + d),
      "CAST((`a` + CAST((`d` * CAST((`a` + `d`) AS Nullable(Int32))) AS Nullable(Int8))) AS Nullable(Int32))"
    )
  }

  test("test alias expressions") {
    testCompileExpression(a.as("a2"), "`a` AS `a2`")
    testCompileExpression(a.as("a b"), "`a` AS `a b`")
  }

  test("test coalesce expressions") {
    testCompileExpression(
      Coalesce(Seq(a.withNullability(false), b.withNullability(false), c)),
      "ifNull(ifNull(`a`, `b`), `c`)"
    )
    testCompileExpression(IfNull(a, b, Coalesce(Seq(a, b))), "ifNull(`a`, `b`)")
  }

  test("test attribute name") {
    val attribute_with_backtick: AttributeReference =
      AttributeReference("`a`", IntegerType, nullable = true)()
    testCompileExpression(attribute_with_backtick, "`\\`a\\``")
  }

  test("create table statements") {
    testCreateTable(
      "Test",
      "tesT",
      new StructType(
        Array(
          StructField("I am", IntegerType, false),
          StructField("D", DoubleType, true),
          StructField("s", StringType, true),
          StructField("dt", DateType, true),
          StructField("dt_NN", DateType, false)
        )
      ),
      Some(64),
      Array("I am"),
      8192,
      "CREATE TABLE `test`.`test` (`i am` Int32, `d` Nullable(Float64), `s` Nullable(String), `dt` Nullable(Date), `dt_nn` Date) ENGINE = MutableMergeTree(64, (`i am`), 8192)"
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
      None,
      Array("I"),
      128,
      "CREATE TABLE `test`.`test` (`i` Int32, `dd` Decimal(20, 1), `ds` Nullable(Decimal(10, 0))) ENGINE = MutableMergeTree((`i`), 128)"
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
      "SELECT  FROM `d`.`t` ORDER BY `a` DESC NULLS LAST, `b` ASC NULLS FIRST"
    )
    ignoreQuery(
      t,
      CHLogicalPlan(
        Seq.empty,
        Seq.empty,
        Seq.empty,
        Seq.empty,
        Seq(a asc, CreateNamedStruct(Seq(b.name, b, "col1", a + b, "col2", b + c)) desc, c asc),
        None
      ),
      "SELECT  FROM `d`.`t` ORDER BY `a` ASC NULLS FIRST, (`b`, (`a` + `b`), (`b` + `c`)) DESC NULLS LAST, `c` ASC NULLS FIRST"
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
      "SELECT  FROM `d`.`t` ORDER BY `a` DESC NULLS LAST, `b` ASC NULLS FIRST LIMIT 1"
    )
  }
}
