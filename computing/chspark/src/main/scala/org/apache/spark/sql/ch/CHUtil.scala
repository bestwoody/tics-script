/*
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
 */

package org.apache.spark.sql.ch

import java.util.UUID

import com.pingcap.theflash.{SparkCHClientSelect, TypeMappingJava}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Abs, Add, And, AttributeReference, Cast, Divide, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, Multiply, Not, Or, Remainder, Subtract, UnaryMinus}
import org.apache.spark.sql.ch.hack.{CHAttributeReference, Hack}
import org.apache.spark.sql.types._

object CHUtil {

  def getPartitionList(table: CHTableRef): Array[String] = {
    val client = new SparkCHClientSelect(CHUtil.genQueryId("C"), CHSql.partitionList(table), table.host, table.port)
    try {
      var partitions = new Array[String](0)

      if (client.hasNext) {
        val block = client.next()
        if (block.numCols != 1) {
          throw new Exception("Send table partition list request, wrong response")
        }

        val fieldCol = block.column(0)
        for (i <- 0 until fieldCol.size()) {
          partitions :+= fieldCol.getUTF8String(i).toString
        }

        // Consume all packets before close.
        while (client.hasNext) {
          client.next()
        }
      }

      partitions
    } finally {
      client.close()
    }
  }

  def getFields(table: CHTableRef): Array[StructField] = {
    val metadata = new MetadataBuilder().putString("name", table.mappedName).build()

    var fields = new Array[StructField](0)

    var names = new Array[String](0)
    var types = new Array[String](0)

    val client = new SparkCHClientSelect(CHUtil.genQueryId("D"), CHSql.desc(table), table.host, table.port)
    try {
      while (client.hasNext) {
        val block = client.next()

        if (block.numCols() < 2) {
          throw new Exception("Send desc table to get schema failed: small column size")
        }

        val fieldCol = block.column(0)
        for (i <- 0 until fieldCol.size()) {
          names :+= fieldCol.getUTF8String(i).toString
        }

        val typeCol = block.column(1)
        for (i <- 0 until typeCol.size()) {
          types :+= typeCol.getUTF8String(i).toString
        }
      }

      if (names.length == 0) {
        throw new Exception("Send desc table to get schema failed: table desc not found")
      }
      for (i <- names.indices) {
        val t = TypeMappingJava.stringToSparkType(types(i))
        val field = Hack.hackStructField(names(i), t, metadata)
        fields :+= field
      }

      // Consume all packets before close.
      while (client.hasNext) {
        client.next()
      }

      fields
    } finally {
      client.close()
    }
  }

  def getRowCount(table: CHTableRef, useSelraw: Boolean = false): Long = {
    val client = new SparkCHClientSelect(CHUtil.genQueryId("C"), CHSql.count(table, useSelraw), table.host, table.port)
    try {
      if (!client.hasNext) {
        throw new Exception("Send table row count request, not response")
      }
      val block = client.next()
      if (block.numCols() != 1) {
        throw new Exception("Send table row count request, wrong response")
      }

      val count = block.column(0).getLong(0)

      // Consume all packets before close.
      while (client.hasNext) {
        client.next()
      }

      count
    } finally {
      client.close()
    }
  }

  // TODO: Pushdown more.
  def isSupportedExpression(exp: Expression): Boolean = {
    // println("PROBE isSupportedExpression:" + exp.getClass.getName + ", " + exp)
    exp match {
      case _: Literal => true
      case _: AttributeReference => true
      case cast @ Cast(_, _) =>
        Hack.hackSupportCast(cast)
      // TODO: Don't pushdown IsNotNull maybe better
      case IsNotNull(child) =>
        isSupportedExpression(child)
      case IsNull(child) =>
        isSupportedExpression(child)
      case UnaryMinus(child) =>
        isSupportedExpression(child)
      case Not(child) =>
        isSupportedExpression(child)
      case Abs(child) =>
        isSupportedExpression(child)
      case Add(lhs, rhs) =>
        isSupportedExpression(lhs) && isSupportedExpression(rhs)
      case Subtract(lhs, rhs) =>
        isSupportedExpression(lhs) && isSupportedExpression(rhs)
      case Multiply(lhs, rhs) =>
        isSupportedExpression(lhs) && isSupportedExpression(rhs)
      case Divide(lhs, rhs) =>
        isSupportedExpression(lhs) && isSupportedExpression(rhs)
      case Remainder(lhs, rhs) =>
        isSupportedExpression(lhs) && isSupportedExpression(rhs)
      case GreaterThan(lhs, rhs) =>
        isSupportedExpression(lhs) && isSupportedExpression(rhs)
      case GreaterThanOrEqual(lhs, rhs) =>
        isSupportedExpression(lhs) && isSupportedExpression(rhs)
      case LessThan(lhs, rhs) =>
        isSupportedExpression(lhs) && isSupportedExpression(rhs)
      case LessThanOrEqual(lhs, rhs) =>
        isSupportedExpression(lhs) && isSupportedExpression(rhs)
      case EqualTo(lhs, rhs) =>
        isSupportedExpression(lhs) && isSupportedExpression(rhs)
      case And(lhs, rhs) =>
        isSupportedExpression(lhs) && isSupportedExpression(rhs)
      case Or(lhs, rhs) =>
        isSupportedExpression(lhs) && isSupportedExpression(rhs)
      case In(value, list) =>
        isSupportedExpression(value) && list.forall(isSupportedExpression)
      case AggregateExpression(aggregateFunction, _, _, _) =>
        isSupportedAggregate(aggregateFunction)
      case _ => false
    }
  }

  def isSupportedAggregate(aggregateFunction: AggregateFunction): Boolean = {
    aggregateFunction match {
      case Average(child) => isSupportedExpression(child)
      case Count(children) => children.forall(isSupportedExpression)
      case Min(child) => isSupportedExpression(child)
      case Max(child) => isSupportedExpression(child)
      case Sum(child) => isSupportedExpression(child)
      case _ => false
    }
  }

  def genQueryId(prefix: String): String = {
    this.synchronized {
      prefix + UUID.randomUUID.toString
    }
  }

}
