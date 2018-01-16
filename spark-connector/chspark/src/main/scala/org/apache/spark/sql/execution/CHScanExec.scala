package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.ch.{CHSqlAgg, CHSqlTopN, CHTableRef}
import org.apache.spark.sql.execution.datasources.CHScanRDD
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.DataType

case class CHScanExec(output: Seq[Attribute],
                      chScanRDD: CHScanRDD,
                      @transient private val sparkSession: SparkSession,
                      @transient private val tables: Seq[CHTableRef],
                      @transient private val requiredColumns: Seq[String],
                      @transient private val filterString: String,
                      @transient private val aggregation: CHSqlAgg,
                      @transient private val topN: CHSqlTopN,
                      @transient private val partitions: Int,
                      @transient private val decoders: Int,
                      @transient private val encoders: Int
                     ) extends LeafExecNode with ColumnarBatchScan {

  override def inputRDDs(): Seq[RDD[InternalRow]] = chScanRDD :: Nil

  override protected def doProduce(ctx: CodegenContext): String = {
    super.doProduce(ctx)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    WholeStageCodegenExec(this).execute()
  }
}
