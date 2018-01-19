package org.apache.spark.sql.execution

import org.apache.hadoop.hive.metastore.api.InvalidOperationException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.ch.{CHRDD, CHSqlAgg, CHSqlTopN, CHTableRef}
import org.apache.spark.sql.execution.datasources.CHScanRDD

case class CHScanExec(
                       output: Seq[Attribute],
                       chScanRDD: CHScanRDD,
                       @transient private val sparkSession: SparkSession,
                       @transient private val tables: Seq[CHTableRef],
                       @transient private val requiredColumns: Seq[String],
                       @transient private val filterString: String,
                       @transient private val aggregation: CHSqlAgg,
                       @transient private val topN: CHSqlTopN,
                       @transient private val partitions: Int,
                       @transient private val decoders: Int,
                       @transient private val encoders: Int,
                       private val enableCodeGen: Boolean = true)
  extends LeafExecNode with ArrowBatchScan {

  override def inputRDDs(): Seq[RDD[InternalRow]] = chScanRDD :: Nil

  override protected def doProduce(ctx: CodegenContext): String = {
    if (enableCodeGen) {
      super.doProduce(ctx)
    } else {
      // Should never reach here
      throw new InvalidOperationException("Code Generation is not enabled in this plan!")
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    if (enableCodeGen) {
      WholeStageCodegenExec(this).execute()
    } else {
      val numOutputRows = longMetric("numOutputRows")
      val types = schema.fields.map(_.dataType)
      val rdd = new CHRDD(sparkSession, tables, requiredColumns, filterString,
        aggregation, topN, partitions, decoders, encoders)

      val result = RDDConversions.rowToRowRdd(rdd, types)

      result.mapPartitionsWithIndexInternal { (partition, iter) =>
        val proj = UnsafeProjection.create(schema)
        proj.initialize(partition)
        iter.map { r =>
          numOutputRows += 1
          proj(r)
        }
      }
    }
  }
}
