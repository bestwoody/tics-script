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
                     ) extends LeafExecNode with CodegenSupport {

  override lazy val metrics = Map(
    "numOutputBlocks" -> SQLMetrics.createMetric(sparkContext, "number of output blocks"),
    "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time")
  )

  override def inputRDDs(): Seq[RDD[InternalRow]] = chScanRDD :: Nil

  override protected def doProduce(ctx: CodegenContext): String = {
    val numOutputBlocks = metricTerm(ctx, "numOutputBlocks")
    // PhysicalRDD always just has one input
    val input = ctx.freshName("input")
    ctx.addMutableState("scala.collection.Iterator", input, s"$input = inputs[0];")

    val resultClz = "org.apache.spark.sql.ch.CHExecutor.Result"
    val result = ctx.freshName("result")

    val fieldVecClz = "org.apache.arrow.vector.FieldVector"
    val columnsClz = s"java.util.List<$fieldVecClz>"
    val columns = ctx.freshName("columns")
    ctx.addMutableState(columnsClz, columns, s"$columns = null;")

    val arrowTypeClz = "org.apache.arrow.vector.types.pojo.ArrowType"
    val fieldTypesClz = s"java.util.List<$arrowTypeClz>"
    val fieldTypes = ctx.freshName("fieldTypes")
    ctx.addMutableState(fieldTypesClz, fieldTypes, s"$fieldTypes = new java.util.ArrayList<>();")

    val uint8Reverser = ctx.freshName("uint8Reverser")
    ctx.addMutableState("int", uint8Reverser, s"$uint8Reverser = 0x100;")
    val uint16Reverser = ctx.freshName("uint16Reverser")
    ctx.addMutableState("int", uint16Reverser, s"$uint16Reverser = 0x10000;")
    val uint32Reverser = ctx.freshName("uint32Reverser")
    ctx.addMutableState("long", uint32Reverser, s"$uint32Reverser = 0x100000000L;")

    s"""
       |boolean hasInitMeta = false;
       |while($input.hasNext()) {
       |  $resultClz $result = ($resultClz) $input.next();
       |  $numOutputBlocks.add(1);
       |
       |  if (!hasInitMeta) {
       |    $columns = $result.block.getFieldVectors();
       |    for ($fieldVecClz col : $columns) {
       |      $fieldTypes.add(col);
       |    }
       |    hasInitMeta = true;
       |  }
       |
       |  int rows = 0;
       |  int curr = 0;
       |  if (!$columns.isEmpty()) {
       |    rows = $columns.get(0).getAccessor().getValueCount()
       |  }
       |  while(curr < rows) {
       |
       |  }
       |  if (shouldStop()) return;
       |}
     """.stripMargin
  }

  override protected def doExecute(): RDD[InternalRow] = {
    WholeStageCodegenExec(this).execute()
  }

  // Support codegen so that we can avoid the UnsafeRow conversion in all cases. Codegen
  // never requires UnsafeRow as input.
  private def doProduceVectorized(ctx: CodegenContext): String = {
    val input = ctx.freshName("input")
    // PhysicalRDD always just has one input
    ctx.addMutableState("scala.collection.Iterator", input, s"$input = inputs[0];")

    // metrics
    val numOutputRows = metricTerm(ctx, "numOutputRows")
    val scanTimeMetric = metricTerm(ctx, "scanTime")
    val scanTimeTotalNs = ctx.freshName("scanTime")
    ctx.addMutableState("long", scanTimeTotalNs, s"$scanTimeTotalNs = 0;")

    val columnarBatchClz = "org.apache.spark.sql.execution.vectorized.ColumnarBatch"
    val batch = ctx.freshName("batch")
    ctx.addMutableState(columnarBatchClz, batch, s"$batch = null;")

    val columnVectorClz = "org.apache.spark.sql.execution.vectorized.ColumnVector"
    val idx = ctx.freshName("batchIdx")
    ctx.addMutableState("int", idx, s"$idx = 0;")
    val colVars = output.indices.map(i => ctx.freshName("colInstance" + i))
    val columnAssigns = colVars.zipWithIndex.map {
      case (name, i) =>
        ctx.addMutableState(columnVectorClz, name, s"$name = null;")
        s"$name = $batch.column($i);"
    }

    val nextBatch = ctx.freshName("nextBatch")
    ctx.addNewFunction(
      nextBatch,
      s"""
         |private void $nextBatch() throws java.io.IOException {
         |  long getBatchStart = System.nanoTime();
         |  if ($input.hasNext()) {
         |    $batch = ($columnarBatchClz)$input.next();
         |    $numOutputRows.add($batch.numRows());
         |    $idx = 0;
         |    ${columnAssigns.mkString("", "\n", "\n")}
         |  }
         |  $scanTimeTotalNs += System.nanoTime() - getBatchStart;
         |}""".stripMargin
    )

    ctx.currentVars = null
    val rowidx = ctx.freshName("rowIdx")
    val columnsBatchInput = (output zip colVars).map {
      case (attr, colVar) =>
        genCodeColumnVector(ctx, colVar, rowidx, attr.dataType, attr.nullable)
    }
    s"""
       |if ($batch == null) {
       |  $nextBatch();
       |}
       |while ($batch != null) {
       |  int numRows = $batch.numRows();
       |  while ($idx < numRows) {
       |    int $rowidx = $idx++;
       |    ${consume(ctx, columnsBatchInput).trim}
       |    if (shouldStop()) return;
       |  }
       |  $batch = null;
       |  $nextBatch();
       |}
       |$scanTimeMetric.add($scanTimeTotalNs / (1000 * 1000));
       |$scanTimeTotalNs = 0;
     """.stripMargin
  }

  private def genCodeColumnVector(ctx: CodegenContext,
                                  columnVar: String,
                                  ordinal: String,
                                  dataType: DataType,
                                  nullable: Boolean): ExprCode = {
    val javaType = ctx.javaType(dataType)
    val value = ctx.getValue(columnVar, dataType, ordinal)
    val isNullVar = if (nullable) {
      ctx.freshName("isNull")
    } else {
      "false"
    }
    val valueVar = ctx.freshName("value")
    val str = s"columnVector[$columnVar, $ordinal, ${dataType.simpleString}]"
    val code = s"${ctx.registerComment(str)}\n" + (if (nullable) {
      s"""
        boolean $isNullVar = $columnVar.isNullAt($ordinal);
        $javaType $valueVar = $isNullVar ? ${ctx.defaultValue(dataType)} : ($value);
      """
    } else {
      s"$javaType $valueVar = $value;"
    }).trim
    ExprCode(code, isNullVar, valueVar)
  }

}
