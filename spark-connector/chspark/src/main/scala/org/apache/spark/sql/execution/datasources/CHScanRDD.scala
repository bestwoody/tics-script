package org.apache.spark.sql.execution.datasources

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.ch._
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Partition, TaskContext}

import scala.collection.mutable.ListBuffer

class CHScanRDD(@transient private val sparkSession: SparkSession,
                val output: Seq[Attribute],
                val tables: Seq[CHTableRef],
                private val requiredColumns: Seq[String],
                private val filterString: String,
                private val aggregation: CHSqlAgg,
                private val topN: CHSqlTopN,
                private val partitionCount: Int,
                private val decoderCount: Int,
                private val encoderCount: Int) extends RDD[InternalRow](sparkSession.sparkContext, Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = new Iterator[CHExecutor.Result] {
    private val part = split.asInstanceOf[CHPartition]
    private val table = part.table
    private val qid = part.qid
    private val sql = CHSql.scan(table.absName, requiredColumns, filterString, aggregation, topN)
    private lazy val schema = StructType.fromAttributes(output)

    private val resp = CHExecutorPool.get(qid, sql, table.host, table.port, table.absName,
      decoderCount, encoderCount, tables.size, part.clientIndex)

    private def nextResult(): CHExecutor.Result = {
      val block = resp.executor.next()
      if (block != null) {
        block.result()
      } else {
        null
      }
    }

    private[this] var curResult: CHExecutor.Result = nextResult()

    override def hasNext: Boolean = {
      if (curResult == null) {
        false
      } else {
        curResult.asInstanceOf[CHRows].close()
        curResult = nextResult()
        if (curResult == null) {
          CHExecutorPool.close(resp)
          false
        } else {
          curResult.isEmpty
        }
      }
    }

    override def next(): CHExecutor.Result = curResult
  }.asInstanceOf[Iterator[InternalRow]]

  // TODO: All paritions may not assign to a same Spark node, so we need a better session module, like:
  // <Spark nodes>-<share handle of a query> --- <CH sessions, each session respond to a handle>
  override protected def getPreferredLocations(split: Partition): Seq[String] =
    split.asInstanceOf[CHPartition].table.host :: Nil

  override protected def getPartitions: Array[Partition] = {
    // TODO: Read cluster info from CH masterH
    val qid = CHUtil.genQueryId
    val result = new ListBuffer[CHPartition]
    var index = 0
    tables.foreach(table => {
      for (i <- 0 until partitionCount) {
        result.append(new CHPartition(index, table, qid, i))
        index += 1
      }
    })
    result.toArray
  }
}
