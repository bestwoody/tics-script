package org.apache.spark.sql.execution.datasources

import org.apache.spark.memory.MemoryMode
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.ch._
import org.apache.spark.sql.execution.vectorized.{ColumnVectorUtils, ColumnarBatch}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

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
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = new Iterator[ColumnarBatch] {
    private val part = split.asInstanceOf[CHPartition]
    private val table = part.table
    private val qid = part.qid
    private val sql = CHSql.scan(table.absName, requiredColumns, filterString, aggregation, topN)
    private lazy val schema = StructType.fromAttributes(output)

    private val resp = CHExecutorPool.get(qid, sql, table.host, table.port, table.absName,
      decoderCount, encoderCount, tables.size, part.clientIndex)

    private def nextBlock(): Iterator[Row] = {
      val block = resp.executor.next()
      if (block != null) {
        block.encoded
      } else {
        null
      }
    }

    private def nextBatch(): ColumnarBatch = ColumnVectorUtils.toBatch(schema, MemoryMode.ON_HEAP, blockIterator)

    private[this] var blockIterator: Iterator[Row] = nextBlock()

    override def hasNext: Boolean = {
      if (blockIterator == null) {
        false
      } else {
        if (!blockIterator.hasNext) {
          blockIterator.asInstanceOf[CHRows].close()
          blockIterator = nextBlock()
          if (blockIterator == null) {
            CHExecutorPool.close(resp)
            false
          } else {
            blockIterator.hasNext
          }
        } else {
          true
        }
      }
    }

    override def next(): ColumnarBatch = nextBatch()
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
