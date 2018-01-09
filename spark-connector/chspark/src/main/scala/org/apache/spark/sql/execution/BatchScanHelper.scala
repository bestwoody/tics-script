package org.apache.spark.sql.execution

import java.math.BigDecimal
import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}
import java.util

import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

object BatchScanHelper {
  private def appendValue(dst: ColumnVector, t: DataType, o: Any): Unit = {
    if (o == null) if (t.isInstanceOf[CalendarIntervalType]) dst.appendStruct(true)
    else dst.appendNull
    else if (t eq DataTypes.BooleanType) dst.appendBoolean(o.asInstanceOf[Boolean])
    else if (t eq DataTypes.ByteType) dst.appendByte(o.asInstanceOf[Byte])
    else if (t eq DataTypes.ShortType) dst.appendShort(o.asInstanceOf[Short])
    else if (t eq DataTypes.IntegerType) dst.appendInt(o.asInstanceOf[Integer])
    else if (t eq DataTypes.LongType) dst.appendLong(o.asInstanceOf[Long])
    else if (t eq DataTypes.FloatType) dst.appendFloat(o.asInstanceOf[Float])
    else if (t eq DataTypes.DoubleType) dst.appendDouble(o.asInstanceOf[Double])
    else if (t eq DataTypes.StringType) {
      val b = o.asInstanceOf[String].getBytes(StandardCharsets.UTF_8)
      dst.appendByteArray(b, 0, b.length)
    }
    else t match {
      case dt: DecimalType =>
        val d = Decimal.apply(o.asInstanceOf[BigDecimal], dt.precision, dt.scale)
        if (dt.precision <= Decimal.MAX_INT_DIGITS) dst.appendInt(d.toUnscaledLong.toInt)
        else if (dt.precision <= Decimal.MAX_LONG_DIGITS) dst.appendLong(d.toUnscaledLong)
        else {
          val integer = d.toJavaBigDecimal.unscaledValue
          val bytes = integer.toByteArray
          dst.appendByteArray(bytes, 0, bytes.length)
        }
      case _ => t match {
        case _: CalendarIntervalType =>
          val c = o.asInstanceOf[CalendarInterval]
          dst.appendStruct(false)
          dst.getChildColumn(0).appendInt(c.months)
          dst.getChildColumn(1).appendLong(c.microseconds)
        case _: DateType => dst.appendInt(DateTimeUtils.fromJavaDate(o.asInstanceOf[Date]))
        case _: TimestampType => dst.appendLong(DateTimeUtils.fromJavaTimestamp(o.asInstanceOf[Timestamp]))
        case _ => throw new UnsupportedOperationException("Type " + t)
      }
    }
  }

  private def appendValue(dst: ColumnVector, t: DataType, src: Row, fieldIdx: Int): Unit = t match {
    case at: ArrayType =>
      if (src.isNullAt(fieldIdx)) dst.appendNull
      else {
        val values = src.getList(fieldIdx)
        dst.appendArray(values.size)
        import scala.collection.JavaConversions._
        for (o <- values) {
          appendValue(dst.arrayData, at.elementType, o)
        }
      }
    case st: StructType =>
      if (src.isNullAt(fieldIdx)) dst.appendStruct(true)
      else {
        dst.appendStruct(false)
        val c = src.getStruct(fieldIdx)
        for (i <- st.fields.indices) {
          appendValue(dst.getChildColumn(i), st.fields(i).dataType, c, i)
        }
      }
    case _ => appendValue(dst, t, src.get(fieldIdx))
  }

  /**
    * Converts an iterator of rows into a single ColumnBatch.
    */
  def toBatch(schema: StructType, memMode: MemoryMode, row: util.Iterator[Row]): ColumnarBatch = {
    val batch = ColumnarBatch.allocate(schema, memMode)
    var n = 0
    while (row.hasNext) {
      val r = row.next()
      for (i <- schema.fields.indices) {
        appendValue(batch.column(i), schema.fields(i).dataType, r, i)
      }
      n += 1
    }
    batch.setNumRows(n)
    batch
  }

}
