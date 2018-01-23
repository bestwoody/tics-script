package org.apache.spark.sql.execution.arrow

import scala.collection.JavaConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import org.apache.spark.sql.types.{DataType, StructType, StructField}

import org.apache.spark.sql.types.{ByteType, ShortType, IntegerType, LongType}
import org.apache.spark.sql.types.{FloatType, DoubleType, DecimalType}
import org.apache.spark.sql.types.{DateType, TimestampType}
import org.apache.spark.sql.types.{BooleanType, StringType, BinaryType, ArrayType}

object ArrowUtils {
  // Maps data type from Spark to Arrow. NOTE: timeZoneId required for TimestampTypes
  def toArrowType(dt: DataType, timeZoneId: String): ArrowType = dt match {
    case BooleanType => ArrowType.Bool.INSTANCE
    case ByteType => new ArrowType.Int(8, true)
    case ShortType => new ArrowType.Int(8 * 2, true)
    case IntegerType => new ArrowType.Int(8 * 4, true)
    case LongType => new ArrowType.Int(8 * 8, true)
    case FloatType => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    case DoubleType => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    case StringType => ArrowType.Utf8.INSTANCE
    case BinaryType => ArrowType.Binary.INSTANCE
    case DecimalType.Fixed(precision, scale) => new ArrowType.Decimal(precision, scale)
    case DateType => new ArrowType.Date(DateUnit.DAY)
    case TimestampType =>
      if (timeZoneId == null) {
        throw new UnsupportedOperationException("TimestampType must supply timeZoneId parameter")
      } else {
        new ArrowType.Timestamp(TimeUnit.MICROSECOND, timeZoneId)
      }
    case _ => throw new UnsupportedOperationException(s"Unsupported data type: ${dt.simpleString}")
  }

  def fromArrowType(dt: ArrowType): DataType = dt match {
    case ArrowType.Bool.INSTANCE => BooleanType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 1 => ByteType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 2 => ShortType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 4 => IntegerType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 8 => LongType
    // Promote
    case int: ArrowType.Int if !int.getIsSigned && int.getBitWidth == 8 * 1 => IntegerType
    // Promote
    case int: ArrowType.Int if !int.getIsSigned && int.getBitWidth == 8 * 2 => IntegerType
    // Promote
    case int: ArrowType.Int if !int.getIsSigned && int.getBitWidth == 8 * 4 => LongType
    // May overflow, unchecked
    case int: ArrowType.Int if !int.getIsSigned && int.getBitWidth == 8 * 8 => LongType
    case float: ArrowType.FloatingPoint
      if float.getPrecision == FloatingPointPrecision.SINGLE => FloatType
    case float: ArrowType.FloatingPoint
      if float.getPrecision == FloatingPointPrecision.DOUBLE => DoubleType
    case ArrowType.Utf8.INSTANCE => StringType
    case ArrowType.Binary.INSTANCE => BinaryType
    case d: ArrowType.Decimal => DecimalType(d.getPrecision, d.getScale)
    case date: ArrowType.Date if date.getUnit == DateUnit.DAY => DateType
    case ts: ArrowType.Timestamp if ts.getUnit == TimeUnit.MICROSECOND => TimestampType
    case _: ArrowType.Time => TimestampType
    case _ => throw new UnsupportedOperationException(s"Unsupported data type: $dt")
  }

  // Maps field from Spark to Arrow. NOTE: timeZoneId required for TimestampType
  def toArrowField(name: String, dt: DataType, nullable: Boolean, timeZoneId: String): Field = {
    dt match {
      case ArrayType(elementType, containsNull) =>
        val fieldType = new FieldType(nullable, ArrowType.List.INSTANCE, null)
        new Field(name, fieldType,
          Seq(toArrowField("element", elementType, containsNull, timeZoneId)).asJava)
      case StructType(fields) =>
        val fieldType = new FieldType(nullable, ArrowType.Struct.INSTANCE, null)
        new Field(name, fieldType,
          fields.map { field =>
            toArrowField(field.name, field.dataType, field.nullable, timeZoneId)
          }.toSeq.asJava)
      case dataType =>
        val fieldType = new FieldType(nullable, toArrowType(dataType, timeZoneId), null)
        new Field(name, fieldType, Seq.empty[Field].asJava)
    }
  }

  def fromArrowField(field: Field): DataType = {
    field.getType match {
      case ArrowType.List.INSTANCE =>
        val elementField = field.getChildren.get(0)
        val elementType = fromArrowField(elementField)
        ArrayType(elementType, containsNull = elementField.isNullable)
      case ArrowType.Struct.INSTANCE =>
        val fields = field.getChildren.asScala.map { child =>
          val dt = fromArrowField(child)
          StructField(child.getName, dt, child.isNullable)
        }
        StructType(fields)
      case arrowType => fromArrowType(arrowType)
    }
  }

  // Maps schema from Spark to Arrow. NOTE: timeZoneId required for TimestampType in StructType
  def toArrowSchema(schema: StructType, timeZoneId: String): Schema = {
    new Schema(schema.map { field =>
      toArrowField(field.name, field.dataType, field.nullable, timeZoneId)
    }.asJava)
  }

  def fromArrowSchema(schema: Schema): StructType = {
    StructType(schema.getFields.asScala.map { field =>
      val dt = fromArrowField(field)
      StructField(field.getName, dt, field.isNullable)
    })
  }
}