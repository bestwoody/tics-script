package com.pingcap.theflash;

import com.pingcap.ch.datatypes.*;
import com.pingcap.ch.datatypes.CHTypeNumber.CHTypeFloat32;
import com.pingcap.ch.datatypes.CHTypeNumber.CHTypeFloat64;
import com.pingcap.ch.datatypes.CHTypeNumber.CHTypeInt16;
import com.pingcap.ch.datatypes.CHTypeNumber.CHTypeInt32;
import com.pingcap.ch.datatypes.CHTypeNumber.CHTypeInt64;
import com.pingcap.ch.datatypes.CHTypeNumber.CHTypeInt8;
import com.pingcap.ch.datatypes.CHTypeNumber.CHTypeUInt16;
import com.pingcap.ch.datatypes.CHTypeNumber.CHTypeUInt32;
import com.pingcap.ch.datatypes.CHTypeNumber.CHTypeUInt64;
import com.pingcap.ch.datatypes.CHTypeNumber.CHTypeUInt8;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.types.*;

public class TypeMappingJava {
  private static class PrecisionAndScale {
    private int precision;
    private int scale;

    private PrecisionAndScale(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
    }

    // Recalculate CH Decimal precision and scale to fit into Spark Decimal range,
    // e.g., Decimal(65,20) will be transformed into Decimal(38, -7), and values
    // within 1.0E+7 will be truncated to zero
    private static PrecisionAndScale fromCHToSpark(int precision, int scale) {
      int adjusted_precision = precision;
      int adjusted_scale = scale;
      if (precision > DecimalType.MAX_PRECISION()) {
        adjusted_precision = DecimalType.MAX_PRECISION();
        adjusted_scale = scale - precision + DecimalType.MAX_PRECISION();
      }
      if (adjusted_scale > DecimalType.MAX_SCALE()) {
        throw new RuntimeException(
            "CH Decimal ("
                + precision
                + ", "
                + scale
                + ") adjusted to ("
                + adjusted_precision
                + ", "
                + adjusted_scale
                + ") scale not supported");
      }
      return new PrecisionAndScale(adjusted_precision, adjusted_scale);
    }

    // Recalculate Spark Decimal precision and scale to fit into CH Decimal range,
    // i.e., negative scale is not allowed in CH so we extend precision by moving scale to zero.
    private static PrecisionAndScale fromSparkToCH(int precision, int scale) {
      int adjusted_precision = precision;
      int adjusted_scale = scale;
      if (scale < 0) {
        adjusted_precision = precision - scale;
        adjusted_scale = 0;
      }
      if (adjusted_precision > CHTypeDecimal.MAX_PRECISION
          || adjusted_scale > CHTypeDecimal.MAX_SCALE) {
        throw new RuntimeException(
            "Spark Decimal ("
                + precision
                + ", "
                + scale
                + ") adjusted to ("
                + adjusted_precision
                + ", "
                + adjusted_scale
                + ") scale not supported");
      }
      return new PrecisionAndScale(adjusted_precision, adjusted_scale);
    }
  }

  public static DataTypeAndNullable chTypeToSparkType(CHType chType) {
    if (chType instanceof CHTypeNullable) {
      DataTypeAndNullable t = chTypeToSparkType(((CHTypeNullable) chType).nested_data_type);
      return new DataTypeAndNullable(t.dataType, true);
    }
    if (chType == CHTypeString.instance || chType instanceof CHTypeFixedString) {
      return new DataTypeAndNullable(DataTypes.StringType);
    } else if (chType instanceof CHTypeDecimal) {
      PrecisionAndScale p =
          PrecisionAndScale.fromCHToSpark(
              ((CHTypeDecimal) chType).precision, ((CHTypeDecimal) chType).scale);
      return new DataTypeAndNullable(DataTypes.createDecimalType(p.precision, p.scale));
    } else if (chType == CHTypeDate.instance) {
      return new DataTypeAndNullable(DataTypes.DateType);
    } else if (chType == CHTypeMyDate.instance) {
      return new DataTypeAndNullable(DataTypes.DateType);
    } else if (chType == CHTypeDateTime.instance) {
      return new DataTypeAndNullable(DataTypes.TimestampType);
    } else if (chType == CHTypeMyDateTime.instance) {
      return new DataTypeAndNullable(DataTypes.TimestampType);
    } else if (chType == CHTypeInt8.instance) {
      return new DataTypeAndNullable(DataTypes.ByteType);
    } else if (chType == CHTypeInt16.instance) {
      return new DataTypeAndNullable(DataTypes.ShortType);
    } else if (chType == CHTypeInt32.instance) {
      return new DataTypeAndNullable(DataTypes.IntegerType);
    } else if (chType == CHTypeInt64.instance) {
      return new DataTypeAndNullable(DataTypes.LongType);
    } else if (chType == CHTypeUInt8.instance || chType == CHTypeUInt16.instance) {
      return new DataTypeAndNullable(DataTypes.IntegerType);
    } else if (chType == CHTypeUInt32.instance) {
      return new DataTypeAndNullable(DataTypes.LongType);
    } else if (chType == CHTypeUInt64.instance) {
      return new DataTypeAndNullable(DataTypes.createDecimalType(20, 0));
    } else if (chType == CHTypeFloat32.instance) {
      return new DataTypeAndNullable(DataTypes.FloatType);
    } else if (chType == CHTypeFloat64.instance) {
      return new DataTypeAndNullable(DataTypes.DoubleType);
    } else {
      throw new UnsupportedOperationException("Unsupported data type: " + chType.name());
    }
  }

  public static DataTypeAndNullable stringToSparkType(String name) throws Exception {
    // May have bugs: promote unsiged types, and ignore uint64 overflow
    // TODO: Support all types
    if (name.startsWith("Nullable")) {
      String remain = StringUtils.removeStart(name, "Nullable");
      remain = StringUtils.removeEnd(StringUtils.removeStart(remain, "("), ")");
      return new DataTypeAndNullable(stringToSparkType(remain).dataType, true);
    }
    if (name.startsWith("FixedString")) {
      return new DataTypeAndNullable(DataTypes.StringType);
    } else if (name.startsWith("Decimal")) {
      String remain = StringUtils.removeStart(name, "Decimal");
      remain = StringUtils.removeEnd(StringUtils.removeStart(remain, "("), ")");
      String[] parts = remain.split(",");
      PrecisionAndScale p =
          PrecisionAndScale.fromCHToSpark(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
      return new DataTypeAndNullable(DataTypes.createDecimalType(p.precision, p.scale));
    } else {
      switch (name) {
        case "String":
          return new DataTypeAndNullable(DataTypes.StringType);
        case "DateTime":
          return new DataTypeAndNullable(DataTypes.TimestampType);
        case "Date":
          return new DataTypeAndNullable(DataTypes.DateType);
        case "Int8":
          return new DataTypeAndNullable(DataTypes.ByteType);
        case "Int16":
          return new DataTypeAndNullable(DataTypes.ShortType);
        case "Int32":
          return new DataTypeAndNullable(DataTypes.IntegerType);
        case "Int64":
          return new DataTypeAndNullable(DataTypes.LongType);
        case "UInt8":
          return new DataTypeAndNullable(DataTypes.IntegerType);
        case "UInt16":
          return new DataTypeAndNullable(DataTypes.IntegerType);
        case "UInt32":
          return new DataTypeAndNullable(DataTypes.LongType);
        case "UInt64":
          return new DataTypeAndNullable(DataTypes.createDecimalType(20, 0));
        case "Float32":
          return new DataTypeAndNullable(DataTypes.FloatType);
        case "Float64":
          return new DataTypeAndNullable(DataTypes.DoubleType);
        default:
          throw new Exception("stringToFieldType unhandled type name: " + name);
      }
    }
  }

  private static Map<Class<? extends DataType>, CHType> sparkTypeToCHTypeMap = new HashMap<>();
  private static Map<Class<? extends DataType>, CHTypeNullable> sparkTypeToCHTypeNullableMap =
      new HashMap<>();

  static {
    sparkTypeToCHTypeMap.put(ByteType$.class, CHTypeInt8.instance);
    sparkTypeToCHTypeNullableMap.put(ByteType$.class, CHTypeInt8.nullableInstance);
    sparkTypeToCHTypeMap.put(IntegerType$.class, CHTypeInt32.instance);
    sparkTypeToCHTypeNullableMap.put(IntegerType$.class, CHTypeInt32.nullableInstance);
    sparkTypeToCHTypeMap.put(LongType$.class, CHTypeInt64.instance);
    sparkTypeToCHTypeNullableMap.put(LongType$.class, CHTypeInt64.nullableInstance);
    sparkTypeToCHTypeMap.put(DateType$.class, CHTypeDate.instance);
    sparkTypeToCHTypeNullableMap.put(DateType$.class, CHTypeDate.nullableInstance);
    sparkTypeToCHTypeMap.put(TimestampType$.class, CHTypeDateTime.instance);
    sparkTypeToCHTypeNullableMap.put(TimestampType$.class, CHTypeDateTime.nullableInstance);
    sparkTypeToCHTypeMap.put(FloatType$.class, CHTypeFloat32.instance);
    sparkTypeToCHTypeNullableMap.put(FloatType$.class, CHTypeFloat32.nullableInstance);
    sparkTypeToCHTypeMap.put(DoubleType$.class, CHTypeFloat64.instance);
    sparkTypeToCHTypeNullableMap.put(DoubleType$.class, CHTypeFloat64.nullableInstance);
    sparkTypeToCHTypeMap.put(StringType$.class, CHTypeString.instance);
    sparkTypeToCHTypeNullableMap.put(StringType$.class, CHTypeString.nullableInstance);
    sparkTypeToCHTypeMap.put(BooleanType$.class, CHTypeUInt8.instance);
    sparkTypeToCHTypeNullableMap.put(BooleanType$.class, CHTypeUInt8.nullableInstance);
    sparkTypeToCHTypeMap.put(ShortType$.class, CHTypeInt16.instance);
    sparkTypeToCHTypeNullableMap.put(ShortType$.class, CHTypeInt16.nullableInstance);
  }

  /**
   * Converts a Spark DataType into CH Type
   *
   * @param dataType spark data type
   * @return corresponding CHType
   */
  public static CHType sparkTypeToCHType(DataType dataType, boolean nullable) {
    if (dataType instanceof DecimalType) {
      DecimalType decimalType = (DecimalType) dataType;
      PrecisionAndScale p =
          PrecisionAndScale.fromSparkToCH(decimalType.precision(), decimalType.scale());
      CHTypeDecimal chTypeDecimal = new CHTypeDecimal(p.precision, p.scale);
      if (nullable) {
        return new CHTypeNullable(chTypeDecimal);
      }
      return chTypeDecimal;
    }
    if (nullable) {
      if (sparkTypeToCHTypeNullableMap.containsKey(dataType.getClass())) {
        return sparkTypeToCHTypeNullableMap.get(dataType.getClass());
      }
    } else {
      if (sparkTypeToCHTypeMap.containsKey(dataType.getClass())) {
        return sparkTypeToCHTypeMap.get(dataType.getClass());
      }
    }
    throw new UnsupportedOperationException(
        "Target dataType " + dataType + " for Cast is not supported.");
  }
}
