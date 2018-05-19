package com.pingcap.theflash;

import com.pingcap.ch.datatypes.CHType;
import com.pingcap.ch.datatypes.CHTypeDate;
import com.pingcap.ch.datatypes.CHTypeDateTime;
import com.pingcap.ch.datatypes.CHTypeFixedString;
import com.pingcap.ch.datatypes.CHTypeNullable;
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
import com.pingcap.ch.datatypes.CHTypeString;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class TypeMappingJava {
    public static DataType chTypetoSparkType(CHType chType) {
        if (chType instanceof CHTypeNullable) {
            return chTypetoSparkType(((CHTypeNullable) chType).nested_data_type);
        }
        if (chType == CHTypeString.instance || chType instanceof CHTypeFixedString) {
            return DataTypes.StringType;
        } else if (chType == CHTypeDate.instance) {
            return DataTypes.DateType;
        } else if (chType == CHTypeDateTime.instance) {
            return DataTypes.TimestampType;
        } else if (chType == CHTypeInt8.instance) {
            return DataTypes.ByteType;
        } else if (chType == CHTypeInt16.instance) {
            return DataTypes.ShortType;
        } else if (chType == CHTypeInt32.instance) {
            return DataTypes.IntegerType;
        } else if (chType == CHTypeInt64.instance) {
            return DataTypes.LongType;
        } else if (chType == CHTypeUInt8.instance
                || chType == CHTypeUInt16.instance) {
            return DataTypes.IntegerType;
        } else if (chType == CHTypeUInt32.instance) {
            return DataTypes.LongType;
        } else if (chType == CHTypeUInt64.instance) {
            return DataTypes.createDecimalType(20, 0);
        } else if (chType == CHTypeFloat32.instance) {
            return DataTypes.FloatType;
        } else if (chType == CHTypeFloat64.instance) {
            return DataTypes.DoubleType;
        } else {
            throw new UnsupportedOperationException("Unsupported data type: " + chType.name());
        }
    }

    public static DataType stringToSparkType(String name) throws Exception {
        // May have bugs: promote unsiged types, and ignore uint64 overflow
        // TODO: Support all types
        if (name.startsWith("Nullable")) {
            String remain = StringUtils.removeStart(name, "Nullable");
            remain = StringUtils.removeEnd(StringUtils.removeStart(remain, "("), ")");
            return stringToSparkType(remain);
        }
        if (name.startsWith("FixedString")) {
            return DataTypes.StringType;
        } else {
            switch (name) {
                case "String":
                    return DataTypes.StringType;
                case "DateTime":
                    return DataTypes.TimestampType;
                case "Date":
                    return DataTypes.DateType;
                case "Int8":
                    return DataTypes.ByteType;
                case "Int16":
                    return DataTypes.ShortType;
                case "Int32":
                    return DataTypes.IntegerType;
                case "Int64":
                    return DataTypes.LongType;
                case "UInt8":
                    return DataTypes.IntegerType;
                case "UInt16":
                    return DataTypes.IntegerType;
                case "UInt32":
                    return DataTypes.LongType;
                case "UInt64":
                    return DataTypes.createDecimalType(20, 0);
                case "Float32":
                    return DataTypes.FloatType;
                case "Float64":
                    return DataTypes.DoubleType;
                default:
                    throw new Exception("stringToFieldType unhandled type name: " + name);
            }
        }
    }
}
