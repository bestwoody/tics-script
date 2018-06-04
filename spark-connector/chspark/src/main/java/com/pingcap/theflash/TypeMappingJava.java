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
import org.apache.spark.sql.types.DataTypes;

public class TypeMappingJava {
    public static DataTypeAndNullable chTypetoSparkType(CHType chType) {
        if (chType instanceof CHTypeNullable) {
            DataTypeAndNullable t = chTypetoSparkType(((CHTypeNullable) chType).nested_data_type);
            return new DataTypeAndNullable(t.dataType, true);
        }
        if (chType == CHTypeString.instance || chType instanceof CHTypeFixedString) {
            return new DataTypeAndNullable(DataTypes.StringType);
        } else if (chType == CHTypeDate.instance) {
            return new DataTypeAndNullable(DataTypes.DateType);
        } else if (chType == CHTypeDateTime.instance) {
            return new DataTypeAndNullable(DataTypes.TimestampType);
        } else if (chType == CHTypeInt8.instance) {
            return new DataTypeAndNullable(DataTypes.ByteType);
        } else if (chType == CHTypeInt16.instance) {
            return new DataTypeAndNullable(DataTypes.ShortType);
        } else if (chType == CHTypeInt32.instance) {
            return new DataTypeAndNullable(DataTypes.IntegerType);
        } else if (chType == CHTypeInt64.instance) {
            return new DataTypeAndNullable(DataTypes.LongType);
        } else if (chType == CHTypeUInt8.instance
                || chType == CHTypeUInt16.instance) {
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
}
