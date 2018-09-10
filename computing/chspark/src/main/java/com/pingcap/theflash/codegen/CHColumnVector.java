package com.pingcap.theflash.codegen;

import com.google.common.primitives.UnsignedLong;
import com.pingcap.ch.columns.CHColumn;
import com.pingcap.ch.columns.CHColumnNullable;
import com.pingcap.ch.columns.CHColumnNumber;
import com.pingcap.ch.columns.CHColumnWithTypeAndName;
import com.pingcap.ch.datatypes.CHType;
import com.pingcap.ch.datatypes.CHTypeDateTime;
import com.pingcap.ch.datatypes.CHTypeDecimal;
import com.pingcap.ch.datatypes.CHTypeNullable;
import com.pingcap.ch.datatypes.CHTypeNumber;
import com.pingcap.theflash.TypeMappingJava;

import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigInteger;

/**
 * ClickHouse data to Spark data adapter.
 */
public class CHColumnVector extends ColumnVector {
    private CHColumnNumber nullMap;
    private CHColumn column;
    private CHType type;

    public CHColumnVector(CHColumnWithTypeAndName column) {
        super(TypeMappingJava.chTypetoSparkType(column.dataType()).dataType);
        CHType originalType = column.dataType();
        if (originalType instanceof CHTypeNullable) {
            this.type = ((CHTypeNullable) originalType).nested_data_type;
            this.column = ((CHColumnNullable) column.column()).nested_column;
            this.nullMap = (CHColumnNumber) ((CHColumnNullable) column.column()).null_map;
        } else {
            this.column = column.column();
            this.type = column.dataType();
        }
    }

    public int size() {
        return column.size();
    }

    @Override
    public void close() {}

    @Override
    public int numNulls() {
        return 0;
    }

    @Override
    public boolean isNullAt(int rowId) {
        return nullMap != null && nullMap.getByte(rowId) != 0;
    }

    @Override
    public boolean getBoolean(int rowId) {
        return column.getByte(rowId) != 0;
    }

    @Override
    public boolean[] getBooleans(int rowId, int count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte getByte(int rowId) {
        return column.getByte(rowId);
    }

    @Override
    public byte[] getBytes(int rowId, int count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(int rowId) {
        return column.getShort(rowId);
    }

    @Override
    public short[] getShorts(int rowId, int count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(int rowId) {
        if (type == CHTypeNumber.CHTypeUInt8.instance) {
            // CHTypeUInt8 -> Spark IntegerType
            return column.getByte(rowId) & 0x0FF;
            // CHTypeUInt16 -> Spark IntegerType
        } else if (type == CHTypeNumber.CHTypeUInt16.instance) {
            return column.getShort(rowId) & 0x0FFFF;
        }
        return column.getInt(rowId);
    }

    @Override
    public int[] getInts(int rowId, int count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int rowId) {
        if (type == CHTypeDateTime.instance) {
            // Spark store Timestamp as microseconds, while ClickHosue as seconds.
            return column.getLong(rowId) * 1000 * 1000;
        } else if (type == CHTypeNumber.CHTypeUInt32.instance) {
            // CHTypeUInt32 -> Spark LongType
            return column.getInt(rowId) & 0x0FFFF_FFFFL;
        }
        return column.getLong(rowId);
    }

    @Override
    public long[] getLongs(int rowId, int count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(int rowId) {
        if (type == CHTypeNumber.CHTypeFloat64.instance) {
            // In some case like min(tp_int8 + tpfloat32 * 2), CH cast the result as Float64 while Spark not.
            // It is a work around here.
            return (float) column.getDouble(rowId);
        }
        return column.getFloat(rowId);
    }

    @Override
    public float[] getFloats(int rowId, int count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int rowId) {
        return column.getDouble(rowId);
    }

    @Override
    public double[] getDoubles(int rowId, int count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getArrayLength(int rowId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getArrayOffset(int rowId) {
        throw new UnsupportedOperationException();
    }

    private static BigInteger toUnsignedBigInteger(long i) {
        if (i >= 0L)
            return BigInteger.valueOf(i);
        else {
            return UnsignedLong.fromLongBits(i).bigIntegerValue();
        }
    }

    @Override
    public Decimal getDecimal(int rowId, int precision, int scale) {
        if (type == CHTypeNumber.CHTypeUInt64.instance) {
            long v = column.getLong(rowId);
            if (v >= 0) {
                return new Decimal().set(v);
            } else {
                return Decimal.apply(new java.math.BigDecimal(toUnsignedBigInteger(v)));
                //return new Decimal().set(BigDecimal.exact(new java.math.BigDecimal(toUnsignedBigInteger(v))));
            }
        } else if (type == CHTypeNumber.CHTypeFloat64.instance) {
            return Decimal.apply(column.getDouble(rowId));
        } else if (type instanceof CHTypeDecimal) {
            Decimal result = column.getDecimal(rowId);
            if (result.changePrecision(precision, scale)) {
                return result;
            } else {
                throw new IllegalArgumentException("Decimal value overflowed");
            }
        }

        throw new UnsupportedOperationException();
    }

    @Override
    public UTF8String getUTF8String(int rowId) {
        return column.getUTF8String(rowId);
    }

    @Override
    public byte[] getBinary(int rowId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnVector arrayData() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnVector getChildColumn(int ordinal) {
        throw new UnsupportedOperationException();
    }


}
