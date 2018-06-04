package com.pingcap.theflash;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Simple row used for test.
 */
public class SimpleRow extends InternalRow {
    private Object[] fields;

    public SimpleRow(Object[] fields) {
        this.fields = fields;
    }

    @Override
    public int numFields() {
        return fields.length;
    }

    @Override
    public void setNullAt(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void update(int i, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalRow copy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean anyNull() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNullAt(int ordinal) {
        return fields[ordinal] == null;
    }

    @Override
    public boolean getBoolean(int ordinal) {
        return (Boolean) fields[ordinal];
    }

    @Override
    public byte getByte(int ordinal) {
        return (Byte) fields[ordinal];
    }

    @Override
    public short getShort(int ordinal) {
        return (Short) fields[ordinal];
    }

    @Override
    public int getInt(int ordinal) {
        return (Integer) fields[ordinal];
    }

    @Override
    public long getLong(int ordinal) {
        return (Long) fields[ordinal];
    }

    @Override
    public float getFloat(int ordinal) {
        return (Float) fields[ordinal];
    }

    @Override
    public double getDouble(int ordinal) {
        return (Double) fields[ordinal];
    }

    @Override
    public Decimal getDecimal(int ordinal, int precision, int scale) {
        return (Decimal) fields[ordinal];
    }

    @Override
    public UTF8String getUTF8String(int ordinal) {
        return (UTF8String) fields[ordinal];
    }

    @Override
    public byte[] getBinary(int ordinal) {
        return (byte[]) fields[ordinal];
    }

    @Override
    public CalendarInterval getInterval(int ordinal) {
        return (CalendarInterval) fields[ordinal];
    }

    @Override
    public InternalRow getStruct(int ordinal, int numFields) {
        return (InternalRow) fields[ordinal];
    }

    @Override
    public ArrayData getArray(int ordinal) {
        return (ArrayData) fields[ordinal];
    }

    @Override
    public MapData getMap(int ordinal) {
        return (MapData) fields[ordinal];
    }

    @Override
    public Object get(int ordinal, DataType dataType) {
        return fields[ordinal];
    }
}
