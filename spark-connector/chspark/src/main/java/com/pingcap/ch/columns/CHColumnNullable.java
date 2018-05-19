package com.pingcap.ch.columns;

import com.pingcap.ch.datatypes.CHType;
import com.pingcap.ch.datatypes.CHTypeNullable;

import org.apache.spark.unsafe.types.UTF8String;

public class CHColumnNullable implements CHColumn {
    public final CHTypeNullable type;
    public final int size;
    public final CHColumnNumber null_map;
    public final CHColumn nested_column;

    public CHColumnNullable(CHTypeNullable type, int size, CHColumnNumber null_map, CHColumn nested_column) {
        this.type = type;
        this.size = size;
        this.null_map = null_map;
        this.nested_column = nested_column;
    }

    @Override
    public CHType dataType() {
        return type;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public long byteCount() {
        return null_map.byteCount() + nested_column.byteCount();
    }

    @Override
    public void free() {
        null_map.free();
        nested_column.free();
    }

    @Override
    public boolean isNullAt(int rowId) {
        return null_map.getByte(rowId) != 0;
    }

    @Override
    public boolean getBoolean(int rowId) {
        return nested_column.getBoolean(rowId);
    }

    @Override
    public byte getByte(int rowId) {
        return nested_column.getByte(rowId);
    }

    @Override
    public short getShort(int rowId) {
        return nested_column.getShort(rowId);
    }

    @Override
    public int getInt(int rowId) {
        return nested_column.getInt(rowId);
    }

    @Override
    public long getLong(int rowId) {
        return nested_column.getLong(rowId);
    }

    @Override
    public float getFloat(int rowId) {
        return nested_column.getFloat(rowId);
    }

    @Override
    public double getDouble(int rowId) {
        return nested_column.getDouble(rowId);
    }

    @Override
    public UTF8String getUTF8String(int rowId) {
        return nested_column.getUTF8String(rowId);
    }
}
