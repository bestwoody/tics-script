package com.pingcap.ch.columns;

import com.pingcap.ch.datatypes.CHType;

import org.apache.spark.unsafe.types.UTF8String;

public interface CHColumn {

    CHType dataType();

    default String typeName() {return dataType().name();}

    int size();

    long byteCount();

    void free();

    default boolean empty() {return size() == 0;}

    default boolean isNullAt(int rowId) {return false;}

    default boolean getBoolean(int rowId) {throw new UnsupportedOperationException();}

    default byte getByte(int rowId) {throw new UnsupportedOperationException();}

    default short getShort(int rowId) {throw new UnsupportedOperationException();}

    default int getInt(int rowId) {throw new UnsupportedOperationException();}

    default long getLong(int rowId) {throw new UnsupportedOperationException();}

    default float getFloat(int rowId) {throw new UnsupportedOperationException();}

    default double getDouble(int rowId) {throw new UnsupportedOperationException();}

    default UTF8String getUTF8String(int rowId) {throw new UnsupportedOperationException();}

}
