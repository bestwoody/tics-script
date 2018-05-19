package com.pingcap.ch.columns;

import com.pingcap.ch.datatypes.CHType;
import com.pingcap.ch.datatypes.CHTypeNumber;
import com.pingcap.common.MemoryUtil;

import java.nio.ByteBuffer;

import static com.pingcap.ch.datatypes.CHTypeNumber.CHTypeFloat32;
import static com.pingcap.ch.datatypes.CHTypeNumber.CHTypeFloat64;
import static com.pingcap.ch.datatypes.CHTypeNumber.CHTypeInt16;
import static com.pingcap.ch.datatypes.CHTypeNumber.CHTypeInt32;
import static com.pingcap.ch.datatypes.CHTypeNumber.CHTypeInt64;
import static com.pingcap.ch.datatypes.CHTypeNumber.CHTypeInt8;
import static com.pingcap.ch.datatypes.CHTypeNumber.CHTypeUInt16;
import static com.pingcap.ch.datatypes.CHTypeNumber.CHTypeUInt32;
import static com.pingcap.ch.datatypes.CHTypeNumber.CHTypeUInt64;
import static com.pingcap.ch.datatypes.CHTypeNumber.CHTypeUInt8;

public class CHColumnNumber implements CHColumn {
    private CHTypeNumber dataType;
    private int size;

    private ByteBuffer data; // We need to keep a reference here to prevent the memory from gc.
    private long dataAddr;

    public CHColumnNumber(CHTypeNumber dataType, int size, ByteBuffer data) {
        this.dataType = dataType;
        this.size = size;
        this.data = data;
        dataAddr = MemoryUtil.getAddress(data);
    }

    @Override
    public CHType dataType() {
        return dataType;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public long byteCount() {
        return size << dataType.shift();
    }

    @Override
    public void free() {
        if (dataAddr == 0) {
            return;
        }
        MemoryUtil.free(data);
        dataAddr = 0;
    }

    @Override
    public byte getByte(int rowId) {
        assert dataType instanceof CHTypeInt8 || dataType instanceof CHTypeUInt8;
        return MemoryUtil.getByte(dataAddr + rowId);
    }

    @Override
    public short getShort(int rowId) {
        assert dataType instanceof CHTypeInt16 || dataType instanceof CHTypeUInt16;
        return MemoryUtil.getShort(dataAddr + (rowId << 1));
    }

    @Override
    public int getInt(int rowId) {
        assert dataType instanceof CHTypeInt32 || dataType instanceof CHTypeUInt32;
        return MemoryUtil.getInt(dataAddr + (rowId << 2));
    }

    @Override
    public long getLong(int rowId) {
        assert dataType instanceof CHTypeInt64 || dataType instanceof CHTypeUInt64;
        return MemoryUtil.getLong(dataAddr + (rowId << 3));
    }

    @Override
    public float getFloat(int rowId) {
        assert dataType instanceof CHTypeFloat32;
        return MemoryUtil.getFloat(dataAddr + (rowId << 2));
    }

    @Override
    public double getDouble(int rowId) {
        assert dataType instanceof CHTypeFloat64;
        return MemoryUtil.getDouble(dataAddr + (rowId << 3));
    }
}
