package com.pingcap.ch.columns;

import com.pingcap.ch.datatypes.CHTypeDateTime;
import com.pingcap.common.MemoryUtil;

import java.nio.ByteBuffer;

public class CHColumnDateTime extends CHColumn {
    private ByteBuffer data; // Keep a reference here to prevent the memory from gc.
    private long dataAddr;

    public CHColumnDateTime(int size, ByteBuffer data) {
        super(CHTypeDateTime.instance, size);
        this.data = data;
        dataAddr = MemoryUtil.getAddress(data);
    }

    public CHColumnDateTime(int maxSize) {
        this(0, MemoryUtil.allocateDirect(maxSize << 3));
    }

    public ByteBuffer data() {
        return data;
    }

    @Override
    public long byteCount() {
        return size << 3;
    }

    @Override
    public long getLong(int rowId) {
        return MemoryUtil.getLong(dataAddr + (rowId << 3));
    }

    @Override
    public void insertLong(long v) {
        MemoryUtil.setLong(dataAddr + (size << 3), v);
        size++;
    }

    @Override
    public CHColumn seal() {
        data.clear();
        data.limit(size << 3);
        return this;
    }

    @Override
    public void free() {
        if (dataAddr == 0) {
            return;
        }
        MemoryUtil.free(data);
        dataAddr = 0;
    }
}
