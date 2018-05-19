package com.pingcap.ch.columns;

import com.pingcap.ch.datatypes.CHType;
import com.pingcap.ch.datatypes.CHTypeDateTime;
import com.pingcap.common.MemoryUtil;

import java.nio.ByteBuffer;

public class CHColumnDateTime implements CHColumn {
    private int size;
    private ByteBuffer data; // Keep a reference here to prevent the memory from gc.
    private long dataAddr;

    public CHColumnDateTime(int size, ByteBuffer data) {
        this.size = size;
        this.data = data;
        dataAddr = MemoryUtil.getAddress(data);
    }

    @Override
    public CHType dataType() {
        return CHTypeDateTime.instance;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public long byteCount() {
        return size << 2;
    }

    @Override
    public int getInt(int rowId) {
        return MemoryUtil.getInt(dataAddr + (rowId << 2));
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
