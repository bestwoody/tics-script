package com.pingcap.ch.columns;

import com.pingcap.ch.datatypes.CHType;
import com.pingcap.ch.datatypes.CHTypeDate;
import com.pingcap.common.MemoryUtil;

import java.nio.ByteBuffer;

public class CHColumnDate implements CHColumn {
    private int size;
    private ByteBuffer data; // We need to keep a reference here to prevent the memory from gc.
    private long dataAddr;

    public CHColumnDate(int size, ByteBuffer data) {
        this.size = size;
        this.data = data;
        dataAddr = MemoryUtil.getAddress(data);
    }

    @Override
    public CHType dataType() {
        return CHTypeDate.instance;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public long byteCount() {
        return size << 1;
    }

    @Override
    public short getShort(int rowId) {
        return MemoryUtil.getShort(dataAddr + (rowId << 1));
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
