package com.pingcap.ch.columns;

import com.pingcap.ch.datatypes.CHType;
import com.pingcap.ch.datatypes.CHTypeFixedString;
import com.pingcap.common.MemoryUtil;

import org.apache.spark.unsafe.types.UTF8String;

import java.nio.ByteBuffer;

public class CHColumnFixedString implements CHColumn {
    private CHTypeFixedString dataType;
    private int size;
    private int valLen;

    private ByteBuffer data;
    private long dataAddr;

    public CHColumnFixedString(int size, int valLen, ByteBuffer data) {
        this.dataType = new CHTypeFixedString(valLen);
        this.size = size;
        this.valLen = valLen;
        this.data = data;
        this.dataAddr = MemoryUtil.getAddress(data);
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
        return size * valLen;
    }

    @Override
    public UTF8String getUTF8String(int rowId) {
        //byte[] bytes = new byte[length];
        //MemoryUtil.getBytes(length * rowId, bytes, 0, length);
        //return UTF8String.fromBytes(bytes);

        return UTF8String.fromAddress(null, dataAddr + valLen * rowId, valLen);
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
