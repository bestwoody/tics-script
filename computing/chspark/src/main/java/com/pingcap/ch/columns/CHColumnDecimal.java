package com.pingcap.ch.columns;

import com.pingcap.ch.datatypes.CHTypeDecimal;
import com.pingcap.common.MemoryUtil;

import org.apache.spark.sql.types.Decimal;

import java.math.BigInteger;
import java.nio.ByteBuffer;

public class CHColumnDecimal extends CHColumn {
    private int precision;
    private int scale;

    private ByteBuffer data;
    private long dataAddr;

    static private final int DECIMAL_SIZE = 64;

    public CHColumnDecimal(int size, int prec, int scale, ByteBuffer data) {
        super(new CHTypeDecimal(prec, scale), size);
        this.precision = prec;
        this.scale = scale;
        this.data = data;
        this.dataAddr = MemoryUtil.getAddress(data);
    }

    public CHColumnDecimal(int prec, int scale, int maxSize) {
        this(0, prec, scale, ByteBuffer.allocateDirect(DECIMAL_SIZE * maxSize));
    }

    public ByteBuffer data() {
        return data;
    }

    @Override
    public long byteCount() {
        return size * DECIMAL_SIZE;
    }

    @Override
    public Decimal getDecimal(int rowId) {
        return MemoryUtil.getDecimal(rowId * DECIMAL_SIZE + dataAddr);
    }

    @Override
    public void insertDecimal(Decimal v) {
        MemoryUtil.setDecimal(dataAddr + size * DECIMAL_SIZE, v, precision, scale);
        size++;
    }

    @Override
    public CHColumn seal() {
        data.clear();
        data.limit(size * DECIMAL_SIZE);
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
