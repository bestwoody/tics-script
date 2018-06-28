package com.pingcap.ch.columns;

import com.pingcap.ch.datatypes.CHTypeDecimal;
import com.pingcap.common.MemoryUtil;

import org.apache.spark.sql.types.Decimal;

import java.math.BigInteger;
import java.nio.ByteBuffer;

public class CHColumnDecimal extends CHColumn {
    private int precision; // unused now
    private int scale; // unused now

    private ByteBuffer data;
    private long dataAddr;

    static int decimalSize = 64;

    public CHColumnDecimal(int size, int prec, int scale, ByteBuffer data) {
        super(new CHTypeDecimal(prec, scale), size);
        this.precision = prec;
        this.scale = scale;
        this.data = data;
        this.dataAddr = MemoryUtil.getAddress(data);
    }

    public CHColumnDecimal(int prec, int scale, int maxSize) {
        this(0, prec, scale, ByteBuffer.allocateDirect(decimalSize * maxSize));
    }

    public ByteBuffer data() {
        return data;
    }

    @Override
    public long byteCount() {
        return size * decimalSize;
    }

    @Override
    public Decimal getDecimal(int rowId) {
        return MemoryUtil.getDecimal(rowId * decimalSize + dataAddr);
    }

    @Override
    public void insertDecimal(Decimal v) {
        int prec = v.precision();
        int scale = v.scale();
        BigInteger bigInt = v.toJavaBigInteger();
        int sign = 0;
        if (bigInt.signum() < 0) {
            sign = 1;
        }
        byte[] arr = bigInt.toByteArray();
        int limbs = arr.length / 8;
        if (arr.length % 8 > 0) {
            limbs++;
        }

        int index = size * decimalSize;

        for (int i = 0; i < arr.length; i++) {
            data.put(index, arr[arr.length - 1 -i]);
        }
        data.putShort(index + 32, (short) limbs);
        data.putShort(index + 34, (short) sign);
        data.putShort(index + 48, (short) prec);
        data.putChar(index + 50, (char) scale);

        size++;
    }

    @Override
    public CHColumn seal() {
        data.clear();
        data.limit(size * decimalSize);
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
