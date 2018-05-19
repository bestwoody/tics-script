package com.pingcap.ch.columns;

import com.pingcap.ch.datatypes.CHType;
import com.pingcap.ch.datatypes.CHTypeString;
import com.pingcap.common.MemoryUtil;

import org.apache.spark.unsafe.types.UTF8String;

import java.nio.ByteBuffer;

public class CHColumnString implements CHColumn {
    private int size;

    /// Maps i'th position to offset to i+1'th element. Last offset maps to the end of all chars (is the size of all chars).
    private ByteBuffer offsets; // UInt64
    /// Bytes of strings, placed contiguously.
    /// For convenience, every string ends with terminating zero byte. Note that strings could contain zero bytes in the middle.
    private ByteBuffer chars;

    private long offsetsAddr;
    private long charsAddr;

    public CHColumnString(int size, ByteBuffer offsets, ByteBuffer chars) {
        this.size = size;

        this.offsets = offsets;
        this.chars = chars;

        this.offsetsAddr = MemoryUtil.getAddress(offsets);
        this.charsAddr = MemoryUtil.getAddress(chars);
    }

    @Override
    public CHType dataType() {
        return CHTypeString.instance;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public long byteCount() {
        return (size << 3) + offsetAt(size);
    }

    @Override
    public void free() {
        if (offsetsAddr == 0) {
            return;
        }
        MemoryUtil.free(offsets);
        MemoryUtil.free(chars);
        offsetsAddr = 0;
        charsAddr = 0;
    }

    private long offsetAt(int i) {
        return i == 0 ? 0 : MemoryUtil.getLong(offsetsAddr + ((i - 1) << 3));
    }

    private int sizeAt(int i) {
        return (int) (i == 0
                ? MemoryUtil.getLong(offsetsAddr)
                : MemoryUtil.getLong(offsetsAddr + (i << 3)) - MemoryUtil.getLong(offsetsAddr + ((i - 1) << 3)));
    }

    @Override
    public UTF8String getUTF8String(int rowId) {
        //int size = sizeAt(rowId);
        //byte[] bytes = new byte[size];
        //MemoryUtil.getBytes(offsetAt(rowId), bytes, 0, size);
        //return UTF8String.fromBytes(bytes);

        // Check carefully about the life cycle of this object and the returned string.
        return UTF8String.fromAddress(null, charsAddr + offsetAt(rowId), sizeAt(rowId) - 1);
    }
}
