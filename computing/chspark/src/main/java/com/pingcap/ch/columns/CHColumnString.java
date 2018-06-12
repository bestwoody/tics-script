package com.pingcap.ch.columns;

import com.google.common.base.Preconditions;

import com.pingcap.ch.datatypes.CHTypeString;
import com.pingcap.common.MemoryUtil;

import org.apache.spark.unsafe.types.UTF8String;

import java.nio.ByteBuffer;

public class CHColumnString extends CHColumn {
    // For insert.
    private UTF8String[] strs;

    /// Maps i'th position to offset to i+1'th element. Last offset maps to the end of all chars (is the size of all chars).
    private ByteBuffer offsets; // UInt64
    /// Bytes of strings, placed contiguously.
    /// For convenience, every string ends with terminating zero byte. Note that strings could contain zero bytes in the middle.
    private ByteBuffer chars;

    private long offsetsAddr;
    private long charsAddr;

    public CHColumnString(int size, ByteBuffer offsets, ByteBuffer chars) {
        super(CHTypeString.instance, size);

        this.offsets = offsets;
        this.chars = chars;

        this.offsetsAddr = MemoryUtil.getAddress(offsets);
        this.charsAddr = MemoryUtil.getAddress(chars);
    }

    public CHColumnString(int maxSize) {
        super(CHTypeString.instance, 0);
        strs = new UTF8String[maxSize];
    }

    public ByteBuffer offsets() {
        return offsets;
    }

    public ByteBuffer chars() {
        return chars;
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

    public long offsetAt(int i) {
        return i == 0 ? 0 : MemoryUtil.getLong(offsetsAddr + ((i - 1) << 3));
    }

    public int sizeAt(int i) {
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

    @Override
    public void insertDefault() {
        strs[size] = UTF8String.EMPTY_UTF8;
        size++;
    }

    @Override
    public void insertUTF8String(UTF8String v) {
        // The passed in string could be a pointer.
        strs[size] = v.clone();
        size++;
    }

    @Override
    public CHColumn seal() {
        long totalLen = 0;
        for (int i = 0; i < size; i++) {
            totalLen += strs[i].numBytes() + 1;
        }
        if (totalLen >= Integer.MAX_VALUE) {
            throw new IllegalStateException("String total length overflow!");
        }
        ByteBuffer offsets = MemoryUtil.allocateDirect(size << 3);
        ByteBuffer chars = MemoryUtil.allocateDirect((int) totalLen);
        long charsAddr = MemoryUtil.getAddress(chars);

        long curOffset = 0;
        for (int i = 0; i < size; i++) {
            UTF8String s = strs[i];
            offsets.putLong(curOffset + s.numBytes() + 1);
            MemoryUtil.copyMemory(s.getBaseObject(), s.getBaseOffset(), null, charsAddr + curOffset, s.numBytes());
            curOffset += s.numBytes() + 1;
        }

        Preconditions.checkState(curOffset == chars.capacity());

        offsets.clear();
        chars.clear();

        this.strs = null;
        this.offsets = offsets;
        this.chars = chars;
        this.offsetsAddr = MemoryUtil.getAddress(offsets);
        this.charsAddr = charsAddr;

        return this;
    }
}
