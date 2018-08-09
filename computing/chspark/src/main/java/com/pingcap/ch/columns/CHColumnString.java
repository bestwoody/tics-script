package com.pingcap.ch.columns;

import com.pingcap.ch.columns.UTF8ChunkBuilder.SealedChunk;
import com.pingcap.ch.datatypes.CHTypeString;
import com.pingcap.common.MemoryUtil;
import java.nio.ByteBuffer;
import org.apache.spark.unsafe.types.UTF8String;

public class CHColumnString extends CHColumn {

    /// Maps i'th position to offset to i+1'th element. Last offset maps to the end of all chars (is the size of all chars).
    private ByteBuffer offsets; // UInt64
    /// Bytes of strings, placed contiguously.
    /// For convenience, every string ends with terminating zero byte. Note that strings could contain zero bytes in the middle.
    private ByteBuffer chars;

    private SealedChunk chunk;

    private long offsetsAddr;
    private long charsAddr;
    private PagedChunkBuilder builder;
    public static final int MIN_PAGE_SIZE = 1000;

    public CHColumnString(int size, ByteBuffer offsets, ByteBuffer chars) {
        super(CHTypeString.instance, size);

        this.offsets = offsets;
        this.chars = chars;

        this.offsetsAddr = MemoryUtil.getAddress(offsets);
        this.charsAddr = MemoryUtil.getAddress(chars);
    }

    public CHColumnString(int maxSize) {
        super(CHTypeString.instance, 0);
        int pageCapacity;
        if (maxSize <= MIN_PAGE_SIZE) {
            pageCapacity = maxSize;
        } else {
            pageCapacity = (int) Math.sqrt(maxSize);
        }
        builder = new PagedChunkBuilder(pageCapacity);
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
        if (chunk != null) {
            chunk.free();
        }
        offsetsAddr = 0;
        charsAddr = 0;
    }

    public long offsetAt(int i) {
        return i == 0 ? 0 : MemoryUtil.getLong(offsetsAddr + ((i - 1) << 3));
    }

    public int sizeAt(int i) {
        return (int) (i == 0
            ? MemoryUtil.getLong(offsetsAddr)
            : MemoryUtil.getLong(offsetsAddr + (i << 3)) - MemoryUtil
                .getLong(offsetsAddr + ((i - 1) << 3)));
    }

    @Override
    public UTF8String getUTF8String(int rowId) {
        // Check carefully about the life cycle of this object and the returned string.
        return UTF8String.fromAddress(null, charsAddr + offsetAt(rowId), sizeAt(rowId) - 1);
    }

    @Override
    public void insertDefault() {
        insertUTF8String(UTF8String.EMPTY_UTF8);
    }

    @Override
    public void insertUTF8String(UTF8String v) {
        // The passed in string could be a pointer.
        builder.insertUTF8String(v);
    }

    @Override
    public CHColumn seal() {
        this.chunk = builder.seal();
        builder = null;

        offsets = chunk.getOffsetBuf();
        chars = chunk.getCharBuf();
        offsets.clear();
        chars.clear();
        offsetsAddr = MemoryUtil.getAddress(offsets);
        charsAddr = MemoryUtil.getAddress(chars);

        size = chunk.size();
        return this;
    }
}
