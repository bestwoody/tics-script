package com.pingcap.ch.datatypes;

import com.google.common.base.Preconditions;

import com.pingcap.ch.columns.CHColumn;
import com.pingcap.ch.columns.CHColumnString;
import com.pingcap.common.AutoGrowByteBuffer;
import com.pingcap.common.MemoryUtil;
import com.pingcap.common.ReadBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.pingcap.common.MemoryUtil.allocateDirect;

public class CHTypeString implements CHType {
    public static final CHTypeString instance = new CHTypeString();
    // Use to prevent frequently reallocate the chars buffer.
    // ClickHouse does not pass a total length at the beginning, so sad...
    private static final ThreadLocal<ByteBuffer> initBuffer = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return MemoryUtil.allocateDirect(102400);
        }
    };

    private CHTypeString() {}

    @Override
    public String name() {
        return "String";
    }

    @Override
    public CHColumn deserialize(ReadBuffer reader, int size) throws IOException {
        if (size == 0) {
            return new CHColumnString(0, MemoryUtil.EMPTY_BYTE_BUFFER_DIRECT, MemoryUtil.EMPTY_BYTE_BUFFER_DIRECT);
        }

        ByteBuffer offsets = allocateDirect(size << 3);
        ByteBuffer initCharsBuf = initBuffer.get();
        AutoGrowByteBuffer autoGrowCharsBuf = new AutoGrowByteBuffer(initCharsBuf);

        double avgValueSize = 1;
        int offset = 0;
        for (int i = 0; i < size; i++) {
            int valueSize = (int) reader.readVarUInt64();

            offset += valueSize + 1;
            offsets.putLong(offset);

            autoGrowCharsBuf.put(reader, valueSize);
            autoGrowCharsBuf.putByte((byte) 0); // terminating zero byte
        }

        Preconditions.checkState(offset == autoGrowCharsBuf.dataSize());

        ByteBuffer chars = autoGrowCharsBuf.getByteBuffer();
        if (chars == initCharsBuf) {
            // Copy out.
            ByteBuffer newChars = MemoryUtil.allocateDirect(offset);
            MemoryUtil.copyMemory(MemoryUtil.getAddress(chars), MemoryUtil.getAddress(newChars), offset);
            chars = newChars;
        }

        return new CHColumnString(size, offsets, chars);
    }
}
