package com.pingcap.ch.datatypes;

import com.pingcap.ch.columns.CHColumn;
import com.pingcap.ch.columns.CHColumnDateTime;
import com.pingcap.common.MemoryUtil;
import com.pingcap.common.ReadBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.pingcap.common.MemoryUtil.allocateDirect;

public class CHTypeDateTime implements CHType {
    public static final CHTypeDateTime instance = new CHTypeDateTime();

    private CHTypeDateTime() {}

    @Override
    public String name() {
        return "DateTime";
    }

    @Override
    public CHColumn deserialize(ReadBuffer reader, int size) throws IOException {
        if (size == 0) {
            return new CHColumnDateTime(0, MemoryUtil.EMPTY_BYTE_BUFFER_DIRECT);
        }
        ByteBuffer buffer = allocateDirect(size << 2);
        reader.read(buffer);
        buffer.clear();
        return new CHColumnDateTime(size, buffer);
    }
}
