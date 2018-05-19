package com.pingcap.ch.datatypes;

import com.pingcap.ch.columns.CHColumn;
import com.pingcap.ch.columns.CHColumnDate;
import com.pingcap.common.MemoryUtil;
import com.pingcap.common.ReadBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.pingcap.common.MemoryUtil.allocateDirect;

public class CHTypeDate implements CHType {
    public static final CHTypeDate instance = new CHTypeDate();

    private CHTypeDate() {}

    @Override
    public String name() {
        return "Date";
    }

    @Override
    public CHColumn deserialize(ReadBuffer reader, int size) throws IOException {
        if (size == 0) {
            return new CHColumnDate(0, MemoryUtil.EMPTY_BYTE_BUFFER_DIRECT);
        }
        ByteBuffer buffer = allocateDirect(size << 1);
        reader.read(buffer);
        buffer.clear();
        return new CHColumnDate(size, buffer);
    }
}
