package com.pingcap.ch.datatypes;

import com.pingcap.ch.columns.CHColumn;
import com.pingcap.ch.columns.CHColumnFixedString;
import com.pingcap.common.MemoryUtil;
import com.pingcap.common.ReadBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.pingcap.common.MemoryUtil.allocateDirect;

public class CHTypeFixedString implements CHType {
    private int length;

    public CHTypeFixedString(int length) {
        assert length > 0;
        this.length = length;
    }

    @Override
    public String name() {
        return "FixedString(" + length + ")";
    }

    @Override
    public CHColumn deserialize(ReadBuffer reader, int size) throws IOException {
        if (size == 0) {
            return new CHColumnFixedString(0, length, MemoryUtil.EMPTY_BYTE_BUFFER_DIRECT);
        }
        ByteBuffer chars = allocateDirect(size * length);
        reader.read(chars);
        chars.clear();
        return new CHColumnFixedString(size, length, chars);
    }
}
