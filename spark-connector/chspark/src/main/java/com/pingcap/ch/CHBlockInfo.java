package com.pingcap.ch;

import com.google.common.base.Preconditions;

import com.pingcap.common.ReadBuffer;
import com.pingcap.common.WriteBuffer;

import java.io.IOException;

public class CHBlockInfo {
    public boolean is_overflows = false;
    public int bucket_num = -1;

    // Not part of this info.
    public boolean isEOF = false;

    private static final int cap = 1 + 1 + 1 + 4 + 1;

    public CHBlockInfo(boolean is_overflows, int bucket_num) {
        this.is_overflows = is_overflows;
        this.bucket_num = bucket_num;
    }

    public CHBlockInfo() {}

    public void write(WriteBuffer writer) throws IOException {
        writer.writeVarUInt64(1);
        writer.writeByte((byte) 0);
        writer.writeVarUInt64(2);
        writer.writeInt(bucket_num);
        writer.writeVarUInt64(0);
    }

    public static CHBlockInfo read(ReadBuffer reader) throws IOException {
        try {
            // Check remaining data.
            reader.ensure(1);
        } catch (IOException e) {
            if ("End of stream.".equals(e.getMessage())) {
                // Legal case. Noting to read means empty block.
                CHBlockInfo info = new CHBlockInfo();
                info.isEOF = true;
                return info;
            } else {
                throw e;
            }
        }

        Preconditions.checkState(reader.readVarUInt64() == 1);
        boolean is_overflows = reader.readByte() != 0;
        Preconditions.checkState(reader.readVarUInt64() == 2);
        int bucket_num = reader.readInt();
        long mark = reader.readVarUInt64();
        Preconditions.checkState(mark == 0);

        return new CHBlockInfo(is_overflows, bucket_num);
    }
}