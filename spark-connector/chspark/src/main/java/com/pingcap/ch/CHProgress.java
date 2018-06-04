package com.pingcap.ch;

import com.pingcap.common.ReadBuffer;
import com.pingcap.common.WriteBuffer;

import java.io.IOException;

public class CHProgress {
    public long rows = 0;
    public long bytes = 0;
    public long totalRows = 0;

    public static CHProgress read(ReadBuffer reader) throws IOException {
        CHProgress p = new CHProgress();
        p.rows = reader.readVarUInt64();
        p.bytes = reader.readVarUInt64();
        p.totalRows = reader.readVarUInt64();
        return p;
    }

    public void write(WriteBuffer writer) throws IOException {
        writer.writeVarUInt64(rows);
        writer.writeVarUInt64(bytes);
        writer.writeVarUInt64(totalRows);
    }
}
