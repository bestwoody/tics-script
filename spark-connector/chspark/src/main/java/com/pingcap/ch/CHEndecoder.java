package com.pingcap.ch;

import com.pingcap.ch.columns.CHColumn;
import com.pingcap.ch.columns.CHColumnWithTypeAndName;
import com.pingcap.ch.datatypes.CHType;
import com.pingcap.ch.datatypes.CHTypeFactory;
import com.pingcap.common.MemoryUtil;
import com.pingcap.common.ReadBuffer;
import com.pingcap.common.WriteBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class CHEndecoder {
    public static CHBlock decode(ReadBuffer reader) throws IOException {
        CHBlockInfo info = CHBlockInfo.read(reader);
        if (info.isEOF) {
            return null;
        }

        ByteBuffer strBuf = MemoryUtil.allocateDirect(1024);

        int colCount = (int) reader.readVarUInt64();
        int rowCount = (int) reader.readVarUInt64();

        ArrayList<CHColumnWithTypeAndName> columns = new ArrayList<>(colCount);
        for (int columnId = 0; columnId < colCount; columnId++) {
            String name = reader.readUTF8StrWithVarLen(strBuf);
            String typeName = reader.readUTF8StrWithVarLen(strBuf);
            CHType type = CHTypeFactory.parseType(typeName);
            CHColumn column = type.deserialize(reader, rowCount);
            columns.add(new CHColumnWithTypeAndName(column, type, name));
        }

        MemoryUtil.free(strBuf);

        return new CHBlock(info, columns);
    }

    public static void encodeEmptyBlock(WriteBuffer writer) throws IOException {
        // Empty info.
        CHBlockInfo info = new CHBlockInfo();
        info.write(writer);

        // 0 clumns and 0 rows.
        writer.writeVarUInt64(0);
        writer.writeVarUInt64(0);
    }
}
