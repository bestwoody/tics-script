package com.pingcap.ch;

import com.pingcap.common.ReadBuffer;
import com.pingcap.common.WriteBuffer;
import java.io.IOException;

public class CHBlockStreamProfileInfo {
  public long rows = 0;
  public long blocks = 0;
  public long bytes = 0;
  public boolean appliedLimit = false;
  public long rowsBeforeLimit = 0;
  public boolean calculatedRowsBeforeLimit = false;

  public static CHBlockStreamProfileInfo read(ReadBuffer reader) throws IOException {
    CHBlockStreamProfileInfo info = new CHBlockStreamProfileInfo();
    info.rows = reader.readVarUInt64();
    info.blocks = reader.readVarUInt64();
    info.bytes = reader.readVarUInt64();
    info.appliedLimit = reader.readByte() != 0;
    info.rowsBeforeLimit = reader.readVarUInt64();
    info.calculatedRowsBeforeLimit = reader.readByte() != 0;
    return info;
  }

  public void write(WriteBuffer writer) throws IOException {
    writer.writeVarUInt64(rows);
    writer.writeVarUInt64(blocks);
    writer.writeVarUInt64(bytes);
    writer.writeByte(appliedLimit ? (byte) 1 : 0);
    writer.writeVarUInt64(rowsBeforeLimit);
    writer.writeByte(calculatedRowsBeforeLimit ? (byte) 1 : 0);
  }
}
