package com.pingcap.ch.datatypes;

import static com.pingcap.common.MemoryUtil.allocateDirect;

import com.pingcap.ch.columns.CHColumn;
import com.pingcap.ch.columns.CHColumnMyDate;
import com.pingcap.common.MemoryUtil;
import com.pingcap.common.ReadBuffer;
import com.pingcap.common.WriteBuffer;
import java.io.IOException;
import java.nio.ByteBuffer;

public class CHTypeMyDate implements CHType {
  public static final CHTypeMyDate instance = new CHTypeMyDate();
  public static final CHTypeNullable nullableInstance = new CHTypeNullable(instance);

  private CHTypeMyDate() {}

  @Override
  public String name() {
    return "MyDate";
  }

  @Override
  public CHColumn allocate(int maxSize) {
    return new CHColumnMyDate(maxSize);
  }

  @Override
  public CHColumn deserialize(ReadBuffer reader, int size) throws IOException {
    if (size == 0) {
      return new CHColumnMyDate(0, MemoryUtil.EMPTY_BYTE_BUFFER_DIRECT);
    }
    ByteBuffer buffer = allocateDirect(size << 3);
    reader.read(buffer);
    buffer.clear();
    return new CHColumnMyDate(size, buffer);
  }

  @Override
  public void serialize(WriteBuffer writer, CHColumn column) throws IOException {
    ByteBuffer data = MemoryUtil.duplicateDirectByteBuffer(((CHColumnMyDate) column).data());
    data.clear().limit(column.size() << 3);
    writer.write(data);
  }
}
