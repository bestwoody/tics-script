package com.pingcap.ch.datatypes;

import static com.pingcap.common.MemoryUtil.allocateDirect;

import com.pingcap.ch.columns.CHColumn;
import com.pingcap.ch.columns.CHColumnMyDateTime;
import com.pingcap.common.MemoryUtil;
import com.pingcap.common.ReadBuffer;
import com.pingcap.common.WriteBuffer;
import java.io.IOException;
import java.nio.ByteBuffer;

public class CHTypeMyDateTime implements CHType {

  public static final CHTypeMyDateTime instance = new CHTypeMyDateTime();
  public static final CHTypeNullable nullableInstance = new CHTypeNullable(instance);

  private CHTypeMyDateTime() {}

  @Override
  public String name() {
    return "MyDateTime";
  }

  @Override
  public CHColumn allocate(int maxSize) {
    return new CHColumnMyDateTime(maxSize);
  }

  @Override
  public CHColumn deserialize(ReadBuffer reader, int size) throws IOException {
    if (size == 0) {
      return new CHColumnMyDateTime(0, MemoryUtil.EMPTY_BYTE_BUFFER_DIRECT);
    }
    ByteBuffer buffer = allocateDirect(size << 3);
    reader.read(buffer);
    buffer.clear();
    return new CHColumnMyDateTime(size, buffer);
  }

  @Override
  public void serialize(WriteBuffer writer, CHColumn column) throws IOException {
    ByteBuffer data = MemoryUtil.duplicateDirectByteBuffer(((CHColumnMyDateTime) column).data());
    data.clear().limit(column.size() << 3);
    writer.write(data);
  }
}
