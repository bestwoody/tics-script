package com.pingcap.ch.datatypes;

import static com.pingcap.common.MemoryUtil.allocateDirect;

import com.pingcap.ch.columns.CHColumn;
import com.pingcap.ch.columns.CHColumnDate;
import com.pingcap.common.MemoryUtil;
import com.pingcap.common.ReadBuffer;
import com.pingcap.common.WriteBuffer;
import java.io.IOException;
import java.nio.ByteBuffer;

public class CHTypeDate implements CHType {
  public static final CHTypeDate instance = new CHTypeDate();
  public static final CHTypeNullable nullableInstance = new CHTypeNullable(instance);

  private CHTypeDate() {}

  @Override
  public String name() {
    return "Date";
  }

  @Override
  public CHColumn allocate(int maxSize) {
    return new CHColumnDate(maxSize);
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

  @Override
  public void serialize(WriteBuffer writer, CHColumn column) throws IOException {
    ByteBuffer data = MemoryUtil.duplicateDirectByteBuffer(((CHColumnDate) column).data());
    data.clear().limit(column.size() << 1);
    writer.write(data);
  }
}
