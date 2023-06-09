package com.pingcap.ch.datatypes;

import static com.pingcap.common.MemoryUtil.allocateDirect;

import com.pingcap.ch.columns.CHColumn;
import com.pingcap.ch.columns.CHColumnString;
import com.pingcap.common.AutoGrowByteBuffer;
import com.pingcap.common.MemoryUtil;
import com.pingcap.common.ReadBuffer;
import com.pingcap.common.WriteBuffer;
import java.io.IOException;
import java.nio.ByteBuffer;
import shade.com.google.common.base.Preconditions;

public class CHTypeString implements CHType {
  public static final CHTypeString instance = new CHTypeString();
  public static final CHTypeNullable nullableInstance = new CHTypeNullable(instance);
  // Use to prevent frequently reallocate the chars buffer.
  // ClickHouse does not pass a total length at the beginning, so sad...
  private static final ThreadLocal<ByteBuffer> initBuffer =
      new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
          return MemoryUtil.allocateDirect(102400);
        }
      };

  private CHTypeString() {}

  @Override
  public String name() {
    return "String";
  }

  @Override
  public CHColumn allocate(int maxSize) {
    return new CHColumnString(maxSize);
  }

  @Override
  public CHColumn deserialize(ReadBuffer reader, int size) throws IOException {
    if (size == 0) {
      return new CHColumnString(
          0, MemoryUtil.EMPTY_BYTE_BUFFER_DIRECT, MemoryUtil.EMPTY_BYTE_BUFFER_DIRECT);
    }

    ByteBuffer offsets = allocateDirect(size << 3);
    ByteBuffer initCharsBuf = initBuffer.get();
    AutoGrowByteBuffer autoGrowCharsBuf = new AutoGrowByteBuffer(initCharsBuf);

    double avgValueSize = 1;
    int offset = 0;
    for (int i = 0; i < size; i++) {
      int valueSize = (int) reader.readVarUInt64();

      offset += valueSize + 1;
      offsets.putLong(offset);

      autoGrowCharsBuf.put(reader, valueSize);
      autoGrowCharsBuf.putByte((byte) 0); // terminating zero byte
    }

    Preconditions.checkState(offset == autoGrowCharsBuf.dataSize());

    ByteBuffer chars = autoGrowCharsBuf.getByteBuffer();
    if (chars == initCharsBuf) {
      // Copy out.
      ByteBuffer newChars = MemoryUtil.allocateDirect(offset);
      MemoryUtil.copyMemory(MemoryUtil.getAddress(chars), MemoryUtil.getAddress(newChars), offset);
      chars = newChars;
    }

    return new CHColumnString(size, offsets, chars);
  }

  @Override
  public void serialize(WriteBuffer writer, CHColumn column) throws IOException {
    CHColumnString strCol = (CHColumnString) column;
    // chars is a pointer to the same memory of strCol.chars().
    ByteBuffer chars = MemoryUtil.duplicateDirectByteBuffer(strCol.chars());
    for (int i = 0; i < strCol.size(); i++) {
      chars.clear();

      int bytes = strCol.sizeAt(i) - 1;
      int pos = (int) strCol.offsetAt(i);
      chars.position(pos);
      chars.limit(pos + bytes);

      writer.writeVarUInt64(bytes);
      writer.write(chars);
    }
  }
}
