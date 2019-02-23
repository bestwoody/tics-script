package com.pingcap.ch.datatypes;

import static com.pingcap.common.MemoryUtil.allocateDirect;

import com.pingcap.ch.columns.CHColumn;
import com.pingcap.ch.columns.CHColumnDecimal;
import com.pingcap.common.MemoryUtil;
import com.pingcap.common.ReadBuffer;
import com.pingcap.common.WriteBuffer;
import java.io.IOException;
import java.nio.ByteBuffer;

public class CHTypeDecimal implements CHType {
  public int precision, scale;

  public static final int MAX_PRECISION = 38;
  public static final int MAX_SCALE = 38;
  public static final int SYSTEM_DEFAULT_PRECISION = 38;
  public static final int SYSTEM_DEFAULT_SCALE = 18;

  public CHTypeDecimal(int precision, int scale) {
    this.precision = precision;
    this.scale = scale;
  }

  /**
   * Checks if decimal's precision and scale is invalid in Spark
   *
   * @param precision precision
   * @param scale scale
   * @return whether invalid
   */
  public static boolean isInvalidDecimal(int precision, int scale) {
    return precision > CHTypeDecimal.MAX_PRECISION || scale > CHTypeDecimal.MAX_SCALE;
  }

  @Override
  public String name() {
    return "Decimal(" + precision + ", " + scale + ")";
  }

  @Override
  public CHColumn allocate(int maxSize) {
    return new CHColumnDecimal(precision, scale, maxSize);
  }

  @Override
  public CHColumn deserialize(ReadBuffer reader, int size) throws IOException {
    if (size == 0) {
      return new CHColumnDecimal(0, precision, scale, MemoryUtil.EMPTY_BYTE_BUFFER_DIRECT);
    }
    ByteBuffer buffer = allocateDirect(size * 64);
    reader.read(buffer);
    buffer.clear();
    return new CHColumnDecimal(size, precision, scale, buffer);
  }

  @Override
  public void serialize(WriteBuffer writer, CHColumn column) throws IOException {
    ByteBuffer data = MemoryUtil.duplicateDirectByteBuffer(((CHColumnDecimal) column).data());
    data.clear().limit(column.size() * 64);
    writer.write(data);
  }
}
