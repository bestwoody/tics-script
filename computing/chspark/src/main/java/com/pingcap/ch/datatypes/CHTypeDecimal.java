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

  public DecimalInternalType tp;
  public int decimalSize;

  public static final int MAX_PRECISION = 65;
  public static final int MAX_SCALE = 30;

  public CHTypeDecimal(int precision, int scale) {
    this.precision = precision;
    this.scale = scale;
    if (precision <= 9) {
      tp = DecimalInternalType.Decimal32;
      decimalSize = 4;
    } else if (precision <= 18) {
      tp = DecimalInternalType.Decimal64;
      decimalSize = 8;
    } else if (precision <= 38) {
      tp = DecimalInternalType.Decimal128;
      decimalSize = 16;
    } else {
      tp = DecimalInternalType.Decimal256;
      decimalSize = 48;
    }
  }

  @Override
  public String name() {
    return "Decimal(" + precision + ", " + scale + ")";
  }

  @Override
  public CHColumn allocate(int maxSize) {
    return new CHColumnDecimal(precision, scale, maxSize, tp, decimalSize);
  }

  @Override
  public CHColumn deserialize(ReadBuffer reader, int size) throws IOException {
    if (size == 0) {
      return new CHColumnDecimal(
          0, precision, scale, tp, decimalSize, MemoryUtil.EMPTY_BYTE_BUFFER_DIRECT);
    }
    ByteBuffer buffer = allocateDirect(size * decimalSize);
    reader.read(buffer);
    buffer.clear();
    return new CHColumnDecimal(size, precision, scale, tp, decimalSize, buffer);
  }

  @Override
  public void serialize(WriteBuffer writer, CHColumn column) throws IOException {
    ByteBuffer data = MemoryUtil.duplicateDirectByteBuffer(((CHColumnDecimal) column).data());
    data.clear().limit(column.size() * decimalSize);
    writer.write(data);
  }
}
