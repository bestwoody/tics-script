package com.pingcap.ch.datatypes;

import static com.pingcap.common.MemoryUtil.allocateDirect;

import com.pingcap.ch.columns.CHColumn;
import com.pingcap.ch.columns.CHColumnNumber;
import com.pingcap.common.MemoryUtil;
import com.pingcap.common.ReadBuffer;
import com.pingcap.common.WriteBuffer;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class CHTypeNumber implements CHType {

  public abstract int shift();

  @Override
  public CHColumn allocate(int maxSize) {
    return new CHColumnNumber(this, maxSize);
  }

  @Override
  public CHColumn deserialize(ReadBuffer reader, int size) throws IOException {
    if (size == 0) {
      return new CHColumnNumber(this, 0, MemoryUtil.EMPTY_BYTE_BUFFER_DIRECT);
    }
    ByteBuffer buffer = allocateDirect(size << shift());
    reader.read(buffer);
    buffer.clear();
    return new CHColumnNumber(this, size, buffer);
  }

  @Override
  public void serialize(WriteBuffer writer, CHColumn column) throws IOException {
    CHColumnNumber numberCol = (CHColumnNumber) column;
    ByteBuffer data = MemoryUtil.duplicateDirectByteBuffer(numberCol.data());
    data.clear().limit(column.size() << shift());
    writer.write(data);
  }

  public static class CHTypeUInt8 extends CHTypeNumber {
    public static final CHTypeUInt8 instance = new CHTypeUInt8();
    public static final CHTypeNullable nullableInstance = new CHTypeNullable(instance);

    private CHTypeUInt8() {}

    @Override
    public String name() {
      return "UInt8";
    }

    @Override
    public int shift() {
      return 0;
    }
  }

  public static class CHTypeUInt16 extends CHTypeNumber {
    public static final CHTypeUInt16 instance = new CHTypeUInt16();
    public static final CHTypeNullable nullableInstance = new CHTypeNullable(instance);

    private CHTypeUInt16() {}

    @Override
    public String name() {
      return "UInt16";
    }

    @Override
    public int shift() {
      return 1;
    }
  }

  public static class CHTypeUInt32 extends CHTypeNumber {
    public static final CHTypeUInt32 instance = new CHTypeUInt32();
    public static final CHTypeNullable nullableInstance = new CHTypeNullable(instance);

    private CHTypeUInt32() {}

    @Override
    public String name() {
      return "UInt32";
    }

    @Override
    public int shift() {
      return 2;
    }
  }

  public static class CHTypeUInt64 extends CHTypeNumber {
    public static final CHTypeUInt64 instance = new CHTypeUInt64();
    public static final CHTypeNullable nullableInstance = new CHTypeNullable(instance);

    private CHTypeUInt64() {}

    @Override
    public String name() {
      return "UInt64";
    }

    @Override
    public int shift() {
      return 3;
    }
  }

  public static class CHTypeInt8 extends CHTypeNumber {
    public static final CHTypeInt8 instance = new CHTypeInt8();
    public static final CHTypeNullable nullableInstance = new CHTypeNullable(instance);

    private CHTypeInt8() {}

    @Override
    public String name() {
      return "Int8";
    }

    @Override
    public int shift() {
      return 0;
    }
  }

  public static class CHTypeInt16 extends CHTypeNumber {
    public static final CHTypeInt16 instance = new CHTypeInt16();
    public static final CHTypeNullable nullableInstance = new CHTypeNullable(instance);

    private CHTypeInt16() {}

    @Override
    public String name() {
      return "Int16";
    }

    @Override
    public int shift() {
      return 1;
    }
  }

  public static class CHTypeInt32 extends CHTypeNumber {
    public static final CHTypeInt32 instance = new CHTypeInt32();
    public static final CHTypeNullable nullableInstance = new CHTypeNullable(instance);

    private CHTypeInt32() {}

    @Override
    public String name() {
      return "Int32";
    }

    @Override
    public int shift() {
      return 2;
    }
  }

  public static class CHTypeInt64 extends CHTypeNumber {
    public static final CHTypeInt64 instance = new CHTypeInt64();
    public static final CHTypeNullable nullableInstance = new CHTypeNullable(instance);

    private CHTypeInt64() {}

    @Override
    public String name() {
      return "Int64";
    }

    @Override
    public int shift() {
      return 3;
    }
  }

  public static class CHTypeFloat32 extends CHTypeNumber {
    public static final CHTypeFloat32 instance = new CHTypeFloat32();
    public static final CHTypeNullable nullableInstance = new CHTypeNullable(instance);

    private CHTypeFloat32() {}

    @Override
    public String name() {
      return "Float32";
    }

    @Override
    public int shift() {
      return 2;
    }
  }

  public static class CHTypeFloat64 extends CHTypeNumber {
    public static final CHTypeFloat64 instance = new CHTypeFloat64();
    public static final CHTypeNullable nullableInstance = new CHTypeNullable(instance);

    private CHTypeFloat64() {}

    @Override
    public String name() {
      return "Float64";
    }

    @Override
    public int shift() {
      return 3;
    }
  }
}
