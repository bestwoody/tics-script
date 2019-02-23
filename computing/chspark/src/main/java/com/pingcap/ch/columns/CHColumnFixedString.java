package com.pingcap.ch.columns;

import com.pingcap.ch.datatypes.CHTypeFixedString;
import com.pingcap.common.MemoryUtil;
import java.nio.ByteBuffer;
import org.apache.spark.unsafe.types.UTF8String;

public class CHColumnFixedString extends CHColumn {
  private int valLen;

  private ByteBuffer data;
  private long dataAddr;

  public CHColumnFixedString(int size, int valLen, ByteBuffer data) {
    super(new CHTypeFixedString(valLen), size);
    this.valLen = valLen;
    this.data = data;
    this.dataAddr = MemoryUtil.getAddress(data);
  }

  public CHColumnFixedString(int valLen, int maxSize) {
    this(0, valLen, MemoryUtil.allocateDirect(valLen * maxSize));
  }

  public ByteBuffer data() {
    return data;
  }

  @Override
  public long byteCount() {
    return size * valLen;
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    // byte[] bytes = new byte[length];
    // MemoryUtil.getBytes(length * rowId, bytes, 0, length);
    // return UTF8String.fromBytes(bytes);

    return UTF8String.fromAddress(null, dataAddr + valLen * rowId, valLen);
  }

  @Override
  public void insertUTF8String(UTF8String v) {
    MemoryUtil.copyMemory(
        v.getBaseObject(),
        v.getBaseOffset(),
        null,
        dataAddr + (size * valLen),
        Math.min(v.numBytes(), valLen));
    size++;
  }

  @Override
  public CHColumn seal() {
    data.clear();
    data.limit(size * valLen);
    return this;
  }

  @Override
  public void free() {
    if (dataAddr == 0) {
      return;
    }
    MemoryUtil.free(data);
    dataAddr = 0;
  }
}
