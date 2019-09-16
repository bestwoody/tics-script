package com.pingcap.ch.columns;

import com.pingcap.ch.datatypes.CHTypeDate;
import com.pingcap.common.MemoryUtil;
import java.nio.ByteBuffer;

public class CHColumnDate extends CHColumn {
  private ByteBuffer data; // We need to keep a reference here to prevent the memory from gc.
  private long dataAddr;

  public CHColumnDate(int size, ByteBuffer data) {
    super(CHTypeDate.instance, size);
    this.data = data;
    dataAddr = MemoryUtil.getAddress(data);
  }

  public CHColumnDate(int maxSize) {
    this(0, MemoryUtil.allocateDirect(maxSize << 1));
  }

  public ByteBuffer data() {
    return data;
  }

  @Override
  public long byteCount() {
    return size << 1;
  }

  @Override
  public short getShort(int rowId) {
    return MemoryUtil.getShort(dataAddr + (rowId << 1));
  }

  @Override
  public void insertShort(short v) {
    MemoryUtil.setShort(dataAddr + (size << 1), v);
    size++;
  }

  @Override
  public CHColumn seal() {
    data.clear();
    data.limit(size << 1);
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
