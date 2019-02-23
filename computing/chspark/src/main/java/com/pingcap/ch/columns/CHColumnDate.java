package com.pingcap.ch.columns;

import com.pingcap.ch.datatypes.CHTypeDate;
import com.pingcap.common.MemoryUtil;
import java.nio.ByteBuffer;

public class CHColumnDate extends CHColumn {
  private ByteBuffer data; // We need to keep a reference here to prevent the memory from gc.
  private long dataAddr;
  // In storage the date starts from 1400-01-01.
  public static final int DATE_EPOCH_OFFSET = -208188;

  public CHColumnDate(int size, ByteBuffer data) {
    super(CHTypeDate.instance, size);
    this.data = data;
    dataAddr = MemoryUtil.getAddress(data);
  }

  public CHColumnDate(int maxSize) {
    this(0, MemoryUtil.allocateDirect(maxSize << 2));
  }

  public ByteBuffer data() {
    return data;
  }

  @Override
  public long byteCount() {
    return size << 2;
  }

  @Override
  public int getInt(int rowId) {
    return DATE_EPOCH_OFFSET + MemoryUtil.getInt(dataAddr + (rowId << 2));
  }

  @Override
  public void insertInt(int v) {
    MemoryUtil.setInt(dataAddr + (size << 2), v - DATE_EPOCH_OFFSET);
    size++;
  }

  @Override
  public CHColumn seal() {
    data.clear();
    data.limit(size << 2);
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
