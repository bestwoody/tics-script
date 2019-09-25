package com.pingcap.ch.columns;

import com.pingcap.ch.datatypes.CHTypeMyDate;
import com.pingcap.common.MemoryUtil;
import java.nio.ByteBuffer;
import java.sql.Date;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;

public class CHColumnMyDate extends CHColumn {
  private ByteBuffer data; // We need to keep a reference here to prevent the memory from gc.
  private long dataAddr;

  public CHColumnMyDate(int size, ByteBuffer data) {
    super(CHTypeMyDate.instance, size);
    this.data = data;
    dataAddr = MemoryUtil.getAddress(data);
  }

  public CHColumnMyDate(int maxSize) {
    this(0, MemoryUtil.allocateDirect(maxSize << 3));
  }

  public ByteBuffer data() {
    return data;
  }

  @Override
  public long byteCount() {
    return size << 3;
  }

  @Override
  public long getLong(int rowId) {
    long v = MemoryUtil.getLong(dataAddr + (rowId << 3));
    long ymd = v >> 41;
    int year = (int) ((ymd >> 5) / 13) - 1900;
    int month = (int) ((ymd >> 5) % 13) - 1;
    int day = (int) (ymd & ((1 << 5) - 1));
    Date dt = new Date(year, month, day);
    return DateTimeUtils.fromJavaDate(dt);
  }

  @Override
  public void insertLong(long v) {
    Date date = DateTimeUtils.toJavaDate((int) v);
    int year = date.getYear() + 1900;
    int day = date.getDate();
    int month = date.getMonth() + 1;
    long ymd = ((year * 13 + month) << 5) | day;
    MemoryUtil.setLong(dataAddr + (size << 3), ymd << 41);
    size++;
  }

  @Override
  public CHColumn seal() {
    data.clear();
    data.limit(size << 3);
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
