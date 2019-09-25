package com.pingcap.ch.columns;

import com.pingcap.ch.datatypes.CHTypeMyDateTime;
import com.pingcap.common.MemoryUtil;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;

public class CHColumnMyDateTime extends CHColumn {
  private ByteBuffer data; // Keep a reference here to prevent the memory from gc.
  private long dataAddr;

  public CHColumnMyDateTime(int size, ByteBuffer data) {
    super(CHTypeMyDateTime.instance, size);
    this.data = data;
    dataAddr = MemoryUtil.getAddress(data);
  }

  public CHColumnMyDateTime(int maxSize) {
    this(0, MemoryUtil.allocateDirect(maxSize << 3));
  }

  public ByteBuffer data() {
    return data;
  }

  @Override
  public long byteCount() {
    return size << 3;
  }

  public static long toSparkValue(long v) {
    long ymdhms = v >> 24;
    long ymd = ymdhms >> 17;
    int day = (int) (ymd & ((1 << 5) - 1));
    long ym = ymd >> 5;
    int month = (int) (ym % 13);
    int year = (int) (ym / 13);

    long hms = ymdhms & ((1 << 17) - 1);
    int second = (int) (hms & ((1 << 6) - 1));
    int minute = (int) ((hms >> 6) & ((1 << 6) - 1));
    int hour = (int) (hms >> 12);

    int nano = (int) (v % (1 << 24));

    Timestamp ts = new Timestamp(year - 1900, month - 1, day, hour, minute, second, nano);
    return DateTimeUtils.fromJavaTimestamp(ts);
  }

  public static long fromSparkValue(long v) {
    Timestamp ts = DateTimeUtils.toJavaTimestamp(v);
    int year = ts.getYear() + 1900;
    int month = ts.getMonth() + 1;
    int day = ts.getDate();
    int hour = ts.getHours();
    int minute = ts.getMinutes();
    int seconds = ts.getSeconds();
    int micro_seconds = ts.getNanos();
    long ymd = ((year * 13 + month) << 5) | day;
    long hms = hour << 12 | minute << 6 | seconds;
    return (ymd << 17 | hms) << 24 | micro_seconds;
  }

  @Override
  public long getLong(int rowId) {
    long v = MemoryUtil.getLong(dataAddr + (rowId << 3));
    return toSparkValue(v);
  }

  @Override
  public void insertLong(long v) {
    long packed = fromSparkValue(v);
    MemoryUtil.setLong(dataAddr + (size << 3), packed);
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
