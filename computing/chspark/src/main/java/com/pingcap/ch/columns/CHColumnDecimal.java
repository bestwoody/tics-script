package com.pingcap.ch.columns;

import com.pingcap.ch.datatypes.CHTypeDecimal;
import com.pingcap.ch.datatypes.DecimalInternalType;
import com.pingcap.common.MemoryUtil;
import java.nio.ByteBuffer;
import org.apache.spark.sql.types.Decimal;

public class CHColumnDecimal extends CHColumn {
  private int precision;
  private int scale;

  private ByteBuffer data;
  private long dataAddr;

  DecimalInternalType tp;
  int decimalSize;

  public CHColumnDecimal(
      int size, int prec, int scale, DecimalInternalType tp, int decimalSize, ByteBuffer data) {
    super(new CHTypeDecimal(prec, scale), size);
    this.precision = prec;
    this.scale = scale;
    this.data = data;
    this.dataAddr = MemoryUtil.getAddress(data);
    this.tp = tp;
    this.decimalSize = decimalSize;
  }

  public CHColumnDecimal(
      int prec, int scale, int maxSize, DecimalInternalType tp, int decimalSize) {
    this(0, prec, scale, tp, decimalSize, MemoryUtil.allocateDirect(decimalSize * maxSize));
  }

  public ByteBuffer data() {
    return data;
  }

  @Override
  public long byteCount() {
    return size * decimalSize;
  }

  @Override
  public Decimal getDecimal(int rowId) {
    if (tp == DecimalInternalType.Decimal256)
      return MemoryUtil.getDecimal256(rowId * decimalSize + dataAddr, scale);
    else if (tp == DecimalInternalType.Decimal128)
      return MemoryUtil.getDecimal128(rowId * decimalSize + dataAddr, scale);
    else if (tp == DecimalInternalType.Decimal64)
      return MemoryUtil.getDecimal64(rowId * decimalSize + dataAddr, scale);
    else return MemoryUtil.getDecimal32(rowId * decimalSize + dataAddr, scale);
  }

  @Override
  public void insertDecimal(Decimal v) {
    if (tp == DecimalInternalType.Decimal256)
      MemoryUtil.setDecimal256(dataAddr + size * decimalSize, v, scale);
    else MemoryUtil.setDecimal(dataAddr + size * decimalSize, v, scale);
    size++;
  }

  @Override
  public CHColumn seal() {
    data.clear();
    data.limit(size * decimalSize);
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
