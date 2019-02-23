package com.pingcap.ch.columns;

import com.pingcap.ch.datatypes.CHTypeNullable;
import com.pingcap.ch.datatypes.CHTypeNumber;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

public class CHColumnNullable extends CHColumn {
  public final CHColumnNumber null_map;
  public final CHColumn nested_column;

  public CHColumnNullable(CHTypeNullable type, CHColumnNumber null_map, CHColumn nested_column) {
    super(type, null_map.size());
    this.null_map = null_map;
    this.nested_column = nested_column;
  }

  public CHColumnNullable(CHTypeNullable type, int maxSize) {
    this(
        type,
        new CHColumnNumber(CHTypeNumber.CHTypeUInt8.instance, maxSize),
        type.nested_data_type.allocate(maxSize));
  }

  @Override
  public int size() {
    return nested_column.size();
  }

  @Override
  public long byteCount() {
    return null_map.byteCount() + nested_column.byteCount();
  }

  @Override
  public void free() {
    null_map.free();
    nested_column.free();
  }

  @Override
  public boolean isNullAt(int rowId) {
    return null_map.getByte(rowId) != 0;
  }

  @Override
  public byte getByte(int rowId) {
    return nested_column.getByte(rowId);
  }

  @Override
  public short getShort(int rowId) {
    return nested_column.getShort(rowId);
  }

  @Override
  public int getInt(int rowId) {
    return nested_column.getInt(rowId);
  }

  @Override
  public long getLong(int rowId) {
    return nested_column.getLong(rowId);
  }

  @Override
  public float getFloat(int rowId) {
    return nested_column.getFloat(rowId);
  }

  @Override
  public double getDouble(int rowId) {
    return nested_column.getDouble(rowId);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    return nested_column.getUTF8String(rowId);
  }

  @Override
  public Decimal getDecimal(int rowId) {
    return nested_column.getDecimal(rowId);
  }

  @Override
  public void insertDefault() {
    null_map.insertDefault();
    nested_column.insertDefault();
  }

  @Override
  public void insertNull() {
    null_map.insertByte((byte) 1);
    nested_column.insertDefault();
  }

  @Override
  public void insertByte(byte v) {
    null_map.insertByte((byte) 0);
    nested_column.insertByte(v);
  }

  @Override
  public void insertShort(short v) {
    null_map.insertByte((byte) 0);
    nested_column.insertShort((short) v);
  }

  @Override
  public void insertInt(int v) {
    null_map.insertByte((byte) 0);
    nested_column.insertInt(v);
  }

  @Override
  public void insertLong(long v) {
    null_map.insertByte((byte) 0);
    nested_column.insertLong(v);
  }

  @Override
  public void insertFloat(float v) {
    null_map.insertByte((byte) 0);
    nested_column.insertFloat(v);
  }

  @Override
  public void insertDouble(double v) {
    null_map.insertByte((byte) 0);
    nested_column.insertDouble(v);
  }

  @Override
  public void insertUTF8String(UTF8String v) {
    null_map.insertByte((byte) 0);
    nested_column.insertUTF8String(v);
  }

  @Override
  public void insertDecimal(Decimal v) {
    null_map.insertByte((byte) 0);
    nested_column.insertDecimal(v);
  }

  @Override
  public CHColumn seal() {
    null_map.seal();
    nested_column.seal();
    size = null_map.size();
    return this;
  }
}
