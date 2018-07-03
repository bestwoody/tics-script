package org.apache.spark.sql.ch;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;
import scala.collection.immutable.Map;

/**
 * Simple row used for test.
 */
public class SimpleRow implements Row {
  private final Object[] values;

  public SimpleRow(Object[] vals) {
    this.values = vals;
  }

  @Override
  public int size() {
    return values.length;
  }

  @Override
  public int length() {
    return size();
  }

  @Override
  public StructType schema() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object apply(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object get(int i) {
    return values[i];
  }

  @Override
  public boolean isNullAt(int i) {
    return values[i] == null;
  }

  @Override
  public boolean getBoolean(int i) {
    return (boolean)values[i];
  }

  @Override
  public byte getByte(int i) {
    return (byte)values[i];
  }

  @Override
  public short getShort(int i) {
    return (short)values[i];
  }

  @Override
  public int getInt(int i) {
    return (int)values[i];
  }

  @Override
  public long getLong(int i) {
    return (long)values[i];
  }

  @Override
  public float getFloat(int i) {
    return (float)values[i];
  }

  @Override
  public double getDouble(int i) {
    return (double)values[i];
  }

  @Override
  public String getString(int i) {
    return (String)values[i];
  }

  @Override
  public BigDecimal getDecimal(int i) {
    return (BigDecimal)values[i];
  }

  @Override
  public Date getDate(int i) {
    return (Date)values[i];
  }

  @Override
  public Timestamp getTimestamp(int i) {
    return (Timestamp)values[i];
  }

  @Override
  public <T> Seq<T> getSeq(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> List<T> getList(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <K, V> scala.collection.Map<K, V> getMap(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <K, V> java.util.Map<K, V> getJavaMap(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Row getStruct(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T getAs(int i) {
    return (T)values[i];
  }

  @Override
  public <T> T getAs(String fieldName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int fieldIndex(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> Map<String, T> getValuesMap(Seq<String> fieldNames) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Row copy() {
    return null;
  }

  @Override
  public boolean anyNull() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Seq<Object> toSeq() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String mkString() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String mkString(String sep) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String mkString(String start, String sep, String end) {
    throw new UnsupportedOperationException();
  }
}
