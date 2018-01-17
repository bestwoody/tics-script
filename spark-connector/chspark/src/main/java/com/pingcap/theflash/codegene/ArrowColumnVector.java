/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.theflash.codegene;

import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.holders.NullableVarCharHolder;

import org.apache.spark.sql.execution.arrow.ArrowUtils;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

public final class ArrowColumnVector extends ColumnVector {

  private final ArrowVectorAccessor accessor;
  private ArrowColumnVector[] childColumns;

  private void ensureAccessible(int index) {
    ensureAccessible(index, 1);
  }

  private void ensureAccessible(int index, int count) {
    int valueCount = accessor.getValueCount();
    if (index < 0 || index + count > valueCount) {
      throw new IndexOutOfBoundsException(
          String.format("index range: [%d, %d), valueCount: %d", index, index + count, valueCount));
    }
  }

  @Override
  public int numNulls() {
    return accessor.getNullCount();
  }

  @Override
  public void close() {
    if (childColumns != null) {
      for (ArrowColumnVector childColumn : childColumns) {
        childColumn.close();
      }
    }
    accessor.close();
  }

  @Override
  public boolean isNullAt(int rowId) {
    ensureAccessible(rowId);
    return accessor.isNullAt(rowId);
  }

  @Override
  public boolean getBoolean(int rowId) {
    ensureAccessible(rowId);
    return accessor.getBoolean(rowId);
  }

  @Override
  public boolean[] getBooleans(int rowId, int count) {
    ensureAccessible(rowId, count);
    boolean[] array = new boolean[count];
    for (int i = 0; i < count; ++i) {
      array[i] = accessor.getBoolean(rowId + i);
    }
    return array;
  }

  @Override
  public byte getByte(int rowId) {
    ensureAccessible(rowId);
    return accessor.getByte(rowId);
  }

  @Override
  public byte[] getBytes(int rowId, int count) {
    ensureAccessible(rowId, count);
    byte[] array = new byte[count];
    for (int i = 0; i < count; ++i) {
      array[i] = accessor.getByte(rowId + i);
    }
    return array;
  }

  @Override
  public short getShort(int rowId) {
    ensureAccessible(rowId);
    return accessor.getShort(rowId);
  }

  @Override
  public short[] getShorts(int rowId, int count) {
    ensureAccessible(rowId, count);
    short[] array = new short[count];
    for (int i = 0; i < count; ++i) {
      array[i] = accessor.getShort(rowId + i);
    }
    return array;
  }

  @Override
  public int getInt(int rowId) {
    ensureAccessible(rowId);
    return accessor.getInt(rowId);
  }

  @Override
  public int[] getInts(int rowId, int count) {
    ensureAccessible(rowId, count);
    int[] array = new int[count];
    for (int i = 0; i < count; ++i) {
      array[i] = accessor.getInt(rowId + i);
    }
    return array;
  }

  @Override
  public long getLong(int rowId) {
    ensureAccessible(rowId);
    return accessor.getLong(rowId);
  }

  @Override
  public long[] getLongs(int rowId, int count) {
    ensureAccessible(rowId, count);
    long[] array = new long[count];
    for (int i = 0; i < count; ++i) {
      array[i] = accessor.getLong(rowId + i);
    }
    return array;
  }

  @Override
  public float getFloat(int rowId) {
    ensureAccessible(rowId);
    return accessor.getFloat(rowId);
  }

  @Override
  public float[] getFloats(int rowId, int count) {
    ensureAccessible(rowId, count);
    float[] array = new float[count];
    for (int i = 0; i < count; ++i) {
      array[i] = accessor.getFloat(rowId + i);
    }
    return array;
  }

  @Override
  public double getDouble(int rowId) {
    ensureAccessible(rowId);
    return accessor.getDouble(rowId);
  }

  @Override
  public double[] getDoubles(int rowId, int count) {
    ensureAccessible(rowId, count);
    double[] array = new double[count];
    for (int i = 0; i < count; ++i) {
      array[i] = accessor.getDouble(rowId + i);
    }
    return array;
  }

  @Override
  public int getArrayLength(int rowId) {
    ensureAccessible(rowId);
    return accessor.getArrayLength(rowId);
  }

  @Override
  public int getArrayOffset(int rowId) {
    ensureAccessible(rowId);
    return accessor.getArrayOffset(rowId);
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    ensureAccessible(rowId);
    return accessor.getDecimal(rowId, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    ensureAccessible(rowId);
    return accessor.getUTF8String(rowId);
  }

  @Override
  public byte[] getBinary(int rowId) {
    ensureAccessible(rowId);
    return accessor.getBinary(rowId);
  }

  @Override
  public ArrowColumnVector arrayData() { return childColumns[0]; }

  @Override
  public ArrowColumnVector getChildColumn(int ordinal) { return childColumns[ordinal]; }

  public ArrowColumnVector(ValueVector vector) {
    super(ArrowUtils.fromArrowField(vector.getField()));

    if (vector instanceof BitVector) {
      accessor = new BooleanAccessor((BitVector) vector);
    } else if (vector instanceof TinyIntVector) {
      accessor = new ByteAccessor((TinyIntVector) vector);
    } else if (vector instanceof SmallIntVector) {
      accessor = new ShortAccessor((SmallIntVector) vector);
    } else if (vector instanceof IntVector) {
      accessor = new IntAccessor((IntVector) vector);
    } else if (vector instanceof UInt1Vector) {
      accessor = new UInt1Accessor((UInt1Vector) vector);
    } else if (vector instanceof UInt2Vector) {
      accessor = new UInt2Accessor((UInt2Vector) vector);
    } else if (vector instanceof UInt4Vector) {
      accessor = new UInt4Accessor((UInt4Vector) vector);
    } else if (vector instanceof UInt8Vector) {
      accessor = new UInt8Accessor((UInt8Vector) vector);
    } else if (vector instanceof BigIntVector) {
      accessor = new LongAccessor((BigIntVector) vector);
    } else if (vector instanceof Float4Vector) {
      accessor = new FloatAccessor((Float4Vector) vector);
    } else if (vector instanceof Float8Vector) {
      accessor = new DoubleAccessor((Float8Vector) vector);
    } else if (vector instanceof DecimalVector) {
      accessor = new DecimalAccessor((DecimalVector) vector);
    } else if (vector instanceof VarCharVector) {
      accessor = new StringAccessor((VarCharVector) vector);
    } else if (vector instanceof VarBinaryVector) {
      accessor = new BinaryAccessor((VarBinaryVector) vector);
    } else if (vector instanceof TimeNanoVector) {
      accessor = new TimeNanoAccessor((TimeNanoVector) vector);
    } else if (vector instanceof DateDayVector) {
      accessor = new DateAccessor((DateDayVector) vector);
    } else if (vector instanceof TimeStampMicroTZVector) {
      accessor = new TimestampAccessor((TimeStampMicroTZVector) vector);
    } else if (vector instanceof TimeStampNanoTZVector) {
      accessor = new TimestampNanoAccessor((TimeStampNanoTZVector) vector);
    } else if (vector instanceof ListVector) {
      ListVector listVector = (ListVector) vector;
      accessor = new ArrayAccessor(listVector);

      childColumns = new ArrowColumnVector[1];
      childColumns[0] = new ArrowColumnVector(listVector.getDataVector());
    } else if (vector instanceof MapVector) {
      MapVector mapVector = (MapVector) vector;
      accessor = new StructAccessor(mapVector);

      childColumns = new ArrowColumnVector[mapVector.size()];
      for (int i = 0; i < childColumns.length; ++i) {
        childColumns[i] = new ArrowColumnVector(mapVector.getVectorById(i));
      }
    } else {
      System.out.println("Unsupported vector:" + vector.getClass().getName());
      throw new UnsupportedOperationException();
    }
  }

  private abstract static class ArrowVectorAccessor {

    private final ValueVector vector;

    ArrowVectorAccessor(ValueVector vector) {
      this.vector = vector;
    }

    // TODO: should be final after removing ArrayAccessor workaround
    boolean isNullAt(int rowId) {
      return vector.isNull(rowId);
    }

    final int getValueCount() {
      return vector.getValueCount();
    }

    final int getNullCount() {
      return vector.getNullCount();
    }

    final void close() {
      vector.close();
    }

    boolean getBoolean(int rowId) {
      throw new UnsupportedOperationException();
    }

    byte getByte(int rowId) {
      throw new UnsupportedOperationException();
    }

    short getShort(int rowId) {
      throw new UnsupportedOperationException();
    }

    int getInt(int rowId) {
      throw new UnsupportedOperationException();
    }

    long getLong(int rowId) {
      throw new UnsupportedOperationException();
    }

    float getFloat(int rowId) {
      throw new UnsupportedOperationException();
    }

    double getDouble(int rowId) {
      throw new UnsupportedOperationException();
    }

    Decimal getDecimal(int rowId, int precision, int scale) {
      throw new UnsupportedOperationException();
    }

    UTF8String getUTF8String(int rowId) {
      throw new UnsupportedOperationException();
    }

    byte[] getBinary(int rowId) {
      throw new UnsupportedOperationException();
    }

    int getArrayLength(int rowId) {
      throw new UnsupportedOperationException();
    }

    int getArrayOffset(int rowId) {
      throw new UnsupportedOperationException();
    }
  }

  private static class BooleanAccessor extends ArrowVectorAccessor {

    private final BitVector accessor;

    BooleanAccessor(BitVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final boolean getBoolean(int rowId) {
      return accessor.get(rowId) == 1;
    }
  }

  private static class ByteAccessor extends ArrowVectorAccessor {

    private final TinyIntVector accessor;

    ByteAccessor(TinyIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final byte getByte(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class ShortAccessor extends ArrowVectorAccessor {

    private final SmallIntVector accessor;

    ShortAccessor(SmallIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final short getShort(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class IntAccessor extends ArrowVectorAccessor {

    private final IntVector accessor;

    IntAccessor(IntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final int getInt(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class UInt1Accessor extends ArrowVectorAccessor {

    private final UInt1Vector accessor;
    private static final int REVERSER = 0x100;

    UInt1Accessor(UInt1Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final int getInt(int rowId) {
      int value = accessor.get(rowId);
      if (value < 0) {
        return value + REVERSER;
      } else {
        return value;
      }
    }
  }

  private static class UInt2Accessor extends ArrowVectorAccessor {

    private final UInt2Vector accessor;

    UInt2Accessor(UInt2Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final int getInt(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class UInt4Accessor extends ArrowVectorAccessor {

    private final UInt4Vector accessor;
    private static long uint32Reverser = 0x100000000L;

    UInt4Accessor(UInt4Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      long value = accessor.get(rowId);
      if (value < 0) {
        return value + uint32Reverser;
      } else {
        return value;
      }
    }
  }

  private static class UInt8Accessor extends ArrowVectorAccessor {

    private final UInt8Vector accessor;

    UInt8Accessor(UInt8Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class LongAccessor extends ArrowVectorAccessor {

    private final BigIntVector accessor;

    LongAccessor(BigIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class FloatAccessor extends ArrowVectorAccessor {

    private final Float4Vector accessor;

    FloatAccessor(Float4Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final float getFloat(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class DoubleAccessor extends ArrowVectorAccessor {

    private final Float8Vector accessor;

    DoubleAccessor(Float8Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final double getDouble(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class DecimalAccessor extends ArrowVectorAccessor {

    private final DecimalVector accessor;

    DecimalAccessor(DecimalVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final Decimal getDecimal(int rowId, int precision, int scale) {
      if (isNullAt(rowId)) return null;
      return Decimal.apply(accessor.getObject(rowId), precision, scale);
    }
  }

  private static class StringAccessor extends ArrowVectorAccessor {

    private final VarCharVector accessor;
    private final NullableVarCharHolder stringResult = new NullableVarCharHolder();

    StringAccessor(VarCharVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      accessor.get(rowId, stringResult);
      if (stringResult.isSet == 0) {
        return null;
      } else {
        return UTF8String.fromAddress(null,
          stringResult.buffer.memoryAddress() + stringResult.start,
          stringResult.end - stringResult.start);
      }
    }
  }

  private static class BinaryAccessor extends ArrowVectorAccessor {

    private final VarBinaryVector accessor;

    BinaryAccessor(VarBinaryVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final byte[] getBinary(int rowId) {
      return accessor.getObject(rowId);
    }
  }

  private static class DateAccessor extends ArrowVectorAccessor {

    private final DateDayVector accessor;

    DateAccessor(DateDayVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final int getInt(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class TimestampAccessor extends ArrowVectorAccessor {

    private final TimeStampMicroTZVector accessor;

    TimestampAccessor(TimeStampMicroTZVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class TimestampNanoAccessor extends ArrowVectorAccessor {

    private final TimeStampNanoTZVector accessor;

    TimestampNanoAccessor(TimeStampNanoTZVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class TimeNanoAccessor extends ArrowVectorAccessor {
    private final TimeNanoVector accessor;

    private TimeNanoAccessor(TimeNanoVector accessor) {
      super(accessor);
      this.accessor = accessor;
    }

    @Override
    long getLong(int rowId) {
      return accessor.get(rowId) * 1000;
    }
  }

  private static class ArrayAccessor extends ArrowVectorAccessor {

    private final ListVector accessor;

    ArrayAccessor(ListVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final boolean isNullAt(int rowId) {
      // TODO: Workaround if vector has all non-null values, see ARROW-1948
      if (accessor.getValueCount() > 0 && accessor.getValidityBuffer().capacity() == 0) {
        return false;
      } else {
        return super.isNullAt(rowId);
      }
    }

    @Override
    final int getArrayLength(int rowId) {
      return accessor.getInnerValueCountAt(rowId);
    }

    @Override
    final int getArrayOffset(int rowId) {
      return accessor.getOffsetBuffer().getInt(rowId * 4);
    }
  }

  private static class StructAccessor extends ArrowVectorAccessor {

    StructAccessor(MapVector vector) {
      super(vector);
    }
  }
}
