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

    if (vector instanceof NullableBitVector) {
      accessor = new NullableBooleanAccessor((NullableBitVector) vector);
    } else if (vector instanceof BitVector) {
      accessor = new BooleanAccessor((BitVector) vector);
    } else if (vector instanceof TinyIntVector) {
      accessor = new ByteAccessor((TinyIntVector) vector);
    } else if (vector instanceof NullableTinyIntVector) {
      accessor = new NullableByteAccessor((NullableTinyIntVector) vector);
    } else if (vector instanceof SmallIntVector) {
      accessor = new ShortAccessor((SmallIntVector) vector);
    } else if (vector instanceof NullableSmallIntVector) {
      accessor = new NullableShortAccessor((NullableSmallIntVector) vector);
    } else if (vector instanceof IntVector) {
      accessor = new IntAccessor((IntVector) vector);
    } else if (vector instanceof NullableIntVector) {
      accessor = new NullableIntAccessor((NullableIntVector) vector);
    } else if (vector instanceof NullableUInt1Vector) {
      accessor = new NullableUInt1Accessor((NullableUInt1Vector) vector);
    } else if (vector instanceof NullableUInt2Vector) {
      accessor = new NullableUInt2Accessor((NullableUInt2Vector) vector);
    } else if (vector instanceof NullableUInt4Vector) {
      accessor = new NullableUInt4Accessor((NullableUInt4Vector) vector);
    } else if (vector instanceof NullableUInt8Vector) {
      accessor = new NullableUInt8Accessor((NullableUInt8Vector) vector);
    } else if (vector instanceof BigIntVector) {
      accessor = new LongAccessor((BigIntVector) vector);
    } else if (vector instanceof NullableBigIntVector) {
      accessor = new NullableLongAccessor((NullableBigIntVector) vector);
    } else if (vector instanceof Float4Vector) {
      accessor = new FloatAccessor((Float4Vector) vector);
    } else if (vector instanceof NullableFloat4Vector) {
      accessor = new NullableFloatAccessor((NullableFloat4Vector) vector);
    } else if (vector instanceof Float8Vector) {
      accessor = new DoubleAccessor((Float8Vector) vector);
    } else if (vector instanceof NullableFloat8Vector) {
      accessor = new NullableDoubleAccessor((NullableFloat8Vector) vector);
    } else if (vector instanceof DecimalVector) {
      accessor = new DecimalAccessor((DecimalVector) vector);
    } else if (vector instanceof NullableDecimalVector) {
      accessor = new NullableDecimalAccessor((NullableDecimalVector) vector);
    } else if (vector instanceof VarCharVector) {
      accessor = new StringAccessor((VarCharVector) vector);
    } else if (vector instanceof NullableVarCharVector) {
      accessor = new NullableStringAccessor((NullableVarCharVector) vector);
    } else if (vector instanceof VarBinaryVector) {
      accessor = new BinaryAccessor((VarBinaryVector) vector);
    } else if (vector instanceof NullableTimeNanoVector) {
      accessor = new NullableTimeNanoAccessor((NullableTimeNanoVector) vector);
    } else if (vector instanceof DateDayVector) {
      accessor = new DateAccessor((DateDayVector) vector);
    } else if (vector instanceof TimeStampMicroTZVector) {
      accessor = new TimestampAccessor((TimeStampMicroTZVector) vector);
    } else if (vector instanceof NullableDateDayVector) {
      accessor = new NullableDateAccessor((NullableDateDayVector) vector);
    } else if (vector instanceof NullableTimeStampMicroTZVector) {
      accessor = new NullableTimestampAccessor((NullableTimeStampMicroTZVector) vector);
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
      System.out.println("Unsupported vector:" +vector.getClass().getName());
      throw new UnsupportedOperationException();
    }
  }

  private abstract static class ArrowVectorAccessor {

    private final ValueVector vector;
    private final ValueVector.Accessor accessor;

    ArrowVectorAccessor(ValueVector vector) {
      this.vector = vector;
      this.accessor = vector.getAccessor();
    }

    // TODO: should be final after removing ArrayAccessor workaround
    boolean isNullAt(int rowId) {
      return accessor.isNull(rowId);
    }

    final int getValueCount() {
      return accessor.getValueCount();
    }

    final int getNullCount() {
      return accessor.getNullCount();
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
      return accessor.getAccessor().get(rowId) == 1;
    }
  }

  private static class NullableBooleanAccessor extends ArrowVectorAccessor {

    private final NullableBitVector accessor;

    NullableBooleanAccessor(NullableBitVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    boolean getBoolean(int rowId) {
      return accessor.getAccessor().get(rowId) == 1;
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
      return accessor.getAccessor().get(rowId);
    }
  }

  private static class NullableByteAccessor extends ArrowVectorAccessor {

    private final NullableTinyIntVector accessor;

    NullableByteAccessor(NullableTinyIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final byte getByte(int rowId) {
      return accessor.getAccessor().get(rowId);
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
      return accessor.getAccessor().get(rowId);
    }
  }

  private static class NullableShortAccessor extends ArrowVectorAccessor {

    private final NullableSmallIntVector accessor;

    NullableShortAccessor(NullableSmallIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final short getShort(int rowId) {
      return accessor.getAccessor().get(rowId);
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
      return accessor.getAccessor().get(rowId);
    }
  }

  private static class NullableIntAccessor extends ArrowVectorAccessor {

    private final NullableIntVector accessor;

    NullableIntAccessor(NullableIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final int getInt(int rowId) {
      return accessor.getAccessor().get(rowId);
    }
  }

  private static class NullableUInt1Accessor extends ArrowVectorAccessor {

    private final NullableUInt1Vector accessor;

    NullableUInt1Accessor(NullableUInt1Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final int getInt(int rowId) {
      return accessor.getAccessor().get(rowId);
    }

    // TODO support unsigned promotion
    @Override
    final long getLong(int rowId) {
      return accessor.getAccessor().get(rowId);
    }
  }

  private static class NullableUInt2Accessor extends ArrowVectorAccessor {

    private final NullableUInt2Vector accessor;

    NullableUInt2Accessor(NullableUInt2Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final int getInt(int rowId) {
      return accessor.getAccessor().get(rowId);
    }

    // TODO support unsigned promotion
    @Override
    final long getLong(int rowId) {
      return accessor.getAccessor().get(rowId);
    }
  }

  private static class NullableUInt4Accessor extends ArrowVectorAccessor {

    private final NullableUInt4Vector accessor;

    NullableUInt4Accessor(NullableUInt4Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final int getInt(int rowId) {
      return accessor.getAccessor().get(rowId);
    }

    // TODO support unsigned promotion
    @Override
    final long getLong(int rowId) {
      return accessor.getAccessor().get(rowId);
    }
  }

  private static class NullableUInt8Accessor extends ArrowVectorAccessor {

    private final NullableUInt8Vector accessor;

    NullableUInt8Accessor(NullableUInt8Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.getAccessor().get(rowId);
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
      return accessor.getAccessor().get(rowId);
    }
  }

  private static class NullableLongAccessor extends ArrowVectorAccessor {

    private final NullableBigIntVector accessor;

    NullableLongAccessor(NullableBigIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.getAccessor().get(rowId);
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
      return accessor.getAccessor().get(rowId);
    }
  }

  private static class NullableFloatAccessor extends ArrowVectorAccessor {

    private final NullableFloat4Vector accessor;

    NullableFloatAccessor(NullableFloat4Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final float getFloat(int rowId) {
      return accessor.getAccessor().get(rowId);
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
      return accessor.getAccessor().get(rowId);
    }
  }

  private static class NullableDoubleAccessor extends ArrowVectorAccessor {

    private final NullableFloat8Vector accessor;

    NullableDoubleAccessor(NullableFloat8Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final double getDouble(int rowId) {
      return accessor.getAccessor().get(rowId);
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
      return Decimal.apply(accessor.getAccessor().getObject(rowId), precision, scale);
    }
  }

  private static class NullableDecimalAccessor extends ArrowVectorAccessor {

    private final NullableDecimalVector accessor;

    NullableDecimalAccessor(NullableDecimalVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final Decimal getDecimal(int rowId, int precision, int scale) {
      if (isNullAt(rowId)) return null;
      return Decimal.apply(accessor.getAccessor().getObject(rowId), precision, scale);
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
      accessor.getAccessor().get(rowId, stringResult);
      if (stringResult.isSet == 0) {
        return null;
      } else {
        return UTF8String.fromAddress(null,
            stringResult.buffer.memoryAddress() + stringResult.start,
            stringResult.end - stringResult.start);
      }
    }
  }

  private static class NullableStringAccessor extends ArrowVectorAccessor {
    private final NullableVarCharVector accessor;
    private final NullableVarCharHolder stringResult = new NullableVarCharHolder();

    private NullableStringAccessor(NullableVarCharVector accessor) {
      super(accessor);
      this.accessor = accessor;
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      accessor.getAccessor().get(rowId, stringResult);
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
      return accessor.getAccessor().getObject(rowId);
    }
  }

  private static class NullableBinaryAccessor extends ArrowVectorAccessor {

    private final NullableVarBinaryVector accessor;

    NullableBinaryAccessor(NullableVarBinaryVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final byte[] getBinary(int rowId) {
      return accessor.getAccessor().getObject(rowId);
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
      return accessor.getAccessor().get(rowId);
    }
  }

  private static class NullableDateAccessor extends ArrowVectorAccessor {

    private final NullableDateDayVector accessor;

    NullableDateAccessor(NullableDateDayVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final int getInt(int rowId) {
      return accessor.getAccessor().get(rowId);
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
      return accessor.getAccessor().get(rowId);
    }
  }

  private static class NullableTimestampAccessor extends ArrowVectorAccessor {

    private final NullableTimeStampMicroTZVector accessor;

    NullableTimestampAccessor(NullableTimeStampMicroTZVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.getAccessor().get(rowId);
    }
  }

  private static class NullableTimeNanoAccessor extends ArrowVectorAccessor {
    private final NullableTimeNanoVector accessor;

    private NullableTimeNanoAccessor(NullableTimeNanoVector accessor) {
      super(accessor);
      this.accessor = accessor;
    }

    @Override
    long getLong(int rowId) {
      return accessor.getAccessor().get(rowId) * 1000;
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
      if (accessor.getAccessor().getValueCount() > 0 && accessor.getValidityBuffer().capacity() == 0) {
        return false;
      } else {
        return super.isNullAt(rowId);
      }
    }

    @Override
    final int getArrayLength(int rowId) {
      return accessor.getAccessor().getInnerValueCountAt(rowId);
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
