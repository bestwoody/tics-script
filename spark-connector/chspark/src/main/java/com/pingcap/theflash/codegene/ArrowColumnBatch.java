package com.pingcap.theflash.codegene;

import org.apache.spark.sql.types.StructType;

public class ArrowColumnBatch {
  private final StructType schema;
  private final int capacity;
  private int numRows;
  private final ColumnVector[] columns;
  /**
   * Sets the number of rows in this batch.
   */
  public void setNumRows(int numRows) {
    assert(numRows <= this.capacity);
    this.numRows = numRows;
  }

  /**
   * Returns the number of columns that make up this batch.
   */
  public int numCols() { return columns.length; }

  /**
   * Returns the number of rows for read, including filtered rows.
   */
  public int numRows() { return numRows; }

  /**
   * Returns the schema that makes up this batch.
   */
  public StructType schema() { return schema; }

  /**
   * Returns the max capacity (in number of rows) for this batch.
   */
  public int capacity() { return capacity; }

  /**
   * Returns the column at `ordinal`.
   */
  public ColumnVector column(int ordinal) { return columns[ordinal]; }

  /**
   * Called to close all the columns in this batch. It is not valid to access the data after
   * calling this. This must be called at the end to clean up memory allocations.
   */
  public void close() {
    for (ColumnVector c: columns) {
      c.close();
    }
  }

  public ArrowColumnBatch(StructType schema, ColumnVector[] columns, int capacity) {
    this.schema = schema;
    this.capacity = capacity;
    this.columns = columns;
  }
}
