package com.pingcap.theflash;

import org.apache.spark.sql.types.DataType;

public class DataTypeAndNullable {
  public DataType dataType;
  public boolean nullable;

  public DataTypeAndNullable(DataType type, boolean nullable) {
    this.dataType = type;
    this.nullable = nullable;
  }

  public DataTypeAndNullable(DataType type) {
    this(type, false);
  }
}
