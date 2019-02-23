package com.pingcap.ch.datatypes;

import com.pingcap.ch.columns.CHColumn;
import com.pingcap.common.ReadBuffer;
import com.pingcap.common.WriteBuffer;
import java.io.IOException;

// TODO Support nullable data types.
// TODO Support nested, array and struct types.
public interface CHType {
  String name();

  CHColumn allocate(int maxSize);

  CHColumn deserialize(ReadBuffer reader, int size) throws IOException;

  void serialize(WriteBuffer writer, CHColumn column) throws IOException;
}
