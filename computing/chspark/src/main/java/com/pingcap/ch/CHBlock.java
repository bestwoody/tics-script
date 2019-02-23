package com.pingcap.ch;

import com.pingcap.ch.columns.CHColumnWithTypeAndName;
import java.util.ArrayList;
import java.util.List;

public class CHBlock {
  private CHBlockInfo info;
  private ArrayList<CHColumnWithTypeAndName> columns;

  public CHBlock(CHBlockInfo info, ArrayList<CHColumnWithTypeAndName> columns) {
    this.info = info;
    this.columns = columns;
  }

  public CHBlock() {
    this(new CHBlockInfo(), new ArrayList<CHColumnWithTypeAndName>());
  }

  public CHBlockInfo info() {
    return info;
  }

  public List<CHColumnWithTypeAndName> columns() {
    return columns;
  }

  public int rowCount() {
    for (CHColumnWithTypeAndName c : columns) {
      return c.column().size();
    }
    return 0;
  }

  public int colCount() {
    return columns.size();
  }

  public boolean isEmpty() {
    return columns.isEmpty();
  }

  /** Estimated bytes this block needs to store its data. */
  public long byteCount() {
    long count = 0;
    for (CHColumnWithTypeAndName c : columns) {
      count += c.column().byteCount();
    }
    return count;
  }

  /** Free the memory it takes. The GC will also do this job if you forget. */
  public void free() {
    for (CHColumnWithTypeAndName c : columns) {
      c.column().free();
    }
  }
}
