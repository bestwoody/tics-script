package com.pingcap.common;

import java.io.IOException;
import java.nio.ByteBuffer;

public class AutoGrowByteBuffer {
  private ByteBuffer initBuf;
  private ByteBuffer buf;

  public AutoGrowByteBuffer(ByteBuffer initBuf) {
    initBuf.clear();
    this.initBuf = initBuf;
    this.buf = initBuf;
  }

  public int dataSize() {
    return buf.position();
  }

  public ByteBuffer getByteBuffer() {
    return buf;
  }

  private void beforeIncrease(int inc) {
    int minCap = buf.position() + inc;
    if (minCap > buf.capacity()) {
      int newCap = buf.capacity();
      do {
        newCap = newCap << 1;
      } while (minCap > newCap);

      ByteBuffer newBuf = MemoryUtil.allocateDirect(newCap);
      MemoryUtil.copyMemory(
          MemoryUtil.getAddress(buf), MemoryUtil.getAddress(newBuf), buf.position());
      newBuf.position(buf.position());

      if (buf != initBuf) {
        MemoryUtil.free(buf);
      }

      buf = newBuf;
    }
  }

  public void put(ReadBuffer reader, int len) throws IOException {
    beforeIncrease(len);

    buf.limit(buf.position() + len);
    reader.read(buf);
  }

  public void putByte(byte v) {
    beforeIncrease(1);

    buf.limit(buf.position() + 1);
    buf.put(v);
  }
}
