package com.pingcap.ch.columns;

import com.pingcap.common.MemoryUtil;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.spark.unsafe.types.UTF8String;
import shade.com.google.common.base.Preconditions;

public class UTF8ChunkBuilder {

  private UTF8String[] strs;
  private int size;
  private int lastOffset;

  static class SealedChunk {

    private ByteBuffer offsetBuf;
    private ByteBuffer charBuf;

    private long offsetsAddr;
    private long charsAddr;

    private SealedChunk(ByteBuffer charBuf, ByteBuffer offsetBuf) {
      Preconditions.checkNotNull(charBuf, "charBuf is null");
      Preconditions.checkNotNull(offsetBuf, "offsetBuf is null");
      this.charBuf = charBuf;
      this.offsetBuf = offsetBuf;
      this.offsetsAddr = MemoryUtil.getAddress(offsetBuf);
      this.charsAddr = MemoryUtil.getAddress(charBuf);
    }

    public int size() {
      return this.offsetBufSize() / 8;
    }

    public int offsetBufSize() {
      return offsetBuf.capacity();
    }

    public int charBufSize() {
      return charBuf.capacity();
    }

    public ByteBuffer getOffsetBuf() {
      return offsetBuf;
    }

    public ByteBuffer getCharBuf() {
      return charBuf;
    }

    public void free() {
      if (offsetsAddr != 0) {
        MemoryUtil.free(offsetBuf);
        offsetsAddr = 0;
      }
      if (charsAddr != 0) {
        MemoryUtil.free(charBuf);
        charsAddr = 0;
      }
    }
  }

  public UTF8ChunkBuilder(int maxSize) {
    this.strs = new UTF8String[maxSize];
    this.lastOffset = 0;
    this.size = 0;
  }

  public boolean isFull() {
    return size >= strs.length;
  }

  public void append(UTF8String str) {
    Preconditions.checkNotNull(str, "str is null");
    if (size >= strs.length) {
      throw new IllegalStateException("UTF8StringChunk is full");
    }
    strs[size] = str.clone();
    size++;
  }

  public boolean isEmpty() {
    return size == 0;
  }

  public SealedChunk seal() {
    long totalCharBufSize = 0;
    for (int i = 0; i < size; i++) {
      totalCharBufSize += strs[i].numBytes() + 1;
    }
    if (totalCharBufSize >= Integer.MAX_VALUE) {
      throw new IllegalStateException("String total length overflow!");
    }

    ByteBuffer offsets = MemoryUtil.allocateDirect(size * 8);
    ByteBuffer chars = MemoryUtil.allocateDirect((int) totalCharBufSize);
    long charsAddr = MemoryUtil.getAddress(chars);

    long curOffset = 0;
    for (int i = 0; i < size; i++) {
      UTF8String s = strs[i];
      offsets.putLong(lastOffset + curOffset + s.numBytes() + 1);
      MemoryUtil.copyMemory(
          s.getBaseObject(), s.getBaseOffset(), null, charsAddr + curOffset, s.numBytes());
      curOffset += s.numBytes() + 1;
    }

    Preconditions.checkState(curOffset == chars.capacity());

    lastOffset += chars.capacity();
    offsets.clear();
    chars.clear();
    size = 0;
    strs = new UTF8String[strs.length];

    return new SealedChunk(chars, offsets);
  }

  public static SealedChunk mergeChunk(List<SealedChunk> chunks) {
    long totalCharBufLen = 0;
    long totalOffsetBufLen = 0;
    for (SealedChunk chunk : chunks) {
      totalCharBufLen += chunk.charBufSize();
      totalOffsetBufLen += chunk.offsetBufSize();
    }
    if (totalCharBufLen > Integer.MAX_VALUE || totalOffsetBufLen > Integer.MAX_VALUE) {
      throw new IllegalStateException("String chunk buffer overflow");
    }

    ByteBuffer offsetBuf = MemoryUtil.allocateDirect((int) totalOffsetBufLen);
    ByteBuffer charBuf = MemoryUtil.allocateDirect((int) totalCharBufLen);
    long charBufAddr = MemoryUtil.getAddress(charBuf);
    long offsetBufAddr = MemoryUtil.getAddress(offsetBuf);
    for (SealedChunk chunk : chunks) {
      MemoryUtil.copyMemory(chunk.charsAddr, charBufAddr, chunk.charBufSize());
      MemoryUtil.copyMemory(chunk.offsetsAddr, offsetBufAddr, chunk.offsetBufSize());
      chunk.free();
      charBufAddr += chunk.charBufSize();
      offsetBufAddr += chunk.offsetBufSize();
    }
    return new SealedChunk(charBuf, offsetBuf);
  }
}
