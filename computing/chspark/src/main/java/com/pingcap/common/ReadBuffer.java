package com.pingcap.common;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import org.apache.commons.io.IOUtils;

/** Used to avoid frequectly call underlying channel, which means system call and could be slow. */
public class ReadBuffer implements Closeable {
  private ReadableByteChannel channel;
  private ByteBuffer buf;
  private long bufAddr;

  public ReadBuffer(ReadableByteChannel channel, int bufSize) {
    bufSize = Math.max(bufSize, 512);
    this.channel = channel;
    this.buf = MemoryUtil.allocateDirect(bufSize);
    this.bufAddr = MemoryUtil.getAddress(buf);

    // We are empty now.
    buf.flip();
  }

  @Override
  public void close() throws IOException {
    if (channel != null) {
      IOUtils.closeQuietly(channel);
      MemoryUtil.free(buf);

      channel = null;
      buf = null;
      bufAddr = 0;
    }
  }

  public int bufferSize() {
    return buf.capacity();
  }

  public void fill() throws IOException {
    fill(0);
  }

  public void fill(int require) throws IOException {
    if (require > buf.capacity()) {
      throw new IllegalArgumentException();
    }

    int pos = buf.position();
    int remain = buf.remaining();
    if (remain == buf.capacity()) {
      // Full.
      return;
    }
    if (remain > 0 && pos != 0) {
      // Move data to the begining.
      MemoryUtil.copyMemory(bufAddr + pos, bufAddr, remain);
      pos = 0;
    }

    buf.position(remain);
    buf.limit(buf.capacity());

    require -= remain;
    do {
      int rt = channel.read(buf);
      require -= rt;
      if (rt < 0 && require > 0) {
        throw new IOException("End of stream.");
      }
    } while (require > 0);

    buf.flip();
  }

  public void ensure(int require) throws IOException {
    if (require > buf.remaining()) {
      fill(require);
    }
  }

  /** Read fully */
  public void read(ByteBuffer dst) throws IOException {
    assert dst.isDirect();

    int bufPos = buf.position();
    int bufRemain = buf.remaining();

    int dstRequire = dst.remaining();
    int dstPos = dst.position();
    long disAddr = MemoryUtil.getAddress(dst);

    int toCopy = Math.min(bufRemain, dstRequire);
    MemoryUtil.copyMemory(bufAddr + bufPos, disAddr + dstPos, toCopy);

    buf.position(bufPos + toCopy);
    dst.position(dstPos + toCopy);

    if (!dst.hasRemaining()) {
      return;
    }

    // Read directly from underlying stream.
    IOUtil.readFully(channel, dst);
  }

  public byte readByte() throws IOException {
    ensure(1);
    return buf.get();
  }

  public int readShort() throws IOException {
    ensure(2);
    return buf.getShort();
  }

  public int readInt() throws IOException {
    ensure(4);
    return buf.getInt();
  }

  public long readLong() throws IOException {
    ensure(8);
    return buf.getLong();
  }

  public float readFloat() throws IOException {
    ensure(4);
    return buf.getFloat();
  }

  public double readDouble() throws IOException {
    ensure(8);
    return buf.getDouble();
  }

  // Convenient methods.

  public long readVarUInt64() throws IOException {
    return IOUtil.readVarUInt64(this);
  }

  public String readUTF8StrWithVarLen() throws IOException {
    ByteBuffer strBuf = MemoryUtil.allocateDirect(1024);
    String s = readUTF8StrWithVarLen(strBuf);
    MemoryUtil.free(strBuf);
    return s;
  }

  public String readUTF8StrWithVarLen(ByteBuffer strBuf) throws IOException {
    return IOUtil.readUTF8StrWithVarLen(this, strBuf);
  }

  public byte[] readBytesWithVarLen(ByteBuffer strBuf) throws IOException {
    return IOUtil.readBytesWithVarLen(this, strBuf);
  }
}
