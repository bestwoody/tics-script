package com.pingcap.common;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import org.apache.commons.io.IOUtils;

/** Used to avoid frequectly call underlying channel, which means system call and could be slow. */
public class WriteBuffer implements Closeable {
  private WritableByteChannel channel;
  private ByteBuffer buf;
  private long bufAddr;

  public WriteBuffer(WritableByteChannel channel, int bufSize) {
    this.channel = channel;
    this.buf = MemoryUtil.allocateDirect(bufSize);
    this.bufAddr = MemoryUtil.getAddress(buf);
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

  public void flush() throws IOException {
    buf.flip();
    if (!buf.hasRemaining()) {
      buf.clear();
      return;
    }
    IOUtil.writeFully(channel, buf);
    buf.clear();
  }

  public void ensure(int require) throws IOException {
    if (require > buf.remaining()) {
      flush();
    }
  }

  /** Write fully. */
  public void write(ByteBuffer src) throws IOException {
    assert src.isDirect();

    int bufPos = buf.position();
    int bufRemain = buf.remaining();

    int srcRequire = src.remaining();
    int srcPos = src.position();
    long srcAddr = MemoryUtil.getAddress(src);

    int toCopy = Math.min(bufRemain, srcRequire);
    MemoryUtil.copyMemory(srcAddr + srcPos, bufAddr + bufPos, toCopy);

    buf.position(bufPos + toCopy);
    src.position(srcPos + toCopy);

    if (!src.hasRemaining()) {
      return;
    }

    flush();
    // Write directly to underlying stream.
    IOUtil.writeFully(channel, src);
  }

  public void writeByte(byte v) throws IOException {
    ensure(1);
    buf.put(v);
  }

  public void writeShort(short v) throws IOException {
    ensure(2);
    buf.putShort(v);
  }

  public void writeInt(int v) throws IOException {
    ensure(4);
    buf.putInt(v);
  }

  public void writeLong(long v) throws IOException {
    ensure(8);
    buf.putLong(v);
  }

  public void writeFloat(float v) throws IOException {
    ensure(4);
    buf.putFloat(v);
  }

  public void writeDouble(double v) throws IOException {
    ensure(8);
    buf.putDouble(v);
  }

  // Convenient methods.

  public void writeVarInt64(long value) throws IOException {
    IOUtil.writeVarInt64(value, this);
  }

  public void writeVarUInt64(long value) throws IOException {
    IOUtil.writeVarUInt64(value, this);
  }

  public void writeUTF8StrWithVarLen(String string) throws IOException {
    ByteBuffer strBuf = MemoryUtil.allocateDirect(1024);
    writeUTF8StrWithVarLen(string, strBuf);
    MemoryUtil.free(strBuf);
  }

  public void writeUTF8StrWithVarLen(String string, ByteBuffer strBuf) throws IOException {
    IOUtil.writeUTF8StrWithVarLen(string, this, strBuf);
  }
}
