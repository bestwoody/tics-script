package com.pingcap.common;

import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class IOUtil {
    public static long DEFAULT_MAX_STRING_SIZE = 0x00FFFFFFL;
    private static final Logger logger = LoggerFactory.getLogger(IOUtil.class);

    public static void readFully(ReadableByteChannel src, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int rt = src.read(buffer);
            if (rt < 0) {
                throw new IOException("End of stream.");
            }
        }
    }

    public static void writeFully(WritableByteChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int rt = channel.write(buffer);
            if (rt < 0) {
                throw new IOException("Failed to write.");
            }
        }
    }

    public static int readVarUInt32(ByteBuffer in) {
        int value = 0;
        int i = 0;
        int b;
        while (((b = in.get() & 0xFF) & 0x80) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
        }
        return value | (b << i);
    }

    public static void writeVarUInt32(int value, ByteBuffer dest) {
        while ((value & 0xFFFFFF80) != 0L) {
            dest.put((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        dest.put((byte) (value & 0x7F));
    }

    public static void writeVarUInt64(long value, ByteBuffer out) {
        while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
            out.put((byte) ((int) ((value & 0x7F) | 0x80)));
            value >>>= 7;
        }
        out.put((byte) (value & 0x7F));
    }

    public static void writeVarUInt64(long value, WriteBuffer writer) throws IOException {
        while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
            writer.writeByte((byte) ((int) ((value & 0x7F) | 0x80)));
            value >>>= 7;
        }
        writer.writeByte((byte) (value & 0x7F));
    }

    public static long readVarUInt64(ByteBuffer in) {
        long value = 0;
        int i = 0;
        long b;
        while (((b = in.get() & 0xFF) & 0x80) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
        }
        return value | (b << i);
    }

    private static byte readOneByte(ReadableByteChannel in, ByteBuffer buffer) throws IOException {
        buffer.clear();
        buffer.limit(1);
        int rt;
        while ((rt = in.read(buffer)) <= 0) {
            if (rt < 0) {
                throw new IOException("End of stream.");
            }
        }
        buffer.flip();
        return buffer.get();
    }

    public static long readVarUInt64(ReadableByteChannel in, ByteBuffer oneByteBuf) throws IOException {
        long value = 0;
        int i = 0;
        long b;
        while (((b = readOneByte(in, oneByteBuf) & 0xFF) & 0x80) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
        }
        return value | (b << i);
    }

    public static long readVarUInt64(ReadBuffer reader) throws IOException {
        long value = 0;
        int i = 0;
        long b;
        while (((b = reader.readByte() & 0xFF) & 0x80) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
        }
        return value | (b << i);
    }

    public static void writeUTF8StrWithVarLen(String string, ByteBuffer buf) {
        try {
            byte[] bytes = string.getBytes("UTF-8");
            writeVarUInt64(bytes.length, buf);
            buf.put(bytes);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static void writeUTF8StrWithVarLen(String string, WriteBuffer writer, ByteBuffer strBuf_) throws IOException {
        try {
            byte[] bytes = string.getBytes("UTF-8");

            ByteBuffer strBuf = bytes.length > strBuf_.capacity() ? MemoryUtil.allocateDirect(bytes.length) : strBuf_;
            strBuf.clear();
            strBuf.put(bytes);
            strBuf.flip();

            writer.writeVarUInt64(bytes.length);
            writer.write(strBuf);

            if (strBuf != strBuf_) {
                MemoryUtil.free(strBuf);
            }

        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static String readUTF8StrWithVarLen(ReadableByteChannel in, ByteBuffer oneByteBuf, ByteBuffer strBuf_) throws IOException {
        long len = readVarUInt64(in, oneByteBuf);
        if (len > DEFAULT_MAX_STRING_SIZE) {
            throw new IOException("Too large string size.");
        }
        int strLen = (int) len;
        ByteBuffer strBuf = len > strBuf_.capacity() ? MemoryUtil.allocateDirect(strLen) : strBuf_;
        strBuf.clear();
        strBuf.limit(strLen);
        readFully(in, strBuf);
        String s = UTF8String.fromAddress(null, MemoryUtil.getAddress(strBuf), strLen).toString();
        if (strBuf != strBuf_) {
            MemoryUtil.free(strBuf);
        }
        return s;
    }

    public static String readUTF8StrWithVarLen(ReadBuffer reader, ByteBuffer strBuf_) throws IOException {
        long len = readVarUInt64(reader);
        if (len > DEFAULT_MAX_STRING_SIZE) {
            throw new IOException("Too large string size.");
        }
        int strLen = (int) len;
        ByteBuffer strBuf = len > strBuf_.capacity() ? MemoryUtil.allocateDirect(strLen) : strBuf_;
        strBuf.clear();
        strBuf.limit(strLen);
        reader.read(strBuf);
        String s = UTF8String.fromAddress(null, MemoryUtil.getAddress(strBuf), strLen).toString();
        if (strBuf != strBuf_) {
            MemoryUtil.free(strBuf);
        }
        return s;
    }

    public static String readUTF8String(ReadableByteChannel in, ByteBuffer buf) throws IOException {
        buf.clear();
        readFully(in, buf);
        buf.flip();
        int len = buf.getInt();

        buf.clear();
        buf.limit(len);
        readFully(in, buf);
        return UTF8String.fromAddress(null, MemoryUtil.getAddress(buf), len).toString();
    }

    public static boolean readBool(ReadableByteChannel in, ByteBuffer oneByteBuf) throws IOException {
        oneByteBuf.clear();
        readFully(in, oneByteBuf);
        oneByteBuf.clear();
        return oneByteBuf.get() != 0;
    }

    public static void closeQuietly(Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException e) {
            logger.warn("Quietly close exception.", e);
        }
    }
}
