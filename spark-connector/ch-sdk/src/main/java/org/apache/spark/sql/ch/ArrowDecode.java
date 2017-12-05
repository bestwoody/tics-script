package org.apache.spark.sql.ch;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.file.ReadChannel;
import org.apache.arrow.vector.file.WriteChannel;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.schema.ArrowMessage;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.stream.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.Iterator;
import java.util.List;

import static java.util.Arrays.asList;


public class ArrowDecode {
    public static ArrowBuf buf(BufferAllocator alloc, byte[] bytes) throws IOException {
        ArrowBuf buffer = alloc.buffer(bytes.length);
        buffer.writeBytes(bytes);
        return buffer;
    }

    public static byte[] array(ArrowBuf buf) throws IOException {
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        return bytes;
    }

    // Serializer
    public static ByteArrayOutputStream encode(byte[] values) throws IOException {
        byte[] validity = new byte[] {(byte) 255, 0};
        BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        ArrowBuf validityb = buf(alloc, validity);
        ArrowBuf valuesb = buf(alloc, values);
        ArrowRecordBatch batch = new ArrowRecordBatch(
                16, asList(new ArrowFieldNode(16, 8)), asList(validityb, valuesb));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), batch);
        return out;
    }

    // schema serializer encode
    public static void schemaEncode(byte[] data, Schema schema) throws IOException{
        BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        byte[] validity = new byte[] {(byte) 255, 0};
        ArrowBuf validityb = buf(alloc, validity);
        ArrowBuf valuesb = buf(alloc, data);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long serialize = MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), schema);
    }


    // deserialize
    public static byte[] decode(ByteArrayOutputStream out) throws IOException {
        BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        ReadChannel channel = new ReadChannel(Channels.newChannel(in));
        ArrowMessage deserialized = MessageSerializer.deserializeMessageBatch(channel, alloc);
        ArrowRecordBatch encode_batch = (ArrowRecordBatch)deserialized;
        List<ArrowBuf> buffers = encode_batch.getBuffers();
        byte[] byt = array(buffers.get(1));
        return byt;
    }
    public static byte[] recordBatch() throws IOException {
        byte[] values = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
        float[] floatType = new float[]{12.3f,24.3f,34.5f};
        double[] douleType = new double[]{13.34,25.6,23.5};
        String[] stringTypr = new String[]{"SDSD","DSDSV","DSCFSD"};

        byte[] floatBytes = ByteUtil.getBytes(floatType[0]);
        byte[] doubleBytes = ByteUtil.getBytes(douleType[0]);
        ByteArrayOutputStream out = encode(doubleBytes);
        byte[] decoded = decode(out);
        return decoded;
    }

}
