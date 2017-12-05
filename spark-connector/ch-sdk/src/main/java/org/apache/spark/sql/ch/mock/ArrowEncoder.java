/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.ch.mock;

import static java.util.Arrays.asList;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.Iterator;
import java.util.List;

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


public class ArrowEncoder {
    public static ArrowBuf newBuf(BufferAllocator alloc, byte[] bytes) throws IOException {
        ArrowBuf buffer = alloc.buffer(bytes.length);
        buffer.writeBytes(bytes);
        return buffer;
    }

    public static byte[] array(ArrowBuf buf) throws IOException {
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        return bytes;
    }

    public static ByteArrayOutputStream encode(byte[] values) throws IOException {
        byte[] validity = new byte[] {(byte) 255, 0};
        BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        ArrowBuf validityb = newBuf(alloc, validity);
        ArrowBuf valuesb = newBuf(alloc, values);
        ArrowRecordBatch batch = new ArrowRecordBatch(
            16, asList(new ArrowFieldNode(16, 8)), asList(validityb, valuesb));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), batch);
        return out;
    }

    public static void encodeSchema(byte[] data, Schema schema) throws IOException{
        BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        byte[] validity = new byte[] {(byte) 255, 0};
        ArrowBuf validityb = newBuf(alloc, validity);
        ArrowBuf valuesb = newBuf(alloc, data);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long serialize = MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), schema);
    }

    public static byte[] decode(byte[] data) throws IOException {
        BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ReadChannel channel = new ReadChannel(Channels.newChannel(in));
        ArrowMessage deserialized = MessageSerializer.deserializeMessageBatch(channel, alloc);
        ArrowRecordBatch encode_batch = (ArrowRecordBatch)deserialized;
        List<ArrowBuf> buffers = encode_batch.getBuffers();
        byte[] bytes = array(buffers.get(1));
        return bytes;
    }

    public static byte[] recordBatch() throws IOException {
        byte[] values = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
        float[] floatType = new float[] {12.3f, 24.3f, 34.5f};
        double[] douleType = new double[] {13.34, 25.6, 23.5};
        String[] stringTypr = new String[] {"SDSD", "DSDSV", "DSCFSD"};

        byte[] floatBytes = ByteUtil.getBytes(floatType[0]);
        byte[] doubleBytes = ByteUtil.getBytes(douleType[0]);
        ByteArrayOutputStream out = encode(doubleBytes);
        byte[] decoded = decode(out.toByteArray());
        return decoded;
    }
}
