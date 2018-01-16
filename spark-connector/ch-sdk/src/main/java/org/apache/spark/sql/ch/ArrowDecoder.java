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

package org.apache.spark.sql.ch;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;


public class ArrowDecoder {
    // private final static long MB_IN_BYTES = 1024 * 1024;
    // private final static long MAX_ALLOC = MB_IN_BYTES * 1024 * 4;
    // private final static RootAllocator rootAllocator = new RootAllocator(MAX_ALLOC);
    // private final static AtomicInteger allocId = new AtomicInteger(0);

    public ArrowDecoder() {
        // alloc = rootAllocator.newChildAllocator(
        //    "ChildAlloc" + allocId.incrementAndGet(),
        //    0,
        //    MB_IN_BYTES * 64);
        alloc = new RootAllocator(Integer.MAX_VALUE);
    }

    public Schema decodeSchema(byte[] data) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ReadChannel channel = new ReadChannel(Channels.newChannel(in));
        return MessageSerializer.deserializeSchema(channel);
    }

    public VectorSchemaRoot decodeBlock(Schema schema, byte[] data) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ReadChannel channel = new ReadChannel(Channels.newChannel(in));
        ArrowRecordBatch batch = (ArrowRecordBatch)MessageSerializer.deserializeMessageBatch(channel, alloc);

        VectorSchemaRoot block = VectorSchemaRoot.create(schema, alloc);
        VectorLoader loader = new VectorLoader(block);
        loader.load(batch);

        channel.close();
        in.close();
        batch.close();

        return block;
    }

    public void close() {
        alloc.close();
    }

    private final BufferAllocator alloc;
}
