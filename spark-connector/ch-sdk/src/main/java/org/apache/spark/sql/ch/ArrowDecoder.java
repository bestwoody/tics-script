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

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.file.ReadChannel;
import org.apache.arrow.vector.stream.MessageSerializer;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorLoader;


public class ArrowDecoder {
    private final static RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    private final static AtomicInteger allocId = new AtomicInteger(0);

    public ArrowDecoder() {
        alloc = rootAllocator.newChildAllocator(
            "ChildAlloc" + allocId.incrementAndGet(),
            0,
            Long.MAX_VALUE);
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
