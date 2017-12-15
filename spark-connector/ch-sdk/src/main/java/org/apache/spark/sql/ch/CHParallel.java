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

import java.net.Socket;

import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.lang.InterruptedException;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.VectorSchemaRoot;


// TODO: Rpc retry
public class CHParallel {
    public CHParallel(String query, String host, int port, int threads)
        throws IOException, CHExecutor.CHExecutorException {

        this.executor = new CHExecutor(query, host, port);
        this.query = query;
        this.finished = false;

        this.decodeds = new LinkedBlockingQueue(48);
        this.decodings = new LinkedBlockingQueue(48);

        startFetch();
        startDecode(threads);
    }

    public void close() throws IOException {
        executor.close();
    }

    public Schema getSchema() {
        return executor.getSchema();
    }

    public boolean hasNext() {
        synchronized(this) {
            return !finished;
        }
    }

    public CHExecutor.Result next() throws InterruptedException, CHExecutor.CHExecutorException {
        CHExecutor.Result decoded = decodeds.take();
        if (decoded.isEmpty()) {
            synchronized(this) {
                finished = true;
            }
            return null;
        } else if (decoded.error != null) {
            synchronized(this) {
                finished = true;
            }
            throw new CHExecutor.CHExecutorException(decoded.error);
        }
        return decoded;
    }

    private void startFetch() {
        Thread worker = new Thread(new Runnable() {
            public void run() {
                try {
                    while (executor.hasNext()) {
                        decodings.put(executor.safeNext());
                    }
                } catch (InterruptedException e) {
                }
            }});
        worker.start();
    }

    private void startDecode(int threads) {
        // TODO: Multi threads
        // TODO: Reorder blocks maybe faster, in some cases
        Thread worker = new Thread(new Runnable() {
            public void run() {
                try {
                    while (true) {
                        CHExecutor.Package decoding = decodings.take();
                        CHExecutor.Result decoded = executor.safeDecode(decoding);
                        decodeds.put(decoded);
                        if (decoded.isLast()) {
                            break;
                        }
                    }
                } catch (InterruptedException e) {
                }
            }});
        worker.start();
    }

    public final String query;
    private CHExecutor executor;
    private boolean finished;
    private BlockingQueue<CHExecutor.Package> decodings;
    private BlockingQueue<CHExecutor.Result> decodeds;
}
