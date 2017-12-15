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

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.VectorSchemaRoot;


public class CHExecutor {
    public static class CHExecutorException extends Exception {
        public CHExecutorException(String msg) {
            super(msg);
        }

        public CHExecutorException(Exception ex) {
            super(ex);
        }
    }

    public static class Package {
        Package(long type, byte[] data) {
            this.type = type;
            this.data = data;
        }

        private long type;
        private byte[] data;
    }

    public static class Result {
        Result(VectorSchemaRoot block) {
            this.error = null;
            this.block = block;
        }
        Result(Exception ex) {
            this.error = ex;
            this.block = null;
        }
        Result() {
            this.error = null;
            this.block = null;
        }
        boolean isEmpty() {
            return error == null && block == null;
        }
        boolean isLast() {
            return error != null || block == null;
        }
        void close() {
            if (block != null) {
                block.close();
                block = null;
            }
        }

        public final Exception error;
        public VectorSchemaRoot block;
    }

    public CHExecutor(String query, String host, int port) throws IOException, CHExecutorException {
        this.arrowDecoder = new ArrowDecoder();
        this.query = query;
        this.socket = new Socket(host, port);
        this.writer = new DataOutputStream(socket.getOutputStream());
        this.reader = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        this.finished = false;

        sendQuery(query);
        fetchSchema();
    }

    public void close() throws IOException {
        arrowDecoder.close();
        socket.close();
    }

    public Schema getSchema() {
        return schema;
    }

    public boolean hasNext() {
        return !finished;
    }

    public Package safeNext() {
        try {
            return next();
        } catch (Exception e) {
            finished = true;
            return new Package(PackageTypeUtf8Error, e.toString().getBytes());
        }
    }

    public Package next() throws IOException, CHExecutorException {
        long type = reader.readLong();
        if (type == PackageTypeEnd) {
            finished = true;
            return new Package(type, null);
        }
        byte[] data = null;
        long size = reader.readLong();
        if (size >= Integer.MAX_VALUE) {
            throw new CHExecutorException("Package too big, size: " + size);
        }
        if (size > 0) {
            data = new byte[(int)size];
            reader.readFully(data);
        }
        finished = (type == PackageTypeUtf8Error);
        return new Package(type, data);
    }

    public Result safeDecode(Package decoding) {
        try {
            return decode(decoding);
        } catch (Exception e) {
            return new Result(e);
        }
    }

    public Result decode(Package decoding) throws IOException {
        if (decoding.type == PackageTypeUtf8Error) {
            return new Result(new IOException(new String(decoding.data)));
        } else if (decoding.type == PackageTypeArrowData) {
            ArrowDecoder arrowDecoder = new ArrowDecoder();
            return new Result(arrowDecoder.decodeBlock(schema, decoding.data));
        } else if (decoding.type == PackageTypeEnd) {
            return new Result();
        } else {
            return new Result(new IOException("Unknown package, type: " + decoding.type));
        }
    }

    private void sendQuery(String query) throws IOException {
        long val = PackageTypeUtf8Query;
        writer.writeLong(val);
        val = query.length();
        writer.writeLong(val);
        writer.writeBytes(query);
        writer.flush();
    }

    private void fetchSchema() throws IOException, CHExecutorException {
        long type = reader.readLong();
        long size = reader.readLong();
        if (size >= Integer.MAX_VALUE) {
            throw new CHExecutorException("Package too big, size: " + size);
        }

        byte[] data = new byte[(int)size];
        reader.readFully(data);
        if (type == PackageTypeUtf8Error) {
            throw new CHExecutorException("Error from storage: " + new String(data));
        }
        if (type != PackageTypeArrowSchema) {
            throw new CHExecutorException("Received package, but not schema, type: " + type);
        }

        schema = arrowDecoder.decodeSchema(data);
    }

    public final String query;
    private ArrowDecoder arrowDecoder;
    private Socket socket;
    private DataOutputStream writer;
    private DataInputStream reader;
    private Schema schema;
    private boolean finished;

    private static final long PackageTypeEnd = 0;
    private static final long PackageTypeUtf8Error = 1;
    private static final long PackageTypeUtf8Query = 2;
    private static final long PackageTypeArrowSchema = 3;
    private static final long PackageTypeArrowData = 4;
}
