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
        Package(long type, byte[] data, int id) {
            this.type = type;
            this.data = data;
            this.id = id;
        }
        boolean isLast() {
            return type == PackageTypeEnd || type == PackageTypeUtf8Error;
        }
        long size() {
            if (data == null) {
                return 0;
            } else {
                return data.length;
            }
        }

        private long type;
        private byte[] data;
        public int id;
    }

    public static class Result {
        Result(VectorSchemaRoot block, ArrowDecoder buffer, int id) {
            this.end = false;
            this.error = null;
            this.block = block;
            this.buffer = buffer;
            this.id = id;
        }
        Result(Exception ex) {
            this.end = false;
            this.error = ex;
            this.block = null;
            this.buffer = null;
            this.id = -1;
        }
        Result() {
            this.end = true;
            this.error = null;
            this.block = null;
            this.buffer = null;
            this.id = -1;
        }
        public boolean isEmpty() {
            return error == null && block == null;
        }
        boolean isLast() {
            return error != null || block == null;
        }
        boolean isEnd() {
            return end;
        }
        void close() {
            if (block != null) {
                block.close();
                block = null;
            }
            if (buffer != null) {
                buffer.close();
                buffer = null;
            }
        }

        private final boolean end;
        private ArrowDecoder buffer;

        public final Exception error;
        public VectorSchemaRoot block;
        public final int id;
    }

    public CHExecutor(String qid, String query, String host, int port, int encoders, int clientCount, int clientIndex)
        throws IOException, CHExecutorException {

        this.arrowDecoder = new ArrowDecoder();
        this.qid = qid;
        this.query = query;
        this.encoders = encoders;
        this.clientCount = clientCount;
        this.clientIndex = clientIndex;

        this.socket = new Socket(host, port);
        this.writer = new DataOutputStream(socket.getOutputStream());
        this.reader = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        this.finished = false;
        this.idgen = 0;

        sendHeader();
        sendQuery();
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
            return new Package(PackageTypeUtf8Error, e.toString().getBytes(), -1);
        }
    }

    public Package next() throws IOException, CHExecutorException {
        long type = reader.readLong();
        if (type == PackageTypeEnd) {
            finished = true;
            return new Package(type, null, -1);
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

        int id = -1;
        if (!finished) {
            id = idgen;
            idgen += 1;
        }
        return new Package(type, data, id);
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
            VectorSchemaRoot decoded = arrowDecoder.decodeBlock(schema, decoding.data);
            return new Result(decoded, null, decoding.id);
        } else if (decoding.type == PackageTypeEnd) {
            return new Result();
        } else {
            return new Result(new IOException("Unknown package, type: " + decoding.type));
        }
    }

    private void sendHeader() throws IOException {
        writer.writeLong(PackageTypeHeader);
        writer.writeLong(PROTOCOL_VERSION_MAJOR);
        writer.writeLong(PROTOCOL_VERSION_MINOR);

        // Client name
        sendString("ch-jvm-sdk");
        // Default database
        sendString("");

        // User/password
        sendString("default");
        sendString("");

        // Encoder name/version/concurrent
        sendString("arrow");
        writer.writeLong(PROTOCOL_ENCODER_VERTION);
        writer.writeLong(encoders);
        writer.flush();

        writer.writeLong(clientCount);
        writer.writeLong(clientIndex);
    }

    private void sendQuery() throws IOException {
        writer.writeLong((long)PackageTypeUtf8Query);
        sendString(qid);
        sendString(query);
        writer.flush();
    }

    private void sendString(String str) throws IOException {
        writer.writeLong((long)str.length());
        writer.writeBytes(str);
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

    public final String qid;
    public final String query;
    public final int encoders;
    public final int clientCount;
    public final int clientIndex;

    private Socket socket;
    private DataOutputStream writer;
    private DataInputStream reader;
    private Schema schema;

    private boolean finished;
    private int idgen;

    private ArrowDecoder arrowDecoder;

    private static final long PackageTypeHeader = 0;
    private static final long PackageTypeEnd = 1;
    private static final long PackageTypeUtf8Error = 8;
    private static final long PackageTypeUtf8Query = 9;
    private static final long PackageTypeArrowSchema = 10;
    private static final long PackageTypeArrowData = 11;

    private static final long PROTOCOL_VERSION_MAJOR = 1;
    private static final long PROTOCOL_VERSION_MINOR = 1;
    private static final long PROTOCOL_ENCODER_VERTION = 1;
}
