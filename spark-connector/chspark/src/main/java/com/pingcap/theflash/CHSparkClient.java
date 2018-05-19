package com.pingcap.theflash;

import com.pingcap.ch.CHBlock;
import com.pingcap.ch.CHConnection;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.TypeMapping;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public class CHSparkClient implements Closeable, Iterator<CHBlock> {
    private String queryId;
    private String query;
    private int clientCount;
    private int clientIndex;

    private CHConnection conn;
    private CHBlock chSchema;
    private StructType sparkSchema;

    private CHBlock curBlock;

    private boolean closed = false;
    private boolean readAll = false;

    public CHSparkClient(String queryId, String query, String host, int port, int clientCount, int clientIndex) {
        this.conn = new CHConnection(host, port, "", "default", "", "CHSpark");
        this.queryId = queryId;
        this.query = query;
        this.clientCount = clientCount;
        this.clientIndex = clientIndex;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;

        IOUtils.closeQuietly(conn);
        conn = null;
        chSchema = null;
        sparkSchema = null;
    }

    public void sendQueryIfNot() throws IOException {
        if (chSchema == null) {
            conn.sendSharedQuery(query, queryId, clientCount);
            CHConnection.Packet p = conn.receivePacket();
            if (p.isEndOfStream() || p.block == null || p.block.isEmpty()) {
                throw new IOException("Read schema failed" +
                        ((p.exceptionMsg == null) ? "" : ", reason: " + p.exceptionMsg));
            }
            chSchema = p.block;
            sparkSchema = TypeMapping.chSchemaToSparkSchema(chSchema);
        }
    }

    public CHBlock chSchema() throws IOException {
        if (closed) {
            return null;
        }
        sendQueryIfNot();
        return chSchema;
    }

    public StructType sparkSchema() throws IOException {
        if (closed) {
            return null;
        }
        sendQueryIfNot();
        return sparkSchema;
    }

    @Override
    public boolean hasNext() {
        String exception = null;
        try {
            if (closed || readAll) {
                return false;
            }

            sendQueryIfNot();

            // Free previous block if exists.
            if (curBlock != null) {
                curBlock.free();
                curBlock = null;
            }

            CHConnection.Packet p = receive();
            curBlock = p.block;
            exception = p.exceptionMsg;
        } catch (Exception e) {
            // We need to close the connection after bad things happened from here.
            // As spark won't do it.
            IOUtils.closeQuietly(this);
            throw new RuntimeException("Exception when fetching next block", e);
        }
        if (exception != null) {
            IOUtils.closeQuietly(this);
            throw new RuntimeException(exception);
        }
        return curBlock != null;
    }

    private CHConnection.Packet receive() throws IOException {
        CHConnection.Packet p = conn.receivePacket();
        if (p.exceptionMsg != null) {
            return p;
        }
        if (p.isEndOfStream()) {
            // We are done.
            readAll = true;
            close();
            return p;
        }
        // ClickHouse sends an empty block after process done.
        // We need to consume all data in network stream to reduce "Connection reset by peer" at ClickHouse side.
        if (p.block == null || p.block.isEmpty()) {
            p = receive();
        }

        return p;
    }

    @Override
    public CHBlock next() {
        return curBlock;
    }

    @Override
    protected void finalize() throws Throwable {
        // Good idea or not?
        IOUtils.closeQuietly(this);
    }
}
