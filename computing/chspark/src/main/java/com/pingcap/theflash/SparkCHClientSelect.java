package com.pingcap.theflash;

import com.pingcap.ch.CHBlock;
import com.pingcap.ch.CHConnection;
import com.pingcap.ch.CHProtocol;
import com.pingcap.theflash.codegen.CHColumnBatch;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.ch.CHUtil;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.TypeMapping;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Select API for spark to send select query to CH.
 *
 * Not multi-thread safe.
 */
public class SparkCHClientSelect implements Closeable, Iterator<CHColumnBatch> {
    private String queryId;
    private String query;
    private int clientCount;
    private int clientIndex;
    private boolean sharedMode;

    private CHConnection conn;
    private CHBlock chSchema;
    private StructType sparkSchema;

    private CHBlock curBlock;

    private AtomicBoolean closed = new AtomicBoolean(false);

    public SparkCHClientSelect(String queryId, String query, String host, int port) {
        this(queryId, query, host, port, 0, 0, false);
    }

    public SparkCHClientSelect(String query, String host, int port) {
        this(CHUtil.genQueryId("G"), query, host, port, 0, 0, false);
    }

    public SparkCHClientSelect(String queryId, String query, String host, int port, int clientCount, int clientIndex) {
        this(queryId, query, host, port, clientCount, clientIndex, true);
    }

    public SparkCHClientSelect(String queryId, String query, String host, int port, int clientCount, int clientIndex, boolean sharedMode) {
        this.conn = new CHConnection(host, port, "", "default", "", "CHSpark");
        this.queryId = queryId;
        this.query = query;
        this.clientCount = clientCount;
        this.clientIndex = clientIndex;
        this.sharedMode = sharedMode;
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true))
            return;

        IOUtils.closeQuietly(conn);
        conn = null;
        chSchema = null;
        sparkSchema = null;
    }

    public void sendQueryIfNot() throws IOException {
        if (chSchema == null) {
            if (sharedMode) {
                conn.sendSharedQuery(query, queryId, clientCount);
            } else {
                conn.sendQuery(query, queryId, Collections.emptyList());
            }
            CHConnection.Packet p = conn.receivePacket();
            if (p.isEndOfStream()) {
                // No schema return. We are done.
                close();
                return;
            }
            if (p.block == null || p.block.isEmpty()) {
                throw new IOException("Read schema failed" +
                        ((p.exceptionMsg == null) ? "" : ", reason: " + p.exceptionMsg));
            }
            chSchema = p.block;
            sparkSchema = TypeMapping.chSchemaToSparkSchema(chSchema);
        }
    }

    public CHBlock chSchema() throws IOException {
        if (closed.get()) {
            return null;
        }
        sendQueryIfNot();
        return chSchema;
    }

    public StructType sparkSchema() throws IOException {
        if (closed.get()) {
            return null;
        }
        sendQueryIfNot();
        return sparkSchema;
    }

    @Override
    public boolean hasNext() {
        String exception = null;
        try {
            if (closed.get()) {
                return false;
            }

            sendQueryIfNot();

            // Free previous block if exists.
            if (curBlock != null) {
                curBlock.free();
                curBlock = null;
            }

            if (conn == null) {
                return false;
            }

            CHConnection.Packet p = receiveValidPacket();
            curBlock = p.block;
            exception = p.exceptionMsg;
        } catch (Exception e) {
            // We need to close the connection after bad things happened from here.
            // As spark won't do it.
            IOUtils.closeQuietly(this);
            throw new RuntimeException("Exception when fetching next block. SQL: " + query, e);
        }
        if (exception != null) {
            IOUtils.closeQuietly(this);
            throw new RuntimeException(exception);
        }
        return curBlock != null;
    }

    private CHConnection.Packet receiveValidPacket() throws IOException {
        while (true) {
            CHConnection.Packet p = conn.receivePacket();
            switch (p.type) {
                case CHProtocol.Server.Data:
                    // Empty block means end of stream and should be ignored.
                    if (p.block.isEmpty()) {
                        continue;
                    } else {
                        return p;
                    }
                case CHProtocol.Server.Exception:
                    return p;
                case CHProtocol.Server.EndOfStream:
                    // We are done.
                    close();
                    return p;
                case CHProtocol.Server.Progress:
                case CHProtocol.Server.ProfileInfo:
                case CHProtocol.Server.Totals:
                case CHProtocol.Server.Extremes:
                    // Ignore those messages and wait for next block.
                    continue;
                default:
                    throw new IllegalStateException("Should not reach here!");
            }
        }
    }

    @Override
    public CHColumnBatch next() {
        return new CHColumnBatch(curBlock, sparkSchema);
    }
}
