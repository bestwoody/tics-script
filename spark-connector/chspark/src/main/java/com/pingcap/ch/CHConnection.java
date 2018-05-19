package com.pingcap.ch;

import com.google.common.base.Preconditions;

import com.pingcap.common.MemoryUtil;
import com.pingcap.common.ReadBuffer;
import com.pingcap.common.WriteBuffer;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.List;

/**
 * ClickHouse java connection.
 * You should {@link #close} it to release connection resources after used.
 *
 * Not multi-thread safe.
 */
public class CHConnection implements Closeable {
    private static final long DBMS_VERSION_MAJOR = 1;
    private static final long DBMS_VERSION_MINOR = 1;
    private static final long VERSION_REVISION = 54370;
    private static final long DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE = 54058;

    private static final int READ_BUFFER_SIZE = 87380; // Linux default read buffer size.
    private static final int WRITE_BUFFER_SIZE = 16384; // Linux default write buffer size.

    private static final Logger logger = LoggerFactory.getLogger(CHConnection.class);

    private String host;
    private int port;
    private String defaultDatabase;
    private String user;
    private String password;
    private String clientName;

    private String serverName;
    private long serverVersionMajor;
    private long serverVersionMinor;
    private long serverRevision;
    private String serverTimezone;

    private SocketChannel channel;
    private ReadBuffer reader;
    private WriteBuffer writer;

    private ByteBuffer strBuff = MemoryUtil.allocateDirect(1024);

    public static class Packet {
        public int type;

        // Only one of those two below could be set. The other will be null.

        public CHBlock block;
        public String exceptionMsg;

        public boolean isEndOfStream() {
            return type == CHProtocol.Server.EndOfStream;
        }
    }

    public CHConnection(String host,
                        int port,
                        String defaultDatabase,
                        String user,
                        String password,
                        String clientName) {
        this.host = host;
        this.port = port;
        this.defaultDatabase = defaultDatabase;
        this.user = user;
        this.password = password;
        this.clientName = clientName;
    }

    private void connect() throws IOException {
        channel = SocketChannel.open();
        channel.connect(new InetSocketAddress(host, port));
        reader = new ReadBuffer(channel, READ_BUFFER_SIZE);
        writer = new WriteBuffer(channel, WRITE_BUFFER_SIZE);

        sendHello();
        receiveHello();

        logger.debug("Connected to " + serverName +
                " server version" + serverVersionMajor +
                "." + serverVersionMinor +
                "." + serverRevision + ".");
    }

    public void disconnect() throws IOException {
        if (channel != null) {
            IOUtils.closeQuietly(channel);
            IOUtils.closeQuietly(reader);
            IOUtils.closeQuietly(writer);

            channel = null;
            writer = null;
            reader = null;
        }
    }

    @Override
    public void close() throws IOException {
        disconnect();

        if (strBuff != null) {
            MemoryUtil.free(strBuff);
            strBuff = null;
        }
    }

    public void sendCancel() throws IOException {
        writer.writeVarUInt64(CHProtocol.Client.Query);
        writer.flush();
    }

    public void sendSharedQuery(String query, String query_id, int clients) throws IOException {
        sendQuery(query, query_id, Collections.singletonList(new CHSetting.SettingUInt("shared_query_clients", clients)));
    }

    /**
     * Currently only support shared query.
     * For normal query, we should complete {@link #receivePacket()} method.
     */
    public void sendQuery(String query, String query_id, List<CHSetting> settings) throws IOException {
        Preconditions.checkArgument(query != null && !query.isEmpty() && query_id != null && !query_id.isEmpty());

        boolean isSharedQuery = false;
        for (CHSetting setting : settings) {
            if (!isSharedQuery) {
                if (setting instanceof CHSetting.SettingUInt) {
                    CHSetting.SettingUInt intSetting = (CHSetting.SettingUInt) setting;
                    isSharedQuery = "shared_query_clients".equals(intSetting.name) && intSetting.value > 0;
                }
            }
        }
        Preconditions.checkArgument(isSharedQuery, "We only support shared query for now.");


        if (channel == null) {
            connect();
        }

        writer.writeVarUInt64(CHProtocol.Client.Query);
        writer.writeUTF8StrWithVarLen(query_id);

        // We don't send ClientInfo
        writer.writeByte((byte) 0);

        CHSetting.serialize(settings, writer);

        // FetchColumns        = 0,    /// Only read/have been read the columns specified in the query.
        // WithMergeableState  = 1,    /// Until the stage where the results of processing on different servers can be combined.
        // Complete            = 2,    /// Completely.
        writer.writeVarUInt64(2);
        writer.writeVarUInt64(0); // compression

        writer.writeUTF8StrWithVarLen(query);

        // Send an empty block.
        writer.writeVarUInt64(CHProtocol.Client.Data);
        writer.writeUTF8StrWithVarLen("");
        CHEndecoder.encodeEmptyBlock(writer);

        writer.flush();
    }

    public Packet receivePacket() throws IOException {
        Packet res = new Packet();
        res.type = (int) reader.readVarUInt64();
        switch (res.type) {
            case CHProtocol.Server.Data:
                res.block = receiveData();
                return res;
            case CHProtocol.Server.Exception:
                res.exceptionMsg = receiveException();
                return res;
            case CHProtocol.Server.EndOfStream:
                return res;

            // TODO Implement those packet types to support normal query.
            case CHProtocol.Server.Progress:
            case CHProtocol.Server.ProfileInfo:
            case CHProtocol.Server.Totals:
            case CHProtocol.Server.Extremes:
            default:
                disconnect();
                throw new IOException("Unknown packet " + res.type + " from server(" + host + ":" + port + ")");
        }
    }

    private void sendHello() throws IOException {
        writer.writeVarUInt64(CHProtocol.Client.Hello);
        writer.writeUTF8StrWithVarLen("CH " + clientName);
        writer.writeVarUInt64(DBMS_VERSION_MAJOR);
        writer.writeVarUInt64(DBMS_VERSION_MINOR);
        writer.writeVarUInt64(VERSION_REVISION);
        writer.writeUTF8StrWithVarLen(defaultDatabase);
        writer.writeUTF8StrWithVarLen(user);
        writer.writeUTF8StrWithVarLen(password);

        writer.flush();
    }

    private CHBlock receiveData() throws IOException {
        // ﻿external_table_name, ignore
        reader.readUTF8StrWithVarLen(strBuff);

        return CHEndecoder.decode(reader);
    }

    private void receiveHello() throws IOException {
        int type = (int) reader.readVarUInt64();
        switch (type) {
            case CHProtocol.Server.Hello:
                serverName = reader.readUTF8StrWithVarLen(strBuff);
                serverVersionMajor = reader.readVarUInt64();
                serverVersionMinor = reader.readVarUInt64();
                serverRevision = reader.readVarUInt64();
                if (serverRevision >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE) {
                    serverTimezone = reader.readUTF8StrWithVarLen(strBuff);
                }
                break;
            case CHProtocol.Server.Exception:
                String msg = receiveException();
                // Close the socket afte received an exception. Simple strategy to remove the risk of
                // using current socket channel with possible dirty data in input stream.
                disconnect();
                throw new IOException(msg);
            default:
                disconnect();
                throw new IOException("Unexpected packet, (expected Hello or Exception, got " + type + ")");
        }
    }

    private String receiveException() throws IOException {
        return readException("Exception read from CH server (" + host + ":" + port + ")");
    }

    private String readException(String additionalMsg) throws IOException {
        long code = 0;
        String name = "";
        String message = "";
        String stackTrace = "";
        boolean hasNested = false;
        try {
            code = reader.readInt();
            name = reader.readUTF8StrWithVarLen(strBuff);
            message = reader.readUTF8StrWithVarLen(strBuff);
            stackTrace = reader.readUTF8StrWithVarLen(strBuff);
            hasNested = reader.readByte() != 0;
        } catch (Exception e) {
            disconnect();
            hasNested = false;
        }

        String msg = additionalMsg + "Code: " + code + ". " + name + ". " + message + ", Stack trace:\n" + stackTrace;
        if (hasNested) {
            String nestedMsg = readException("");
            msg += "\n" + nestedMsg;
        }
        return msg;
    }
}
