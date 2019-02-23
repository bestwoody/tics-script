package com.pingcap.ch;

import com.pingcap.common.MemoryUtil;
import com.pingcap.common.ReadBuffer;
import com.pingcap.common.WriteBuffer;
import com.pingcap.tikv.kvproto.Kvrpcpb;
import com.pingcap.tikv.txn.Lock;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shade.com.google.common.base.Preconditions;
import shade.com.google.protobuf.ByteString;

/**
 * ClickHouse java connection. You should {@link #close} it to release connection resources after
 * used.
 *
 * <p>Not multi-thread safe.
 */
public class CHConnection implements Closeable {
  private static final long DBMS_VERSION_MAJOR = 1;
  private static final long DBMS_VERSION_MINOR = 1;
  private static final long VERSION_REVISION = 54370;
  private static final long DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE = 54058;

  private static final int READ_BUFFER_SIZE = 87380; // Linux default read buffer size.
  private static final int WRITE_BUFFER_SIZE = 16384; // Linux default write buffer size.

  private static final Logger logger = LoggerFactory.getLogger(CHConnection.class);

  private final String host;
  private final int port;
  private final String defaultDatabase;
  private final String user;
  private final String password;
  private final String clientName;

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

    // Only one of those below could be set. The other will be null.

    public CHBlock block;
    public String exceptionMsg;
    public CHProgress progress;
    public CHBlockStreamProfileInfo profileInfo;
    public List<Lock> lockInfos;

    public boolean isEndOfStream() {
      return type == CHProtocol.Server.EndOfStream;
    }
  }

  public CHConnection(
      String host,
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
    try {
      channel = SocketChannel.open();
      channel.connect(new InetSocketAddress(host, port));
      reader = new ReadBuffer(channel, READ_BUFFER_SIZE);
      writer = new WriteBuffer(channel, WRITE_BUFFER_SIZE);

      sendHello();
      receiveHello();

      logger.debug(
          "Connected to "
              + serverName
              + " server version"
              + serverVersionMajor
              + "."
              + serverVersionMinor
              + "."
              + serverRevision
              + ".");
    } catch (IOException ex) {
      throw new IOException(genErrorMessage(), ex);
    }
  }

  public void disconnect() {
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
    try {
      writer.writeVarUInt64(CHProtocol.Client.Query);
      writer.flush();
    } catch (IOException ex) {
      throw new IOException(genErrorMessage(), ex);
    }
  }

  public void sendSharedQuery(String query, String query_id, int clients) throws IOException {
    sendQuery(
        query,
        query_id,
        Collections.singletonList(new CHSetting.SettingUInt("shared_query_clients", clients)));
  }

  /**
   * Currently only support shared query. For normal query, we should complete {@link
   * #receivePacket()} method.
   */
  public void sendQuery(String query, String query_id, List<CHSetting> settings)
      throws IOException {
    Preconditions.checkArgument(query != null && !query.isEmpty());
    if (query_id == null) {
      query_id = "";
    }

    if (channel == null) {
      connect();
    }

    try {
      writer.writeVarUInt64(CHProtocol.Client.Query);
      writer.writeUTF8StrWithVarLen(query_id);

      // We don't send ClientInfo
      writer.writeByte((byte) 0);

      CHSetting.serialize(settings, writer);

      // FetchColumns        = 0,    /// Only read/have been read the columns specified in the
      // query.
      // WithMergeableState  = 1,    /// Until the stage where the results of processing on
      // different servers can be combined.
      // Complete            = 2,    /// Completely.
      writer.writeVarUInt64(2);
      writer.writeVarUInt64(0); // uncompress

      writer.writeUTF8StrWithVarLen(query);

      // Send an empty block.
      writer.writeVarUInt64(CHProtocol.Client.Data);
      writer.writeUTF8StrWithVarLen("");
      CHEndecoder.encode(writer, new CHBlock());

      writer.flush();
    } catch (IOException ex) {
      throw new IOException(genErrorMessage(), ex);
    }
  }

  public void sendData(CHBlock block, String name) throws IOException {
    try {
      writer.writeVarUInt64(CHProtocol.Client.Data);
      writer.writeUTF8StrWithVarLen(name);
      CHEndecoder.encode(writer, block);
      writer.flush();
    } catch (IOException ex) {
      throw new IOException(genErrorMessage(), ex);
    }
  }

  public Packet receivePacket() throws IOException {
    Packet res = new Packet();
    try {
      res.type = (int) reader.readVarUInt64();
      switch (res.type) {
        case CHProtocol.Server.Data:
          res.block = receiveData();
          return res;
        case CHProtocol.Server.Exception:
          res.exceptionMsg = receiveException();
          return res;
        case CHProtocol.Server.LockInfos:
          res.lockInfos = receiveLockInfos();
          return res;
        case CHProtocol.Server.EndOfStream:
          return res;
        case CHProtocol.Server.Progress:
          res.progress = CHProgress.read(reader);
          return res;
        case CHProtocol.Server.ProfileInfo:
          res.profileInfo = CHBlockStreamProfileInfo.read(reader);
          return res;

          // Block with total or extreme values is passed in same form as ordinary block. The only
          // difference is packed id.
        case CHProtocol.Server.Totals:
        case CHProtocol.Server.Extremes:
          res.block = receiveData();
          return res;
        default:
          disconnect();
      }
    } catch (IOException ex) {
      throw new IOException(genErrorMessage(), ex);
    }
    throw new IOException("Unknown packet " + res.type + " from server(" + host + ":" + port + ")");
  }

  /** Only return Data, Exception or EndOfStream packets, other packets are ignore. */
  public Packet receiveDataPacket() throws IOException {
    while (true) {
      CHConnection.Packet p = receivePacket();
      switch (p.type) {
        case CHProtocol.Server.Data:
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
        // Close the socket after received an exception. Simple strategy to remove the risk of
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

  private List<Lock> receiveLockInfos() throws IOException {
    List<Lock> lockInfos = new ArrayList<Lock>();
    long size = reader.readVarUInt64();
    for (int i = 0; i < size; i++) {
      byte[] primary = reader.readBytesWithVarLen(strBuff);
      long txnID = reader.readVarUInt64();
      byte[] key = reader.readBytesWithVarLen(strBuff);
      long ttl = reader.readVarUInt64();
      Kvrpcpb.LockInfo.Builder builder =
          Kvrpcpb.LockInfo.newBuilder()
              .setKey(ByteString.copyFrom(key))
              .setLockTtl(ttl)
              .setLockVersion(txnID)
              .setPrimaryLock(ByteString.copyFrom(primary));
      Lock lock = new Lock(builder.build());
      lockInfos.add(lock);
    }
    return lockInfos;
  }

  private String readException(String additionalMsg) {
    long code = 0;
    String name = "";
    String message = "";
    String stackTrace = "";
    boolean hasNested;
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

    String msg =
        additionalMsg
            + "Code: "
            + code
            + ". "
            + name
            + ". "
            + message
            + ", Stack trace:\n"
            + stackTrace;
    if (hasNested) {
      String nestedMsg = readException("");
      msg += "\n" + nestedMsg;
    }
    return msg;
  }

  private String genErrorMessage() {
    return String.format("Error in connection: host=[%s] port=[%d]", host, port);
  }
}
