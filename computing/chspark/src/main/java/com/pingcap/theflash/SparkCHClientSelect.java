package com.pingcap.theflash;

import static com.pingcap.tikv.util.BackOffFunction.BackOffFuncType.BoTxnLockFast;

import com.google.common.collect.ImmutableList;
import com.pingcap.ch.CHBlock;
import com.pingcap.ch.CHConnection;
import com.pingcap.ch.CHProtocol;
import com.pingcap.ch.CHSetting;
import com.pingcap.theflash.codegen.CHColumnBatch;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.kvproto.Metapb;
import com.pingcap.tikv.kvproto.TikvGrpc;
import com.pingcap.tikv.meta.TiTimestamp;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.txn.Lock;
import com.pingcap.tikv.txn.LockResolverClient;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import com.pingcap.tikv.util.Pair;
import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.spark.sql.ch.CHUtil;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.TypeMapping;
import shade.io.grpc.ManagedChannel;

/**
 * Select API for spark to send select query to CH.
 *
 * <p>Not multi-thread safe.
 */
public class SparkCHClientSelect implements Closeable, Iterator<CHColumnBatch> {
  private final String queryId;
  private final String query;
  private final int clientCount;
  private final int clientIndex;
  private final boolean sharedMode;

  private TiSession tiSession;
  private TiTimestamp startTs;

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

  public SparkCHClientSelect(
      String query, String host, int port, TiSession tiSession, TiTimestamp startTs) {
    this(CHUtil.genQueryId("G"), query, host, port, 0, 0, false, tiSession, startTs);
  }

  public SparkCHClientSelect(
      String queryId,
      String query,
      String host,
      int port,
      int clientCount,
      int clientIndex,
      boolean sharedMode) {
    this(queryId, query, host, port, clientCount, clientIndex, sharedMode, null, null);
  }

  public SparkCHClientSelect(
      String queryId,
      String query,
      String host,
      int port,
      int clientCount,
      int clientIndex,
      boolean sharedMode,
      TiSession tiSession,
      TiTimestamp startTs) {
    this.conn = new CHConnection(host, port, "", "default", "", "CHSpark");
    this.queryId = queryId;
    this.query = query;
    this.clientCount = clientCount;
    this.clientIndex = clientIndex;
    this.sharedMode = sharedMode;
    this.tiSession = tiSession;
    this.startTs = startTs;
  }

  @Override
  public void close() throws IOException {
    if (!closed.compareAndSet(false, true)) return;

    IOUtils.closeQuietly(conn);
    conn = null;
    chSchema = null;
    sparkSchema = null;
  }

  protected CHConnection.Packet sendQueryInternal() throws IOException {
    if (sharedMode) {
      conn.sendSharedQuery(query, queryId, clientCount);
    } else {
      ImmutableList.Builder<CHSetting> listBuilder = ImmutableList.builder();
      if (startTs != null) {
        listBuilder.add(new CHSetting.SettingUInt("read_tso", startTs.getVersion()));
      }
      if (tiSession != null) {
        listBuilder.add(new CHSetting.SettingUInt("resolve_locks", 1));
      }
      conn.sendQuery(query, queryId, listBuilder.build());
    }
    return conn.receivePacket();
  }

  public void sendQueryIfNot() throws IOException {
    if (chSchema == null) {
      CHConnection.Packet p = sendQueryInternal();
      if (p.lockInfos != null) {
        if (tiSession == null) {
          throw new RuntimeException(new LockException());
        }
        RegionManager regionMgr = tiSession.getRegionManager();
        for (Lock lockInfo : p.lockInfos) {
          Pair<TiRegion, Metapb.Store> pair = regionMgr.getRegionStorePairByKey(lockInfo.getKey());
          Metapb.Store store = pair.second;
          String addressStr = store.getAddress();
          ManagedChannel channel = tiSession.getChannel(addressStr);

          TikvGrpc.TikvBlockingStub blockingStub = TikvGrpc.newBlockingStub(channel);
          TikvGrpc.TikvStub asyncStub = TikvGrpc.newStub(channel);

          LockResolverClient lockResolver =
              new LockResolverClient(tiSession, blockingStub, asyncStub);
          BackOffer backOffer = ConcreteBackOffer.newCopNextMaxBackOff();
          boolean ok = lockResolver.resolveLocks(backOffer, Collections.singletonList(lockInfo));
          if (!ok) {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
            backOffer.doBackOff(BoTxnLockFast, new LockException());
          }
        }
        // Resend query after resolving locks.
        p = sendQueryInternal();
        if (p.lockInfos != null) {
          throw new IOException(
              "CH query failed due to received "
                  + p.lockInfos.size()
                  + " locks after resolving locks");
        }
      }
      if (p.isEndOfStream()) {
        // No schema return. We are done.
        close();
        return;
      }
      if (p.block == null || p.block.isEmpty()) {
        throw new IOException(
            "Read schema failed" + ((p.exceptionMsg == null) ? "" : ", reason: " + p.exceptionMsg));
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
