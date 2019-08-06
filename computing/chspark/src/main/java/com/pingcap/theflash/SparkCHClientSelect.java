package com.pingcap.theflash;

import static com.pingcap.tikv.util.BackOffFunction.BackOffFuncType.BoRegionMiss;
import static com.pingcap.tikv.util.BackOffFunction.BackOffFuncType.BoTxnLockFast;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.pingcap.ch.CHBlock;
import com.pingcap.ch.CHConnection;
import com.pingcap.ch.CHProtocol;
import com.pingcap.ch.CHSetting;
import com.pingcap.theflash.codegen.CHColumnBatch;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.event.CacheInvalidateEvent;
import com.pingcap.tikv.key.Key;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.log4j.Logger;
import org.apache.spark.sql.ch.CHUtil;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.TypeMapping;
import org.spark_project.jetty.util.ArrayQueue;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.TikvGrpc;
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
  private final Function<CacheInvalidateEvent, Void> cacheInvalidateCallBack;

  private TiSession tiSession;
  private TiTimestamp startTs;
  private Long schemaVersion;

  private CHBlock chSchema;
  private StructType sparkSchema;

  private Queue<CHSession> sessionQ;

  private AtomicBoolean closed = new AtomicBoolean(false);

  private static final Logger logger = Logger.getLogger(SparkCHClientSelect.class);

  public SparkCHClientSelect(String query, String host, int port) {
    this(CHUtil.genQueryId("G"), query, host, port);
  }

  public SparkCHClientSelect(String queryId, String query, String host, int port) {
    this(queryId, query, host, port, 0, 0, false, null, null, null, null);
  }

  public SparkCHClientSelect(
      String query,
      String host,
      int port,
      TiSession tiSession,
      TiTimestamp startTs,
      Long schemaVersion,
      TiRegion[] region) {
    this(
        CHUtil.genQueryId("G"),
        query,
        host,
        port,
        0,
        0,
        false,
        tiSession,
        startTs,
        schemaVersion,
        region);
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
      TiTimestamp startTs,
      Long schemaVersion,
      TiRegion[] regions) {
    this.queryId = queryId;
    this.query = query;
    this.clientCount = clientCount;
    this.clientIndex = clientIndex;
    this.sharedMode = sharedMode;
    this.tiSession = tiSession;
    this.startTs = startTs;
    this.schemaVersion = schemaVersion;
    this.sessionQ = new ArrayQueue<>();
    this.cacheInvalidateCallBack =
        tiSession == null ? null : tiSession.getCacheInvalidateCallback();
    // null regions is used to get data from system table (e.g., fetch schema information)
    // regions won't be null when reading data from TxnMergeTree engine
    if (regions == null) {
      this.sessionQ.offer(new CHSession(host, port, (TiRegion) null));
    } else {
      this.sessionQ.offer(new CHSession(host, port, new HashSet<>(Arrays.asList(regions))));
    }
  }

  @Override
  public void close() throws IOException {
    if (!closed.compareAndSet(false, true)) return;

    while (!sessionQ.isEmpty()) {
      CHSession session = sessionQ.poll();
      session.close();
    }
    chSchema = null;
    sparkSchema = null;
  }

  private class CHSession {
    private CHConnection conn;
    private Set<TiRegion> regions;
    private CHBlock curBlock;
    private AtomicBoolean closed = new AtomicBoolean(false);

    public CHSession(String host, int port, Set<TiRegion> regions) {
      this.conn = new CHConnection(host, port, "", "default", "", "CHSpark");
      this.regions = regions;
    }

    public CHSession(String host, int port, TiRegion region) {
      this.conn = new CHConnection(host, port, "", "default", "", "CHSpark");
      if (region != null) {
        this.regions = new HashSet<>();
        this.regions.add(region);
      }
    }

    public void close() {
      if (!closed.compareAndSet(false, true)) return;

      IOUtils.closeQuietly(conn);
      conn = null;
      sparkSchema = null;
    }

    private class RegionsArray {
      private List<String> regions;

      public RegionsArray(List<String> regions) {
        this.regions = regions;
      }

      public List<String> getRegions() {
        return regions;
      }
    }

    protected CHConnection.Packet sendQueryInternal() throws IOException {
      if (sharedMode) {
        conn.sendSharedQuery(query, queryId, clientCount);
      } else {
        ImmutableList.Builder<CHSetting> listBuilder = ImmutableList.builder();
        if (startTs != null) {
          listBuilder.add(new CHSetting.SettingUInt("read_tso", startTs.getVersion()));
        }
        if (schemaVersion != null) {
          listBuilder.add(new CHSetting.SettingInt("schema_version", schemaVersion));
        }
        if (tiSession != null) {
          listBuilder.add(new CHSetting.SettingUInt("resolve_locks", 1));
        }
        if (regions != null && regions.size() > 0) {
          List<String> regionArray = new ArrayList<>();
          for (TiRegion region : regions) {
            regionArray.add(region.getMeta().toString());
          }
          listBuilder.add(
              new CHSetting.SettingString(
                  "regions", new ObjectMapper().writeValueAsString(new RegionsArray(regionArray))));
        }
        conn.sendQuery(query, queryId, listBuilder.build());
      }
      return conn.receivePacket();
    }

    public boolean hasNext() throws RuntimeException {
      String exception = null;
      try {
        if (closed.get()) {
          return false;
        }

        sendQueryIfNot();

        if (closed.get()) {
          return false;
        }

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
        throw new RuntimeException("Exception when fetching next block. SQL: " + query, e);
      }
      if (exception != null) {
        throw new RuntimeException(exception);
      }
      return curBlock != null;
    }

    public CHColumnBatch Next() {
      return new CHColumnBatch(curBlock, sparkSchema);
    }

    private void updateRegions(List<TiRegion> regions) {
      for (TiRegion region : regions) {

        long storeId =
            CHUtil.getRegionLearnerPeerByLabel(
                    region, tiSession.getRegionManager(), "zone", "engine")
                .get()
                .getStoreId();
        tiSession.getRegionManager().onRequestFail(region.getId(), storeId);
        cacheInvalidateCallBack.apply(
            new CacheInvalidateEvent(
                region.getId(), storeId, true, true, CacheInvalidateEvent.CacheType.REGION_STORE));
      }
      Set<TiRegion> newRegions = new HashSet<>();
      for (TiRegion region : regions) {
        Key startKey = Key.toRawKey(region.getStartKey());
        Key endKey = Key.toRawKey(region.getEndKey());
        Key cur = startKey;
        while (cur.compareTo(endKey) < 0) {
          TiRegion newRegion = tiSession.getRegionManager().getRegionByKey(cur.toByteString());
          String host =
              tiSession
                  .getRegionManager()
                  .getStoreById(
                      CHUtil.getRegionLearnerPeerByLabel(
                              region, tiSession.getRegionManager(), "zone", "engine")
                          .get()
                          .getStoreId())
                  .getAddress()
                  .split(":")[0];
          if (!host.equals(conn.host)) {
            sessionQ.offer(new CHSession(host, conn.port, newRegion));
          } else {
            newRegions.add(newRegion);
          }
          cur = Key.toRawKey(newRegion.getEndKey());
        }
      }
      this.regions.addAll(newRegions);
    }

    public void sendQueryIfNot() throws IOException {
      BackOffer backOffer = ConcreteBackOffer.newCopNextMaxBackOff();
      while (chSchema == null) {
        CHConnection.Packet p = sendQueryInternal();
        if (p.lockInfos != null) {
          if (tiSession == null) {
            throw new RuntimeException(new LockException());
          }
          RegionManager regionMgr = tiSession.getRegionManager();
          boolean resolved = true;
          for (Lock lockInfo : p.lockInfos) {
            Pair<TiRegion, Metapb.Store> pair =
                regionMgr.getRegionStorePairByKey(lockInfo.getKey());
            Metapb.Store store = pair.second;
            String addressStr = store.getAddress();
            ManagedChannel channel = tiSession.getChannel(addressStr);

            TikvGrpc.TikvBlockingStub blockingStub = TikvGrpc.newBlockingStub(channel);
            TikvGrpc.TikvStub asyncStub = TikvGrpc.newStub(channel);

            LockResolverClient lockResolver =
                new LockResolverClient(tiSession, blockingStub, asyncStub);
            boolean ok = lockResolver.resolveLocks(backOffer, Collections.singletonList(lockInfo));
            if (!ok) {
              resolved = false;
            }
          }
          if (!resolved) {
            backOffer.doBackOff(BoTxnLockFast, new LockException());
          }
        } else if (p.exceptionRegionIDs != null) {
          List<TiRegion> retryRegions =
              regions
                  .stream()
                  .filter(x -> p.exceptionRegionIDs.contains(x.getId()))
                  .collect(Collectors.toList());
          for (TiRegion region : retryRegions) {
            regions.remove(region);
          }
          logger.warn(String.format("retry regions: [%s]", retryRegions));
          updateRegions(retryRegions);
          if (regions.size() == 0) {
            close();
            return;
          }
          backOffer.doBackOff(
              BoRegionMiss, new RuntimeException("retry timeout for regions: " + regions));
        } else {
          if (p.isEndOfStream()) {
            // No schema return. We are done.
            close();
            return;
          }
          if (p.block == null || p.block.isEmpty()) {
            throw new IOException(
                "Read schema failed"
                    + ((p.exceptionMsg == null) ? "" : ", reason: " + p.exceptionMsg));
          }
          chSchema = p.block;
          sparkSchema = TypeMapping.chSchemaToSparkSchema(chSchema);
        }
      }
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
  }

  @Override
  public boolean hasNext() {
    while (!sessionQ.isEmpty()) {
      try {
        CHSession sess = sessionQ.element();
        if (sess.hasNext()) {
          return true;
        }
        sess.close();
        sessionQ.poll();
        continue;
      } catch (RuntimeException e) {
        // We need to close the connection after bad things happened from here.
        // As spark won't do it.
        IOUtils.closeQuietly(this);
        throw e;
      }
    }
    return false;
  }

  @Override
  public CHColumnBatch next() {
    return sessionQ.element().Next();
  }
}
