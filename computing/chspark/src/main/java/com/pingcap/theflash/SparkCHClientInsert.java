package com.pingcap.theflash;

import com.pingcap.ch.CHBlock;
import com.pingcap.ch.CHBlockInfo;
import com.pingcap.ch.CHConnection;
import com.pingcap.ch.CHProtocol;
import com.pingcap.ch.CHSetting;
import com.pingcap.ch.columns.CHColumn;
import com.pingcap.ch.columns.CHColumnWithTypeAndName;
import com.pingcap.ch.datatypes.*;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.ch.CHUtil;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.TypeMapping;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Insert API for spark to send insert query to CH.
 *
 * <p>Not multi-thread safe.
 */
public class SparkCHClientInsert implements Closeable {
  public static final int CLIENT_BATCH_INSERT_COUNT = 1024 * 32;
  public static final long STORAGE_BATCH_INSERT_COUNT_ROWS = 1024L * 1024L * 256L;
  public static final long STORAGE_BATCH_INSERT_COUNT_BYTES = 1024L * 1024L * 1024L * 4L;

  private String queryId;
  private String query;
  private CHConnection conn;
  private CHBlock sampleBlock;
  private CHType[] dataTypes;
  private StructType sparkSchema;

  private CHColumn[] curColumns;
  private int clientBatchCount = CLIENT_BATCH_INSERT_COUNT;
  private long storageBatchCountRows = STORAGE_BATCH_INSERT_COUNT_ROWS;
  private long storageBatchCountBytes = STORAGE_BATCH_INSERT_COUNT_BYTES;

  public SparkCHClientInsert(String queryId, String query, String host, int port) {
    this.conn = new CHConnection(host, port, "", "default", "", "CHSpark");
    this.queryId = queryId;
    this.query = query;
  }

  public SparkCHClientInsert(String query, String host, int port) {
    this(CHUtil.genQueryId("I"), query, host, port);
  }

  public void setClientBatch(int batch) {
    this.clientBatchCount = batch;
  }

  public void setStorageBatchRows(long rows) {
    this.storageBatchCountRows = rows;
  }

  public void setStorageBatchBytes(long bytes) {
    this.storageBatchCountBytes = bytes;
  }

  public CHBlock sampleBlock() {
    return sampleBlock;
  }

  public StructType sparkSchema() {
    return sparkSchema;
  }

  public CHType[] dataTypes() {
    return dataTypes;
  }

  private void freeCacheColumns() {
    if (curColumns == null) return;
    for (CHColumn c : curColumns) {
      if (c != null) c.free();
    }
    curColumns = null;
  }

  private void initCacheColumns() {
    freeCacheColumns();

    curColumns = new CHColumn[dataTypes.length];
    for (int i = 0; i < dataTypes.length; i++) {
      curColumns[i] = dataTypes[i].allocate(clientBatchCount);
    }
  }

  public void insertPrefix() throws IOException {
    conn.sendQuery(
        query,
        queryId,
        Arrays.asList(
            new CHSetting.SettingUInt("min_insert_block_size_rows", storageBatchCountRows),
            new CHSetting.SettingUInt("min_insert_block_size_bytes", storageBatchCountBytes)));
    receiveSampleBlock();
    sparkSchema = TypeMapping.chSchemaToSparkSchema(sampleBlock);

    List<CHColumnWithTypeAndName> cols = sampleBlock.columns();
    dataTypes = new CHType[cols.size()];
    for (int i = 0; i < sampleBlock.colCount(); i++) {
      dataTypes[i] = cols.get(i).dataType();
    }
    initCacheColumns();
  }

  public void insertSuffix() throws IOException {
    flushCache();
    // Send an empty block indicates we have finished data transfering.
    insert(new CHBlock());
    receiveEndPacket();
    close();
  }

  @Override
  public void close() throws IOException {
    IOUtils.closeQuietly(conn);
    freeCacheColumns();
    conn = null;
  }

  private void flushCache() throws IOException {
    if (curColumns == null) {
      return;
    }
    ArrayList<CHColumnWithTypeAndName> list = new ArrayList<>(curColumns.length);
    for (int i = 0; i < curColumns.length; i++) {
      list.add(
          new CHColumnWithTypeAndName(
              curColumns[i].seal(), dataTypes[i], sampleBlock.columns().get(i).name()));
    }
    insert(new CHBlock(new CHBlockInfo(), list));
    initCacheColumns();
  }

  public void insertFromTiDB(Row row) throws IOException {
    for (int i = 0; i < dataTypes.length; i++) {
      StructField sparkField = sparkSchema.fields()[i];
      CHColumn col = curColumns[i];
      CHType chType = dataTypes[i];

      if (chType instanceof CHTypeNullable) {
        if (row.isNullAt(i)) {
          col.insertNull();
          continue;
        } else {
          chType = ((CHTypeNullable) chType).nested_data_type;
        }
      }

      // TODO  In some case like min(tp_int8 + tpfloat32 * 2), CH cast the result as Float64 while
      // Spark not.
      if (chType == CHTypeString.instance || chType instanceof CHTypeFixedString) {
        Object val = row.get(i);
        if (val instanceof byte[]) {
          byte[] byteValue = ((byte[]) val);
          col.insertUTF8String(UTF8String.fromBytes(byteValue));
        } else {
          col.insertUTF8String(UTF8String.fromString(row.get(i).toString()));
        }
      } else if (chType == CHTypeDate.instance) {
        Date v = row.getDate(i);
        col.insertShort((short) DateTimeUtils.fromJavaDate(v));
      } else if (chType == CHTypeDateTime.instance) {
        // java.sql.Timestamp as milliseconds, while ClickHouse as seconds.
        Timestamp ts = row.getTimestamp(i);
        col.insertInt((int) (ts.getTime() / 1000));
      } else if (chType == CHTypeNumber.CHTypeInt8.instance) {
        col.insertByte((byte) row.getLong(i));
      } else if (chType == CHTypeNumber.CHTypeInt16.instance) {
        col.insertShort((short) row.getLong(i));
      } else if (chType == CHTypeNumber.CHTypeInt32.instance) {
        col.insertInt((int) row.getLong(i));
      } else if (chType == CHTypeNumber.CHTypeInt64.instance) {
        col.insertLong(row.getLong(i));
      } else if (chType == CHTypeNumber.CHTypeUInt8.instance) {
        // CHTypeUInt8 -> Spark IntegerType
        col.insertByte((byte) (row.getLong(i) & 0x0FF));
      } else if (chType == CHTypeNumber.CHTypeUInt16.instance) {
        // CHTypeUInt16 -> Spark IntegerType
        col.insertShort((short) (row.getLong(i) & 0x0FFFF));
      } else if (chType == CHTypeNumber.CHTypeUInt32.instance) {
        // CHTypeUInt32 -> Spark LongType
        col.insertInt((int) (row.getLong(i) & 0x0FFFF_FFFFL));
      } else if (chType == CHTypeNumber.CHTypeUInt64.instance) {
        col.insertLong(row.getLong(i));
      } else if (chType == CHTypeNumber.CHTypeFloat32.instance) {
        col.insertFloat(row.getFloat(i));
      } else if (chType == CHTypeNumber.CHTypeFloat64.instance) {
        Number n = (Number) row.get(i);
        col.insertDouble(n.doubleValue());
      } else if (chType instanceof CHTypeDecimal) {
        col.insertDecimal(Decimal.fromDecimal(row.getDecimal(i)));
      } else {
        throw new UnsupportedOperationException("Unsupported data type: " + chType.name());
      }
    }

    if (curColumns[0].size() >= clientBatchCount) {
      flushCache();
    }
  }

  public void insert(Row row) throws IOException {
    for (int i = 0; i < dataTypes.length; i++) {
      String colName = sparkSchema.fields()[i].name();
      CHColumn col = curColumns[i];
      CHType chType = dataTypes[i];

      if (chType instanceof CHTypeNullable) {
        if (row.isNullAt(i)) {
          col.insertNull();
          continue;
        } else {
          chType = ((CHTypeNullable) chType).nested_data_type;
        }
      }

      // TODO  In some case like min(tp_int8 + tpfloat32 * 2), CH cast the result as Float64 while
      // Spark not.

      if (chType == CHTypeString.instance || chType instanceof CHTypeFixedString) {
        col.insertUTF8String(UTF8String.fromString(row.getString(i)));
      } else if (chType == CHTypeDate.instance) {
        Date v = row.getDate(i);
        col.insertShort((short) DateTimeUtils.fromJavaDate(v));
      } else if (chType == CHTypeMyDate.instance) {
        Date v = row.getDate(i);
        col.insertLong(DateTimeUtils.fromJavaDate(v));
      } else if (chType == CHTypeDateTime.instance) {
        // java.sql.Timestamp as milliseconds, while ClickHouse as seconds.
        Timestamp ts = row.getTimestamp(i);
        col.insertInt((int) (ts.getTime() / 1000));
      } else if (chType == CHTypeMyDateTime.instance) {
        // java.sql.Timestamp as milliseconds, while ClickHouse as seconds.
        Timestamp ts = row.getTimestamp(i);
        col.insertLong(ts.getTime());
      } else if (chType == CHTypeNumber.CHTypeInt8.instance) {
        col.insertByte(row.getByte(i));
      } else if (chType == CHTypeNumber.CHTypeInt16.instance) {
        col.insertShort(row.getShort(i));
      } else if (chType == CHTypeNumber.CHTypeInt32.instance) {
        col.insertInt(row.getInt(i));
      } else if (chType == CHTypeNumber.CHTypeInt64.instance) {
        col.insertLong(row.getLong(i));
      } else if (chType == CHTypeNumber.CHTypeUInt8.instance) {
        // CHTypeUInt8 -> Spark IntegerType
        col.insertByte((byte) (row.getInt(i) & 0x0FF));
      } else if (chType == CHTypeNumber.CHTypeUInt16.instance) {
        // CHTypeUInt16 -> Spark IntegerType
        col.insertShort((short) (row.getInt(i) & 0x0FFFF));
      } else if (chType == CHTypeNumber.CHTypeUInt32.instance) {
        // CHTypeUInt32 -> Spark LongType
        col.insertInt((int) (row.getLong(i) & 0x0FFFF_FFFFL));
      } else if (chType == CHTypeNumber.CHTypeUInt64.instance) {
        col.insertLong(row.getDecimal(i).longValue());
      } else if (chType == CHTypeNumber.CHTypeFloat32.instance) {
        col.insertFloat(row.getFloat(i));
      } else if (chType == CHTypeNumber.CHTypeFloat64.instance) {
        col.insertDouble(row.getDouble(i));
      } else if (chType instanceof CHTypeDecimal) {
        col.insertDecimal(Decimal.fromDecimal(row.getDecimal(i)));
      } else {
        throw new UnsupportedOperationException("Unsupported data type: " + chType.name());
      }
    }

    if (curColumns[0].size() >= clientBatchCount) {
      flushCache();
    }
  }

  public void insert(CHBlock block) throws IOException {
    if (conn == null) {
      throw new IOException("Closed");
    }
    conn.sendData(block, "");
  }

  /// Receive the block that serves as an example of the structure of table where data will be
  // inserted.
  private boolean receiveSampleBlock() throws IOException {
    CHConnection.Packet packet = conn.receivePacket();
    switch (packet.type) {
      case CHProtocol.Server.Data:
        sampleBlock = packet.block;
        return true;
      case CHProtocol.Server.Exception:
        close();
        throw new IOException("Exception when receive sample block: " + packet.exceptionMsg);
      default:
        close();
        throw new IOException(
            "Unexpected packet from server (expected Data, got " + packet.type + ")");
    }
  }

  private void receiveEndPacket() throws IOException {
    CHConnection.Packet packet = conn.receivePacket();
    switch (packet.type) {
      case CHProtocol.Server.EndOfStream:
        return;
      case CHProtocol.Server.Exception:
        close();
        throw new IOException("Exception when receive sample block: " + packet.exceptionMsg);
      default:
        close();
        throw new IOException(
            "Unexpected packet from server (expected Data, got " + packet.type + ")");
    }
  }
}
