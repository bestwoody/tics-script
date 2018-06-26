package com.pingcap.theflash;

import com.pingcap.ch.CHBlock;
import com.pingcap.ch.CHBlockInfo;
import com.pingcap.ch.CHConnection;
import com.pingcap.ch.CHProtocol;
import com.pingcap.ch.columns.CHColumn;
import com.pingcap.ch.columns.CHColumnWithTypeAndName;
import com.pingcap.ch.datatypes.CHType;
import com.pingcap.ch.datatypes.CHTypeDate;
import com.pingcap.ch.datatypes.CHTypeDecimal;
import com.pingcap.ch.datatypes.CHTypeDateTime;
import com.pingcap.ch.datatypes.CHTypeFixedString;
import com.pingcap.ch.datatypes.CHTypeNullable;
import com.pingcap.ch.datatypes.CHTypeNumber;
import com.pingcap.ch.datatypes.CHTypeString;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.TypeMapping;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Insert API for spark to send insert query to CH.
 *
 * Not multi-thread safe.
 */
public class SparkCHClientInsert implements Closeable {
    public static final int BATCH_INSERT_COUNT = 655360;

    private String queryId;
    private String query;
    private CHConnection conn;
    private CHBlock sampleBlock;
    private CHType[] dataTypes;
    private StructType sparkSchema;

    private CHColumn[] curColumns;
    private int batchCount = BATCH_INSERT_COUNT;

    public SparkCHClientInsert(String queryId, String query, String host, int port) {
        this.conn = new CHConnection(host, port, "", "default", "", "CHSpark");
        this.queryId = queryId;
        this.query = query;
    }

    public void setBatch(int batch) {
        this.batchCount = batch;
    }

    public CHBlock sampleBlock() {
        return sampleBlock;
    }

    public StructType sparkSchema() {
        return sparkSchema;
    }

    private void freeCacheColumns() {
        if (curColumns == null)
            return;
        for (CHColumn c : curColumns) {
            c.free();
        }
        curColumns = null;
    }

    private void initCacheColumns() {
        freeCacheColumns();

        curColumns = new CHColumn[dataTypes.length];
        for (int i = 0; i < dataTypes.length; i++) {
            curColumns[i] = dataTypes[i].allocate(batchCount);
        }
    }

    public void insertPrefix() throws IOException {
        conn.sendQuery(query, queryId, Collections.emptyList());
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
            list.add(new CHColumnWithTypeAndName(curColumns[i].seal(), dataTypes[i], sampleBlock.columns().get(i).name()));
        }
        insert(new CHBlock(new CHBlockInfo(), list));
        initCacheColumns();
    }

    public void insert(InternalRow row) throws IOException {
        for (int i = 0; i < dataTypes.length; i++) {
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

            // TODO  In some case like min(tp_int8 + tpfloat32 * 2), CH cast the result as Float64 while Spark not.

            if (chType == CHTypeString.instance || chType instanceof CHTypeFixedString) {
                col.insertUTF8String(row.getUTF8String(i));
            } else if (chType == CHTypeDate.instance) {
                // Spark store Date type by int, while ClickHosue use int16, i.e. short.
                int v = row.getInt(i);
                if (v < 0) {
                    throw new IllegalStateException("Illegal date value: " + v);
                }
                col.insertShort((short) v);
            } else if (chType == CHTypeDateTime.instance) {
                // Spark store Timestamp by long, as microseconds, while ClickHosue use int32, i.e. int as seconds.
                col.insertInt((int) (row.getLong(i) / 1000 / 1000));
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
                // TODO We might have a bug here. Should use getDecimal instead of getLong.
                col.insertLong(row.getLong(i));
            } else if (chType == CHTypeNumber.CHTypeFloat32.instance) {
                col.insertFloat(row.getFloat(i));
            } else if (chType == CHTypeNumber.CHTypeFloat64.instance) {
                col.insertDouble(row.getDouble(i));
            } else if (chType instanceof CHTypeDecimal) {
                CHTypeDecimal decType = (CHTypeDecimal) chType;
                col.insertDecimal(row.getDecimal(i, decType.precision, decType.scale));
            } else {
                throw new UnsupportedOperationException("Unsupported data type: " + chType.name());
            }
        }

        if (curColumns[0].size() >= batchCount) {
            flushCache();
        }
    }

    public void insert(CHBlock block) throws IOException {
        if (conn == null) {
            throw new IOException("Closed");
        }
        conn.sendData(block, "");
    }

    /// Receive the block that serves as an example of the structure of table where data will be inserted.
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
                throw new IOException("Unexpected packet from server (expected Data, got " + packet.type + ")");
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
                throw new IOException("Unexpected packet from server (expected Data, got " + packet.type + ")");
        }
    }
}
