package com.pingcap.theflash;

import com.pingcap.theflash.codegene.CHColumnBatch;

import org.apache.spark.sql.ch.SimpleRow;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Random;
import java.util.function.Consumer;

public class SparkCHClientInsertTest {
    @Test
    public void insert() throws Exception {
        String dropSql = "drop table if exists default.spark_insert_test";
        try (SparkCHClientSelect select = new SparkCHClientSelect("inserqid" + 0, dropSql, "127.0.0.1", 9000)) {
            while (select.hasNext())
                select.next();
        }

        String createSql = "create table default.spark_insert_test (\n" +
                "id_dt       Date,\n" +
                "tp_boolean  UInt8,\n" +
                "tp_date     Date,\n" +
                "tp_datetime DateTime,\n" +
                "tp_float32  Float32,\n" +
                "tp_float64  Float64,\n" +
                "tp_uint8    UInt8,\n" +
                "tp_uint16   UInt16,\n" +
                "tp_uint32   UInt32,\n" +
                "tp_uint64   UInt64,\n" +
                "tp_int8     Int8,\n" +
                "tp_int16    Int16,\n" +
                "tp_int32    Int32,\n" +
                "tp_int64    Int64,\n" +
                "tp_string   String,\n" +
                "\n" +
                "null_id_dt        Nullable(Date),\n" +
                "null_tp_boolean   Nullable(UInt8),\n" +
                "null_tp_date      Nullable(Date),\n" +
                "null_tp_datetime  Nullable(DateTime),\n" +
                "null_tp_float32   Nullable(Float32),\n" +
                "null_tp_float64   Nullable(Float64),\n" +
                "null_tp_uint8     Nullable(UInt8),\n" +
                "null_tp_uint16    Nullable(UInt16),\n" +
                "null_tp_uint32    Nullable(UInt32),\n" +
                "null_tp_uint64    Nullable(UInt64),\n" +
                "null_tp_int8      Nullable(Int8),\n" +
                "null_tp_int16     Nullable(Int16),\n" +
                "null_tp_int32     Nullable(Int32),\n" +
                "null_tp_int64     Nullable(Int64),\n" +
                "null_tp_string    Nullable(String)\n" +
                "\n" +
                ") ENGINE = MutableMergeTree(id_dt, 8192);";
        try (SparkCHClientSelect select = new SparkCHClientSelect("inserqid" + 1, createSql, "127.0.0.1", 9000)) {
            while (select.hasNext())
                select.next();
        }

        Random random = new Random();
        long insertCount = random.nextInt(SparkCHClientInsert.BATCH_INSERT_COUNT * 3 + 1000) + SparkCHClientInsert.BATCH_INSERT_COUNT * 3;
        String insertSql = "insert into default.spark_insert_test values";
        try (SparkCHClientInsert insert = new SparkCHClientInsert("inserqid" + 2, insertSql, "127.0.0.1", 9000)) {
            insert.insertPrefix();
            for (int i = 0; i < insertCount; i++) {
                Object[] fields = new Object[]{
                        new Date(365 * 1000L * 60 * 60 * 24),
                        new Integer(1),
                        new Date(365 * 1000L * 60 * 60 * 24),
                        new Timestamp(31536001000L),
                        new Float(3.14159),
                        new Double(3.14159),
                        new Integer(20),
                        new Integer(333),
                        new Long(123144),
                        new BigDecimal((long) i),
                        new Byte((byte) -23),
                        new Short((short) -244),
                        new Integer(-9877323),
                        new Long(-9998712323L),
                        "Hello!",

                        new Date(365 * 1000L * 60 * 60 * 24),
                        new Integer(1),
                        new Date(365 * 1000L * 60 * 60 * 24),
                        new Timestamp(31536001000L),
                        new Float(3.14159),
                        new Double(3.14159),
                        new Integer(20),
                        new Integer(333),
                        new Long(123144),
                        new BigDecimal(7784564564L),
                        new Byte((byte) -23),
                        new Short((short) -244),
                        new Integer(-9877323),
                        new Long(-9998712323L),
                        "Hello!",
                };
                insert.insert(new SimpleRow(fields));
            }
            insert.insertSuffix();
        }

        String countSql = "select count(*) from default.spark_insert_test";
        try (SparkCHClientSelect select = new SparkCHClientSelect("inserqid" + 3, countSql, "127.0.0.1", 9000)) {
            long count = -1;
            while (select.hasNext()) {
                CHColumnBatch batch = select.next();
                count = batch.column(0).getLong(0);
            }
            Assert.assertEquals(insertCount, count);
        }

        try (SparkCHClientSelect select = new SparkCHClientSelect("inserqid" + 4, dropSql, "127.0.0.1", 9000)) {
            while (select.hasNext())
                select.next();
        }
    }

    private void selectSql(String sql) throws Exception {
        selectSql(sql, null);
    }

    private void selectSql(String sql, Consumer<CHColumnBatch> f) throws Exception {
        try (SparkCHClientSelect select = new SparkCHClientSelect("", sql, "127.0.0.1", 9000)) {
            while (select.hasNext()) {
                if (f != null) f.accept(select.next());
            }
        }
    }

    private static class ValueFetcher implements Consumer<CHColumnBatch> {
        Object[] values;

        @Override
        public void accept(CHColumnBatch b) {
            values[0] = new Date(b.column(0).getInt(0) * 1000L * 60 * 60 * 24);
            values[1] = new Date(b.column(1).getInt(0) * 1000L * 60 * 60 * 24);
            values[2] = new Timestamp(b.column(2).getLong(0) / 1000);
            values[3] = b.column(3).getFloat(0);
            values[4] = b.column(4).getDouble(0);
            values[5] = b.column(5).getInt(0);
            values[6] = b.column(6).getInt(0);
            values[7] = b.column(7).getLong(0);
            values[8] = b.column(8).getDecimal(0, 20, 0).toJavaBigDecimal();
            values[9] = b.column(9).getByte(0);
            values[10] = b.column(10).getShort(0);
            values[11] = b.column(11).getInt(0);
            values[12] = b.column(12).getLong(0);
            values[13] = b.column(13).getUTF8String(0).toString();
        }
    }

    @Test
    public void insert2() throws Exception {
        selectSql("drop table if exists default.spark_insert_test");
        String createSql = "create table default.spark_insert_test (\n" +
                "id_dt       Date,\n" +
                "tp_date     Date,\n" +
                "tp_datetime DateTime,\n" +
                "tp_float32  Float32,\n" +
                "tp_float64  Float64,\n" +
                "tp_uint8    UInt8,\n" +
                "tp_uint16   UInt16,\n" +
                "tp_uint32   UInt32,\n" +
                "tp_uint64   UInt64,\n" +
                "tp_int8     Int8,\n" +
                "tp_int16    Int16,\n" +
                "tp_int32    Int32,\n" +
                "tp_int64    Int64,\n" +
                "tp_string   String\n" +
                ") ENGINE = MutableMergeTree(id_dt, 8192);";
        selectSql(createSql);

        Object[] values = new Object[]{
                new Date(365 * 1000L * 60 * 60 * 24),
                new Date(365 * 1000L * 60 * 60 * 24),
                new Timestamp(31536001000L),
                new Float(3.14159),
                new Double(3.14159),
                new Integer(255),
                new Integer(65535),
                new Long(4294967295L),
                new BigDecimal("18446744073709551615"),
                new Byte((byte) -23),
                new Short((short) -244),
                new Integer(-9877323),
                new Long(-9998712323L),
                "Hello!"};

        try (SparkCHClientInsert insert = new SparkCHClientInsert("", "insert into table default.spark_insert_test values", "127.0.0.1", 9000)) {
            insert.insertPrefix();
            insert.insert(new SimpleRow(values));
            insert.insertSuffix();
        }

        Object[] new_values = new Object[14];
        ValueFetcher f = new ValueFetcher();
        f.values = new_values;
        selectSql("select * from default.spark_insert_test", f);
        selectSql("drop table if exists default.spark_insert_test");

        Assert.assertArrayEquals(values, new_values);
    }

    private static class ValueFetcher2 implements Consumer<CHColumnBatch> {
        Object[] values;

        @Override
        public void accept(CHColumnBatch b) {
            values[0] = b.column(0).getDecimal(0, 65, 30).toJavaBigDecimal();
        }
    }

    @Test
    public void insert3() throws Exception {
        selectSql("drop table if exists default.spark_insert_test");
        String createSql = "create table default.spark_insert_test (\n" +
            "tp_decimal  Decimal(65, 30)\n" +
            ") ENGINE = MutableMergeTree(tp_decimal, 8192);";
        selectSql(createSql);

        Object[] values = new Object[]{
            new BigDecimal("255000000001000000002000000003.000000004000000050000000000000")};

        try (SparkCHClientInsert insert = new SparkCHClientInsert("", "insert into table default.spark_insert_test values", "127.0.0.1", 9000)) {
            insert.insertPrefix();
            insert.insert(new SimpleRow(values));
            insert.insertSuffix();
        }

        Object[] new_values = new Object[1];
        ValueFetcher2 f = new ValueFetcher2();
        f.values = new_values;
        selectSql("select * from default.spark_insert_test", f);
        selectSql("drop table if exists default.spark_insert_test");

        Assert.assertArrayEquals(values, new_values);
    }
}
