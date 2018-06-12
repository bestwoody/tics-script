package com.pingcap.theflash;

import com.pingcap.theflash.codegene.CHColumnBatch;

import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

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
                ") ENGINE = MergeTree(id_dt, id_dt, 8192);";
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
                        new Integer(3333),
                        new Integer(2),
                        new Integer(3333),
                        new Long(3423424),
                        new Float(3.14159),
                        new Double(3.14159),
                        new Integer(20),
                        new Integer(333),
                        new Long(123144),
                        new Long((long) i),
                        new Byte((byte) -23),
                        new Short((short) -244),
                        new Integer(-9877323),
                        new Long(-9998712323L),
                        UTF8String.fromString("Hello!"),

                        new Integer(3333),
                        new Integer(2),
                        new Integer(3333),
                        new Long(3423424),
                        new Float(3.14159),
                        new Double(3.14159),
                        new Integer(20),
                        new Integer(333),
                        new Long(123144),
                        new Long(7784564564L),
                        new Byte((byte) -23),
                        new Short((short) -244),
                        new Integer(-9877323),
                        new Long(-9998712323L),
                        UTF8String.fromString("Hello!"),
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
}
