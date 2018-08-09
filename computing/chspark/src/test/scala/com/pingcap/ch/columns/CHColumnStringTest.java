package com.pingcap.ch.columns;

import static org.junit.Assert.assertEquals;

import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Test;

public class CHColumnStringTest {

    public void insertAndRead(int num) throws Exception {
        final String prefix = "abc你好";
        CHColumnString str = new CHColumnString(num);
        for (int i = 0; i < num - 1; i++) {
            str.insertUTF8String(UTF8String.fromString(prefix + i));
        }
        str.insertDefault();
        CHColumn col = str.seal();
        for (int i = 0; i < num - 1; i++) {
            assertEquals(prefix + i, col.getUTF8String(i).toString());
        }
        assertEquals("", col.getUTF8String(col.size() - 1).toString());
        assertEquals(num, col.size());
        col.free();
    }

    @Test
    public void singlePageTest() throws Exception {
        insertAndRead(CHColumnString.MIN_PAGE_SIZE - 1);
    }

    @Test
    public void multiplePageTest() throws Exception {
        insertAndRead(CHColumnString.MIN_PAGE_SIZE + 1);
    }
}