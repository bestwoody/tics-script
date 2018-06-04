package com.pingcap.theflash.codegene;

import com.pingcap.ch.CHBlock;
import com.pingcap.ch.columns.CHColumnWithTypeAndName;

import org.apache.spark.sql.types.StructType;

import java.util.List;

public class CHColumnBatch {
    private final StructType schema;
    private final CHBlock chBlock;
    private final int numRows;
    private final CHColumnVector[] columns;

    public CHColumnBatch(CHBlock chBlock, StructType schema) {
        this.schema = schema;
        this.chBlock = chBlock;
        this.numRows = chBlock.rowCount();
        List<CHColumnWithTypeAndName> chCols = chBlock.columns();
        this.columns = new CHColumnVector[chCols.size()];
        for (int i = 0; i < columns.length; i++) {
            columns[i] = new CHColumnVector(chCols.get(i));
        }
    }

    public CHBlock chBlock() {
        return chBlock;
    }

    public CHColumnVector[] columns() {
        return columns;
    }

    /**
     * Returns the number of columns that make up this batch.
     */
    public int numCols() { return columns.length; }

    /**
     * Returns the number of rows for read, including filtered rows.
     */
    public int numRows() { return numRows; }

    /**
     * Returns the schema that makes up this batch.
     */
    public StructType schema() { return schema; }

    /**
     * Returns the column at `ordinal`.
     */
    public CHColumnVector column(int ordinal) {
        return columns[ordinal];
    }

}
