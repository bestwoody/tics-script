package com.pingcap.ch.datatypes;

import com.pingcap.ch.columns.CHColumn;
import com.pingcap.ch.columns.CHColumnNullable;
import com.pingcap.ch.columns.CHColumnNumber;
import com.pingcap.ch.datatypes.CHTypeNumber.CHTypeUInt8;
import com.pingcap.common.ReadBuffer;

import java.io.IOException;

public class CHTypeNullable implements CHType {
    public final CHType nested_data_type;

    public CHTypeNullable(CHType nested_data_type) {
        this.nested_data_type = nested_data_type;
    }

    @Override
    public String name() {
        return "Nullable(" + nested_data_type.name() + ")";
    }

    @Override
    public CHColumn deserialize(ReadBuffer reader, int size) throws IOException {
        CHColumnNumber nullMap = (CHColumnNumber) CHTypeUInt8.instance.deserialize(reader, size);
        CHColumn nestedData = nested_data_type.deserialize(reader, size);
        return new CHColumnNullable(this, size, nullMap, nestedData);
    }
}
