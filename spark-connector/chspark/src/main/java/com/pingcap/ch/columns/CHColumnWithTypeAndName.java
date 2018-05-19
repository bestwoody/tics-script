package com.pingcap.ch.columns;

import com.pingcap.ch.datatypes.CHType;

public class CHColumnWithTypeAndName {
    private CHColumn column;
    private CHType type;
    private String name;

    public CHColumnWithTypeAndName(CHColumn column, CHType type, String name) {
        this.column = column;
        this.type = type;
        this.name = name;
    }

    public CHColumn column() {
        return column;
    }

    public CHType dataType() {
        return type;
    }

    public String name() {
        return name;
    }
}
