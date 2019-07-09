package com.mixer.query.sql;

public final class DBEntry {
    public Object object;
    public long rowIndex;

    public DBEntry(final Object object, final long rowIndex) {
        this.object = object;
        this.rowIndex = rowIndex;
    }
}
