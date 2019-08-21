package com.mixer.query.sql;

/**
 * This class represents an entry in the database - a row
 */
public final class DBEntry {
    // the stored object
    public Object object;

    // index of the object
    public long rowIndex;

    public DBEntry(final Object object, final long rowIndex) {
        this.object = object;
        this.rowIndex = rowIndex;
    }
}
