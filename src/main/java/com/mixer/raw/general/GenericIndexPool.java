package com.mixer.raw.general;

import com.mixer.exceptions.DBException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * This class holds reference to all GenericIndex objects.
 * When a new table is opened, then a new Index is created and stored
 * in this class
 */
public final class GenericIndexPool {

    // tableName, index
    private final Map<String, GenericIndex> indexStore;

    public GenericIndexPool() {
        this.indexStore = Collections.synchronizedMap(new HashMap<>());
    }

    public GenericIndex createIndex(final String tableName, final Schema schema) throws DBException {
        GenericIndex _index = new GenericIndex(schema);
        this.indexStore.put(tableName, _index);

        return _index;
    }

    public void deleteIndex(final String tableName) {
        GenericIndex _index = this.indexStore.remove(tableName);
        _index.clear();
    }

    public void close() {
        this.indexStore.forEach((k, v) -> v.clear());
    }
}
