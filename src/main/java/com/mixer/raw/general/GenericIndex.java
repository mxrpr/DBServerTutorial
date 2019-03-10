package com.mixer.raw.general;


import com.mixer.exceptions.DBException;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class GenericIndex {

    private Schema schema;

    // row number, byte position
    private final ConcurrentHashMap<Long, Long> rowIndex;

    // String name of the index, String value, row Number
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Long>> indexes;

    private long totalRowNumber = 0;

    public GenericIndex(final Schema schema) throws DBException {
        this.schema = schema;
        String indexBy = this.schema.indexBy;
        if (indexBy == null || indexBy.isEmpty()) {
            throw new DBException("No index field was set in schema!");
        }
        this.rowIndex = new ConcurrentHashMap<>();
        this.indexes = new ConcurrentHashMap<>();
    }

    public synchronized void add(long bytePosition) {
        this.rowIndex.put(this.totalRowNumber, bytePosition);
        this.totalRowNumber++;
    }

    public long getBytePosition(long rowNumber) {
        return this.rowIndex.getOrDefault(rowNumber, -1L);
    }

    public synchronized void remove(long row) {
        this.rowIndex.remove(row);
        this.totalRowNumber--;
        // remove also from the indexes
        ConcurrentHashMap<String, Long> _index = this.indexes.get(this.schema.indexBy);

        String nameToDelete = _index.search(2, (k, v) -> v == row ? k : null);
        if (nameToDelete != null) {
            _index.remove(nameToDelete);
        }
    }

    public synchronized long getTotalNumberOfRows() {
        return this.totalRowNumber;
    }

    public void addIndexedValue(final String indexedValue, long rowIndex) {

        if (!this.indexes.containsKey(this.schema.indexBy)) {
            this.indexes.putIfAbsent(this.schema.indexBy, new ConcurrentHashMap<>());
        }
        ConcurrentHashMap<String, Long> _index = this.indexes.get(this.schema.indexBy);
        _index.put(indexedValue, rowIndex);
    }

    public boolean hasInIndex(final String indexedValue) {
        ConcurrentHashMap<String, Long> _index = this.indexes.get(this.schema.indexBy);
        if (_index == null)
            return false;
        return _index.containsKey(indexedValue);
    }

    public long getRowNumberByIndex(final String indexedValue) {
        ConcurrentHashMap<String, Long> _index = this.indexes.get(this.schema.indexBy);
        return _index.getOrDefault(indexedValue, -1L);
    }

    public Set<String> getIndexedValues() {
        ConcurrentHashMap<String, Long> _index = this.indexes.get(this.schema.indexBy);
        if (_index == null)
            return new LinkedHashSet<>();

        return _index.keySet();
    }

    public void clear() {
        this.totalRowNumber = 0;
        this.rowIndex.clear();
        this.indexes.clear();
    }

    public void removeByFilePosition(long position) {
        if (this.rowIndex.isEmpty())
            return;
        long row = this.rowIndex.search(1, (k, v) -> v == position ? k : -1);
        if (row != -1) {
            this.remove(row);
        }
    }

//    public void initialize(final Schema schema) throws DBException {
//        this.schema = schema;
//        String indexBy = this.schema.indexBy;
//        if (indexBy == null) {
//            throw new DBException("No indexBy field was set in schema!");
//        }
//    }
}
