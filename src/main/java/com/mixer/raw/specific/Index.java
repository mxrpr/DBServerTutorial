package com.mixer.raw.specific;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class Index {

    private static Index index;
    // row number, byte position
    private final ConcurrentHashMap<Long, Long> rowIndex;

    // String name, row Number
    private final ConcurrentHashMap<String, Long> nameIndex;

    private long totalRowNumber = 0;

    private Index(){
        this.rowIndex = new ConcurrentHashMap<>();
        this.nameIndex = new ConcurrentHashMap<>();
    }

    public static Index getInstance() {
        synchronized (Index.class) {
            if (index == null) {
                index = new Index();
            }
        }
        return index;
    }

    public synchronized void add(long bytePosition) {
        this.rowIndex.put(this.totalRowNumber, bytePosition);
        this.totalRowNumber++;
    }

    public long getBytePosition(long rowNumber) {
        return this.rowIndex.getOrDefault(rowNumber, -1L);
    }

    private synchronized void remove(long row) {
        this.rowIndex.remove(row);
        this.totalRowNumber--;
        // remove also from the name index
        String nameToDelete = this.nameIndex.search(2, (k, v) -> v==row ? k : null);
        if (nameToDelete != null) {
            this.nameIndex.remove(nameToDelete);
        }
    }

    public synchronized long getTotalNumberOfRows() {
        return this.totalRowNumber;
    }

    public void addNameToIndex(final String name, long rowIndex) {
        this.nameIndex.put(name, rowIndex);
    }

    public boolean hasNameinIndex(final String name) {
        return this.nameIndex.containsKey(name);
    }

    public long getRowNumberByName(final String name) {
        return this.nameIndex.getOrDefault(name, -1L);
    }

    public Set<String> getNames() {
        return this.nameIndex.keySet();
    }

    public synchronized void clear() {
        this.totalRowNumber = 0;
        this.rowIndex.clear();
        this.nameIndex.clear();
    }

    public void removeByFilePosition(long position) {
        if (this.rowIndex.isEmpty())
            return;
//        long row = this.rowIndex.search(1, (k, v) -> v == position ? k : -1);
        Long row = this.rowIndex.search(1, (k,v) -> {
           if (v == position )
               return k;
           return null;
       });

        if (row != null) {
            this.remove(row);
        }
    }
}
