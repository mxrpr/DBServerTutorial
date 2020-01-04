package com.mixer.raw.specific;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class to store indexed fields. When we search records, then we
 * search in this index object, and we can get the position of the records/rows in the database
 * Index object is final and singleton.
 */
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

    /**
     * Add a new row
     * @param bytePosition To which byte position we have added the new row in database
     */
    public synchronized void add(long bytePosition) {
        this.rowIndex.put(this.totalRowNumber, bytePosition);
        this.totalRowNumber++;
    }

    /**
     * Get the byte position of a given row from where we can read the object
     * @param rowNumber number of record
     * @return byte position in file
     */
    public long getBytePosition(long rowNumber) {
        return this.rowIndex.getOrDefault(rowNumber, -1L);
    }

    /**
     * Remove a record from the database
     *
     * @param row row number
     */
    private synchronized void remove(long row) {
        this.rowIndex.remove(row);
        this.totalRowNumber--;
        // remove also from the name index
        String nameToDelete = this.nameIndex.search(2, (k, v) -> v==row ? k : null);
        if (nameToDelete != null) {
            this.nameIndex.remove(nameToDelete);
        }
    }

    /**
     * Returns the total number of records in the database
     *
     * @return Total record number
     */
    public synchronized long getTotalNumberOfRows() {
        return this.totalRowNumber;
    }

    /**
     * Add a string to the index. This helps to do search in database. We store the indexed field as string
     * and store the row number.
     * @param name Indexed field
     * @param rowIndex Index of the row in which it has been stored
     */
    public void addNameToIndex(final String name, long rowIndex) {
        this.nameIndex.put(name, rowIndex);
    }

    /**
     * Check for an indexed field exist or not
     * @param name Indexed field value
     * @return contains or not.
     */
    public boolean hasNameInIndex(final String name) {
        return this.nameIndex.containsKey(name);
    }

    /**
     * Get the row number where the object with the specifix index key has been stored
     * @param name Indexed field value
     * @return Row number
     */
    public long getRowNumberByName(final String name) {
        return this.nameIndex.getOrDefault(name, -1L);
    }

    /**
     * Get the stored keys in the index.
     * @return Stored index keys
     */
    public Set<String> getNames() {
        return this.nameIndex.keySet();
    }

    /**
     * Clear up the index
     */
    public synchronized void clear() {
        this.totalRowNumber = 0;
        this.rowIndex.clear();
        this.nameIndex.clear();
    }

    /**
     * Clear a value from index by the file position of the record.
     *
     * @param position Byte position of the record in the file
     */
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
