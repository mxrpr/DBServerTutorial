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

	// Total number of rows
	private long totalRowNumber = 0;

	/**
	 * Constructor needs the shema, because it contains information about the object fields,
	 * indexed field. All these information is used to ctore/load data from/to database file
	 *  
	 * @param schema Scheme
	 * @throws DBException If the schema is null or is empty, then we cannot read/write
	 * object into file, so we have to stop createing the GenericIndex file.  
	 */
	public GenericIndex(final Schema schema) throws DBException {
		this.schema = schema;
		String indexBy = this.schema.indexBy;
		if (indexBy == null || indexBy.isEmpty()) {
			throw new DBException("No index field was set in schema!");
		}
		this.rowIndex = new ConcurrentHashMap<>();
		this.indexes = new ConcurrentHashMap<>();
	}

	/**
	 * store which row number is at which byte position
	 * 
	 * @param bytePosition Position of the row in the database file
	 */
	public synchronized void add(long bytePosition) {
		this.rowIndex.put(this.totalRowNumber, bytePosition);
		this.totalRowNumber++;
	}

	/**
	 * Returns the byte position of a specific row number
	 * 
	 * @param rowNumber Number of the row
	 * @return long, the byte position of the row inside the file
	 */
	public long getBytePosition(long rowNumber) {
		return this.rowIndex.getOrDefault(rowNumber, -1L);
	}

	/**
	 * Remove a specidifc row from the index 
	 * 
	 * @param row Row number
	 */
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

	/**
	 * Returns the total number of rows
	 * 
	 * @return long
	 */
	public synchronized long getTotalNumberOfRows() {
		return this.totalRowNumber;
	}

	/**
	 * Add value to the index, where the key is the indexed field's value and the value is the row number
	 * 
	 * @param indexedValue String, the indexed field's value
	 * @param rowIndex Number of the row which contains the value
	 */
	public void addIndexedValue(final String indexedValue, long rowIndex) {

		if (!this.indexes.containsKey(this.schema.indexBy)) {
			this.indexes.putIfAbsent(this.schema.indexBy, new ConcurrentHashMap<>());
		}
		ConcurrentHashMap<String, Long> _index = this.indexes.get(this.schema.indexBy);
		_index.put(indexedValue, rowIndex);
	}

	/**
	 * Check if the String in parameter is in the index
	 * 
	 * @param indexedValue String, value of an indexed fiels
	 * @return true if it can be found in the index
	 */
	public boolean hasInIndex(final String indexedValue) {
		ConcurrentHashMap<String, Long> _index = this.indexes.get(this.schema.indexBy);
		if (_index == null)
			return false;
		return _index.containsKey(indexedValue);
	}

	/**
	 * Returns the row number which is associated with the indexed field
	 *  
	 * @param indexedValue String value
	 * @return long, the row number. If the String is not in the index, then 
	 * this method will return with -1
	 */
	public long getRowNumberByIndex(final String indexedValue) {
		ConcurrentHashMap<String, Long> _index = this.indexes.get(this.schema.indexBy);
		return _index.getOrDefault(indexedValue, -1L);
	}

	/**
	 * Return keys of the index
	 * 
	 * @return Set of Strings
	 */
	public Set<String> getIndexedValues() {
		ConcurrentHashMap<String, Long> _index = this.indexes.get(this.schema.indexBy);
		if (_index == null)
			return new LinkedHashSet<>();

		return _index.keySet();
	}

	/**
	 * Clear everithing from the index. Method is called, when 
	 * the Index has to be closed
	 */
	public void clear() {
		this.totalRowNumber = 0;
		this.rowIndex.clear();
		this.indexes.clear();
	}

	/**
	 * Remove an entry from index by file position
	 * 
	 * @param position Byte position of the row 
	 */
	public void removeByFilePosition(long position) {
		if (this.rowIndex.isEmpty())
			return;
		long row = this.rowIndex.search(1, (k, v) -> v == position ? k : -1);
		if (row != -1) {
			this.remove(row);
		}
	}
}
