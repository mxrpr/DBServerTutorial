package com.mixer.raw.general;

import com.mixer.exceptions.DBException;
import com.mixer.exceptions.DuplicateNameException;
import com.mixer.query.sql.ResultSet;
import com.mixer.transaction.ITransaction;
import com.mixer.util.DebugRowInfo;

import java.io.IOException;
import java.util.List;

public interface Table {
	
	/**
	 * Store object
	 * @param object Object
	 * @throws DuplicateNameException If a value with the same indexed field exists,
	 * then DuplicateNameException is thrown
	 * @throws DBException If there is an error during meanwhile we try to save the
	 * object, a DBException is thrown
	 */
    void add(Object object) throws DuplicateNameException, DBException;

    /**
     * Delete object by rownNumber
     * 
     * @param rowNumber Number of row we would like to delete
     * @throws DBException If there is an error during meanwhile we try to delete the
	 * row, a DBException is thrown
     */
    void delete(long rowNumber) throws DBException;
    
    /**
     * Update object by rownNumber
     * 
     * @param rowNumber Number of row we would like to delete
     * @param object The object which contains the new values
     * 
     * @throws DBException If there is an error during meanwhile we try to update the
	 * row, a DBException is thrown
     */
    void update(long rowNumber, final Object object) throws  DuplicateNameException, DBException;
    
    /**
     * Update object by index field value
     * 
     * @param indexedFieldName Value of the indexed field
     * @param object The object which contains the new values
     * 
     * @throws DBException If there is an error during meanwhile we try to update the
	 * row, a DBException is thrown
     */
    void update(String indexedFieldName, final Object object) throws  DuplicateNameException, DBException;

    
    /**
     * Read object from a specific row
     * 
     * @param rowNumber Number of the row we would like to read
     * @return object The read object
     * 
     * @throws DBException If there is an error during meanwhile we try to read the
	 * row, a DBException is thrown
     */
    Object read(long rowNumber) throws DBException;

    /**
     * Search object by a specific value
     * 
     * @param indexedFieldName Value of the indexed field
     * @return object The found object
     * 
     * @throws DBException If there is an error during meanwhile we try to search the
	 * row, a DBException is thrown
     */
    Object search(final String indexedFieldName) throws DBException;

    /**
     * Search object by a specific value
     * 
     * @param indexedFieldName Value of the indexed field
     * @param tolerance int, the tolerance value (check the algorithm)
     * @return List of found objects
     * 
     * @throws DBException If there is an error during meanwhile we try to search the
	 * row, a DBException is thrown
     */
    List<Object> searchWithLeveinshtein(final String indexedFieldName, int tolerance) throws DBException;

    /**
     * Search object by regular expression
     * 
     * @param regexp The regular expression
     * @return List of found objects
     * 
     * @throws DBException If there is an error during meanwhile we try to search the
	 * row, a DBException is thrown
     */
    List<Object> searchWithRegexp(final String regexp) throws DBException;

    /**
     * Begin transaction
     * 
     * @return Returns a new Transaction object
     */
    ITransaction beginTransaction();

    /**
     * Commits the transaction associated with the thread
     * 
     * @throws DBException If there is an error during during commit, a DBException is thrown
     */    
    void commit() throws DBException;

    /**
     * Rolls back the transaction associated with the thread
     * 
     * @throws DBException If there is an error during mrollback a DBException is thrown
     */    
    void rollback() throws DBException;

    /**
     * Returns the total number of records in the table
     * 
     * @return int
     */
    long getTotalRecordNumber();

    /**
     * Close the table
     * 
     * @throws DBException If there is an error during close, a DBException is thrown
     */    
    void close() throws DBException;
    
    /**
     * Returns the name of the table
     * 
     * @return Table name
     */
    String getTableName();

    /**
     * Returns the table version
     * 
     * @return Table version
     */
    String getTableVersion() throws DBException;

    /**
     * For debugging purposes
     * 
     * @return List of rowns with debug information
     */
    List<DebugRowInfo> listAllRowsWithDebug() throws DBException;

    /**
     * Defragment the database
     * 
     * @throws IOException If there is an error during defragmeented, a IOException is thrown
     * @throws DuplicateNameException If there is an error during defragmeented, a DuplicateNameException is thrown
     * @throws DBException If there is an error during defragmeented, a DBException is thrown
     */
    void defragmentDatabase() throws IOException, DuplicateNameException, DBException;
    
    /**
     * Run SQL query
     * 
     * @param query SQL query string
     *
     * @return ResultSet
     */
    ResultSet runQuery(final String query) throws DBException;
}
