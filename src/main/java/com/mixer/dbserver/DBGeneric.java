package com.mixer.dbserver;

import com.mixer.exceptions.DBException;
import com.mixer.raw.general.Table;

import java.io.Closeable;
import java.io.IOException;

public interface DBGeneric extends Closeable {
	
	/**
	 * Use the specified table with the given schema
	 * 
	 * @param tableName Name of the table
	 * @param schema Schema information in JSON
	 * @param zclass Class of the stored object
	 * @return Table
	 * 
	 * @throws DBException In case of any error, the API throws DBException
	 */
    Table useTable(final String tableName,
                   final String schema,
                   final Class zclass) throws DBException;

    /**
     * Drop the currently used table
     * 
     * @return true if the deletion of the table is successfull
     * 
     * @throws DBException In case of any error, the API throws DBException
     */
    boolean dropCurrentTable() throws DBException;
    
    /**
     * Drop table
     * 
     * @param tableName
     * @return true if the deletion of the table is successfull
     * 
     * @throws DBException In case of any error, the API throws DBException
     */

    boolean dropTable(final String tableName) throws DBException;

    /**
     * Check whether a given table exists or not
     * 
     * @return true if the the table exists
     * 
     * @throws DBException In case of any error, the API throws DBException
     */

    boolean tableExists(final String tableName);

    /**
     * Export given table to csv file
     * 
     * @param tableName Name of the table
     * @param schema SChema in JSON format
     * @param zclass Class of the stored object
     * 
     * @return String The generated string
     * 
     * @throws DBException  In case of any error, the API throws DBException
     */
    String exportTableToCSV(final String tableName, final String schema, final Class zclass ) throws DBException;

    /**
     * Export currrently used table to csv file
     * 
     * @return String The generated string
     * 
     * @throws DBException  In case of any error, the API throws DBException
     */    
    String exportCurrentTableToSCV() throws DBException;

    // List<String> getAvailableTables();

    /**
     * Close the database
     * 
     * @throws DBException  In case of any error, the API throws DBException
     */
    void close() throws IOException;
    
    /**
     * Runs a query against our database
     * 
     * @param queryString SQL query
     * 
     * @throws DBException  In case of any error, the API throws DBException
     */
    void runQuery(final String queryString) throws DBException;
}
