package com.mixer.dbserver;

import com.mixer.exceptions.DBException;
import com.mixer.raw.general.GenericIndexPool;
import com.mixer.raw.general.ICSVRepresentation;
import com.mixer.raw.general.MxrTable;
import com.mixer.raw.general.Table;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class DBGenericServer implements DBGeneric {
	// table name, reference to table instance
	private Map<String, Table> tablePool;
	private Table currentlyUsedTable = null;
	private GenericIndexPool indexPool = null;

	public static final Logger LOGGER = Logger.getLogger("DBServer");
	private static final String PROPERTY_FILE_NAME = "config.properties";
	private static final String LOG_LEVEL = "LOG_LEVEL";

	public DBGenericServer() throws DBException {
		this.indexPool = new GenericIndexPool();
		this.tablePool = new HashMap<>();
		try {
			this.initialise();
		} catch (IOException e) {
			throw new DBException(e.getMessage());
		}
	}

	private void initialise() throws IOException {

		Properties properties = new Properties();
		try (FileInputStream fis = new FileInputStream(PROPERTY_FILE_NAME)) {
			properties.load(fis);
		}

		boolean hasLogLevel = properties.containsKey(LOG_LEVEL);
		if (!hasLogLevel)
			LOGGER.setLevel(Level.SEVERE);
		else {
			String logLevel = (String) properties.get(LOG_LEVEL);
			if (logLevel.equalsIgnoreCase("SEVERE"))
				LOGGER.setLevel(Level.SEVERE);
			else if (logLevel.equalsIgnoreCase("INFO"))
				LOGGER.setLevel(Level.INFO);
			else if (logLevel.equalsIgnoreCase("ALL"))
				LOGGER.setLevel(Level.ALL);
		}
	}

	@Override
	public void close() throws IOException {
		// cleanup the tablepool and close the tables
		Set<String> _tableNames = this.tablePool.keySet();
		try {
			for (String tableName : _tableNames) {
				this.tablePool.get(tableName).close();
			}
		} catch (DBException dbe) {
			throw new IOException(dbe.getMessage());
		}
		// cleanup the indexpool and close the indexes
		this.indexPool.close();
	}

	@Override
	public Table useTable(final String tableName, final String schema, final Class zclass) throws DBException {

		// if the table is already loaded, then use it
		if (this.tablePool.containsKey(tableName)) {
			this.currentlyUsedTable = this.tablePool.get(tableName);
		} else {
			Table _table = new MxrTable(tableName, schema, zclass, this.indexPool);
			this.tablePool.put(tableName, _table);
			this.currentlyUsedTable = _table;
		}

		return this.currentlyUsedTable;
	}

	@Override
	public boolean dropCurrentTable() throws DBException {
		String _tableName = this.currentlyUsedTable.getTableName();
		return this.dropTable(_tableName);
	}

	@Override
	public boolean dropTable(final String tableName) throws DBException {
		this.currentlyUsedTable.close();
		this.tablePool.remove(tableName);
		this.indexPool.deleteIndex(tableName);
		this.currentlyUsedTable = null;

		File file = new File((tableName));
		if (!file.exists()) {
			throw new DBException("Cannot delete table " + tableName);
		}
		return file.delete();
	}

	@Override
	public boolean tableExists(final String tableName) {
		return new File(tableName).exists();
	}

	@Override
	public String exportTableToCSV(final String tableName, final String schema, final Class zclass) throws DBException {
		Table _table = this.useTable(tableName, schema, zclass);
		List<Object> result = _table.searchWithRegexp(".*");
		StringBuilder stringBuilder = new StringBuilder(600);
		for (Object obj : result) {
			stringBuilder.append(((ICSVRepresentation) obj).toSCV());
		}

		return stringBuilder.toString();
	}

	@Override
	public String exportCurrentTableToSCV() throws DBException {
		List<Object> result = this.currentlyUsedTable.searchWithRegexp(".*");
		StringBuilder stringBuilder = new StringBuilder(600);
		for (Object obj : result) {
			stringBuilder.append(((ICSVRepresentation) obj).toSCV());
		}

		return stringBuilder.toString();

	}

	@Override
	public void runQuery(final String queryString) throws DBException {
		if (this.currentlyUsedTable == null) {
			throw new DBException("No table is in use. Select a table!");
		}
		this.currentlyUsedTable.runQuery(queryString);
	}

}
