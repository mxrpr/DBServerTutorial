package com.mixer.dbserver;

import com.mixer.exceptions.DBException;
import com.mixer.raw.general.Table;

import java.io.Closeable;
import java.io.IOException;

public interface DBGeneric extends Closeable {

    Table useTable(final String tableName,
                   final String schema,
                   final Class zclass) throws DBException;

    boolean dropCurrentTable() throws DBException;

    boolean dropTable(final String tableName) throws DBException;

    boolean tableExists(final String tableName);

    String exportTableToCSV(final String tableName, final String schema, final Class zclass ) throws DBException;

    String exportCurrentTableToSCV() throws DBException;

    // List<String> getAvailableTables();

    void close() throws IOException;
}
