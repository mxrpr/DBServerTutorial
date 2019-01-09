package com.mixer.dbserver;

import com.mixer.exceptions.DBException;

import java.io.IOException;

public final class DBFactory {

    public static DB getSpecificDB(final String databaseName) throws IOException {
        return new DBServer(databaseName);
    }

    public static DBGeneric getGenericDB(final String databaseName, final String schema, final Class zclass) throws DBException {
        return new DBGenericServer(databaseName, schema, zclass);
    }
}
