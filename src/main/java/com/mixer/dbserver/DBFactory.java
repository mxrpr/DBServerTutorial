package com.mixer.dbserver;

import com.mixer.exceptions.DBException;

import java.io.IOException;

/**
 * Helper class to easily create the two main types of databases.
 * We can have the "simple" specific database and a generic one, which can store
 * all kind of objects.
 * */
public final class DBFactory {

    /**
     * Get "simple" DB which can store very specific classes (Persons)
     * @param databaseName
     * @return Specific database
     * @see DB
     * @throws IOException
     */
    public static DB getSpecificDB(final String databaseName) throws IOException {
        return new DBServer(databaseName);
    }

    /**
     * Returns a new generic database which can store all type of objects
     *
     * @return Generic database
     * @see DBGeneric
     * @throws DBException
     */
    public static DBGeneric getGenericDB() throws DBException {
        return new DBGenericServer();
    }
}

