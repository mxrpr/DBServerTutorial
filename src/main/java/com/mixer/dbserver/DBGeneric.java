package com.mixer.dbserver;

import com.mixer.exceptions.DBException;
import com.mixer.exceptions.DuplicateNameException;
import com.mixer.transaction.ITransaction;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface DBGeneric extends Closeable {

    void add(Object object) throws DuplicateNameException, DBException;

    void delete(long rowNumber) throws DBException;

    void update(long rowNumber, final Object object) throws  DuplicateNameException, DBException;

    void update(String indexedFieldName, final Object object) throws  DuplicateNameException, DBException;

    Object read(long rowNumber) throws DBException;

    Object search(final String indexedFieldName) throws DBException;

    List<Object> searchWithLeveinshtein(final String indexedFieldName, int tolerance) throws DBException;

    List<Object> searchWithRegexp(final String regexp) throws DBException;

    ITransaction beginTransaction();

    void commit() throws DBException;

    void rollback() throws DBException;

    long getTotalRecordNumber();

    void close() throws IOException;
}
