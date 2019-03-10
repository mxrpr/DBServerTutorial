package com.mixer.raw.general;

import com.mixer.exceptions.DBException;
import com.mixer.exceptions.DuplicateNameException;
import com.mixer.transaction.ITransaction;
import com.mixer.util.DebugRowInfo;

import java.io.IOException;
import java.util.List;

public interface Table {

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

    void close() throws DBException;

    String getTableName();

    String getTableVersion() throws DBException;

    List<DebugRowInfo> listAllRowsWithDebug() throws DBException;

    void defragmentDatabase() throws IOException, DuplicateNameException, DBException;
}
