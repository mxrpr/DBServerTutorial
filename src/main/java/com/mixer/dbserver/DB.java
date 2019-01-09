package com.mixer.dbserver;

import com.mixer.exceptions.DuplicateNameException;
import com.mixer.raw.Person;
import com.mixer.transaction.ITransaction;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface DB extends Closeable {

    void add(Person person) throws IOException, DuplicateNameException;

    void delete(long rowNumber) throws IOException;

    void update(long rowNumber, final Person person) throws IOException, DuplicateNameException;

    void update(String name, final Person person) throws IOException, DuplicateNameException;

    Person read(long rowNumber) throws IOException;

    Person search(final String name) throws IOException;

    List<Person> searchWithLeveinshtein(final String name, int tolerance) throws IOException;

    List<Person> searchWithRegexp(final String regexp) throws IOException;

    ITransaction beginTransaction();

    void commit() throws IOException;

    void rollback() throws IOException;

    long getTotalRecordNumber();

    void close() throws IOException;
}
