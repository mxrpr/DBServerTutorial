package com.mixer.dbserver;

import com.mixer.exceptions.DuplicateNameException;
import com.mixer.raw.Person;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Interface for a simple and very specific Database server.
 * The implementation of this interface can store only Person objects.
 */
public interface DB extends Closeable {

    /**
     * Add a new record to database.
     * @param person Person object
     * @see Person
     * @throws IOException
     * @throws DuplicateNameException
     */
    void add(Person person) throws IOException, DuplicateNameException;

    /**
     * Delete a given row from database
     * @param rowNumber Row number
     * @throws IOException
     */
    void delete(long rowNumber) throws IOException;

    /**
     * Update a given row in the database with a new Person object
     * @param rowNumber Number of row to update
     * @param person Person object
     * @see Person
     *
     * @throws IOException
     * @throws DuplicateNameException
     */
    void update(long rowNumber, final Person person) throws IOException, DuplicateNameException;

    /**
     * Update a given row in the database with a new Person object
     * @param name Name of the person - which must be unique in the database
     * @param person Person object
     * @see Person
     *
     * @throws IOException
     * @throws DuplicateNameException
     */
    void update(String name, final Person person) throws IOException, DuplicateNameException;

    /**
     * Read a given row from the database
     * @param rowNumber Number of row to be read
     *
     * @return Person object. The values stored in the row are read, and a new person object is created
     * @throws IOException
     */
    Person read(long rowNumber) throws IOException;

    /**
     * Search a given row by the name field.
     * @param name search value
     *
     * @return Person object. The values are searched in the index, then
     * the system know where the row is in the file. Then the values in the stored row are read, and
     * a new person object is created
     * @throws IOException
     */
    Person search(final String name) throws IOException;

    /**
     * Search a given row by the name field, using the Leveinshtein algorithm
     * @param name search value
     * @param tolerance See the Leveinshtein algorithm tutorial
     *
     * @return List of Person objects. The values are searched in the index, then
     * the system know where the row is in the file. Then the values in the stored row are read, and
     * a new person object is created. After all the found person objects has been created, the list will be returned.
     * @throws IOException
     */
    List<Person> searchWithLeveinshtein(final String name, int tolerance) throws IOException;

    /**
     * Search a given row by the name field, using regular expression
     * @param regexp search value
     * @return List of Person objects. The values are searched in the index, then
     * the system know where the row is in the file. Then the values in the stored row are read, and
     * a new person object is created. After all the found person objects has been created, the list will be returned.
     * @throws IOException
     */
    List<Person> searchWithRegexp(final String regexp) throws IOException;

    /**
     * Begin a new transaction
     */
    void beginTransaction();

    /**
     * Commit the changes in the database, make them final
     * @throws IOException
     */
    void commit() throws IOException;

    /**
     * Rollback the changes in the database
     * @throws IOException
     */
    void rollback() throws IOException;

    /**
     * Get total number of records/rows in the database
     * @return
     */
    long getTotalRecordNumber();

    /**
     * Close the database
     * @see Closeable
     *
     * @throws IOException
     */
    void close() throws IOException;
}
