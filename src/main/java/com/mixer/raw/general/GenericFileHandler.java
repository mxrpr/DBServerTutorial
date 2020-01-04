package com.mixer.raw.general;


import com.mixer.dbserver.DBServer;
import com.mixer.exceptions.DBException;
import com.mixer.exceptions.DuplicateNameException;
import com.mixer.util.Leveinshtein;
import com.mixer.util.OperationUnit;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Class is responsible to handle the basic file operations.
 * Extends GenericBaseFileHandler operations with implementing the methods defined in  DBGeneric interface
 *
 * @see GenericBaseFileHandler
 */
class GenericFileHandler extends GenericBaseFileHandler {

    /**
     * Construct a new GenericFileHandler object
     * @param dbFileName Name of the database file
     * @param index Index object which has to be used during work
     * @see GenericIndex
     * @throws FileNotFoundException If the file cannot be opened, exception will be thrown
     */
    public GenericFileHandler(final String dbFileName, final GenericIndex index) throws FileNotFoundException {
        super(dbFileName, index);
    }

    /**
     * Construct a new GenericFileHandler object
     * @param randomAccessFile RandomAccessFile for the database
     * @param dbFileName Name of the database file
     * @param index Index object which has to be used during work
     * @see RandomAccessFile
     * @see GenericIndex
     */
    public GenericFileHandler(final RandomAccessFile randomAccessFile,
                              final String dbFileName,
                              final GenericIndex index) {
        super(randomAccessFile, dbFileName, index);
    }

    /**
     * Store object in the database
     *
     * @param object   Object to store
     * @param defragOperation Is a defragment operation?
     * @return OperationUnit
     * @see OperationUnit
     *
     * @throws DuplicateNameException
     * @throws DBException
     */
    public OperationUnit add(final Object object, boolean defragOperation) throws DuplicateNameException, DBException {


        OperationUnit ou = new OperationUnit();
        writeLock.lock();
        try {
            String _name = (String) object.getClass().getDeclaredField(this.indexByFieldName).get(object);

            if (this.index.hasInIndex(_name)) {
                throw new DuplicateNameException(String.format("Name '%s' already exists!", _name));
            }

            // seek to the end of the file
            long currentPositionToInsert = this.dbFile.length();
            this.dbFile.seek(currentPositionToInsert);

            /** we have to store the following data in case of a row:
                o isTemporary byte
                o isDeleted byte
                o record length : int
                o name length : int
                o name
                o address length : int
                o address
                o carplatenumber length
                o carplatenum
                o description length : int
                o description
            **/
            int recordLength = 0;
            for (Field field : this.schema.fields) {
                recordLength += getFieldLengthByType(field, object);
            }
            // is temporary
            if (defragOperation) {
                this.dbFile.writeBoolean(false);
            } else {
                this.dbFile.writeBoolean(true);
            }


            // it is deleted
            this.dbFile.writeBoolean(false);

            // record length
            this.dbFile.writeInt(recordLength);

            // write down the records
            for (Field field : this.schema.fields) {
                // get the value
                Object value = object.getClass().getDeclaredField(field.fieldName).get(object);
                if (value == null)
                    throw new DBException(field.fieldName + " is null. Cannot store it");
                if (field.fieldType.equals("String")) {
                    this.dbFile.writeInt(((String) value).length());
                    this.dbFile.write(((String) value).getBytes(StandardCharsets.UTF_8));

                } else if (field.fieldType.equals("int")) {
                    this.dbFile.writeInt((Integer) value);
                }
                // TODO implement other field types
            }


            ou.addedRowPosition = currentPositionToInsert;
            DBServer.LOGGER.info("[GenericFileHandler] Add person, position " + currentPositionToInsert);

            return ou;

        } catch (NoSuchFieldException | IllegalAccessException | IOException e) {
            e.printStackTrace();
            throw new DBException("Field related problems during add " + e.getLocalizedMessage());
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Read a given row
     *
     * @param rowNumber Number of row to read from table/db
     * @return The object is filled with data from table/db
     *
     * @throws DBException
     */
    public Object readRow(long rowNumber) throws DBException {
        DBServer.LOGGER.info("[GenericFileHandler] Read row: " + rowNumber);
        readLock.lock();
        try {
            long bytePosition = this.index.getBytePosition(rowNumber);
            if (bytePosition == -1) {
                return null;
            }

            byte[] row = this.readRawRecord(bytePosition);

            DataInputStream stream = new DataInputStream(new ByteArrayInputStream(row));

            DBServer.LOGGER.info("[GenericFileHandler] Read done");

            return this.readFromByteStream(stream, this.zClass);
        } catch (IOException ioe) {
            throw new DBException(ioe.getMessage());
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Delete a given row
     * @param rowNumber Number of row to delete
     * @return OperationUnit
     * @see OperationUnit
     *
     * @throws DBException
     */
    public OperationUnit deleteRow(long rowNumber) throws DBException {
        DBServer.LOGGER.info("[GenericFileHandler] Delete row: " + rowNumber);
        writeLock.lock();
        try {
            long bytePositionOfRecord = this.index.getBytePosition(rowNumber);
            if (bytePositionOfRecord == -1) {
                throw new DBException("Row does not exists in Index");
            }

            this.dbFile.seek(bytePositionOfRecord);
            // it is temporary
            this.dbFile.writeBoolean(true);
            this.dbFile.seek(bytePositionOfRecord + 1);
            this.dbFile.writeBoolean(true);

            // update the index
            this.index.remove(rowNumber);
            OperationUnit ou = new OperationUnit();
            ou.deletedRowPosition = bytePositionOfRecord;
            DBServer.LOGGER.info("[GenericFileHandler] Row deleted ");
            return ou;
        } catch (IOException ioe) {
            throw new DBException(ioe.getMessage());
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Update a given row
     *
     * @param rowNumber Number of row to update
     * @param object The object which contains the information to store
     * @return OperationUnit
     * @see OperationUnit
     *
     * @throws DuplicateNameException
     * @throws DBException
     */
    public OperationUnit update(long rowNumber, final Object object) throws DuplicateNameException, DBException {
        DBServer.LOGGER.info("[GenericFileHandler] Update row: " + rowNumber);
        writeLock.lock();
        try {
            OperationUnit deleteoperation = this.deleteRow(rowNumber);
            OperationUnit addoperation = this.add(object, false);
            OperationUnit operation = new OperationUnit();
            operation.deletedRowPosition = deleteoperation.deletedRowPosition;
            operation.addedRowPosition = addoperation.addedRowPosition;

            DBServer.LOGGER.info("[GenericFileHandler] Update row done");
            return operation;

        } finally {
            writeLock.unlock();
        }
    }

    public OperationUnit update(final String indexedFieldName, final Object object) throws
            DuplicateNameException, DBException {
        DBServer.LOGGER.info("[GenericFileHandler] Update row: " + indexedFieldName);
        writeLock.lock();
        try {
            long rowNumber = this.index.getRowNumberByIndex(indexedFieldName);
            if (rowNumber == -1) {
                return new OperationUnit();
            }

            return this.update(rowNumber, object);
        } finally {
            writeLock.unlock();
            DBServer.LOGGER.info("[GenericFileHandler] Update row done");
        }
    }

    /**
     * Search row by name (the table must be is indexed, and the search is performed in the
     * indexed value)
     * @param name String to search
     *
     * @return  The found object. Can be null.
     * @throws DBException
     */
    public Object search(String name) throws DBException {
        DBServer.LOGGER.info("[GenericFileHandler] Search by name: " + name);
        long rowNumber = this.index.getRowNumberByIndex(name);
        if (rowNumber == -1)
            return null;
        return this.readRow(rowNumber);
    }

    /**
     * Search in table/db with Leveinshtein algorithm
     *
     * @param indexedFieldName Name of the field which was used for indexing
     * @param tolerance  Tolerance of th algorithm
     * @return List of found objects
     * @throws DBException
     */
    public List<Object> searchWithLeveinshtein(String indexedFieldName, int tolerance) throws DBException {
        List<Object> result = new ArrayList<>();

        Set<String> names = this.index.getIndexedValues();
        List<String> goodNames = new ArrayList<>();
        for (String storedName : names) {
            if (Leveinshtein.leveinshteinDistance(storedName, indexedFieldName) <= tolerance)
                goodNames.add(storedName);
        }
        // now we have all the names, get the records
        for (String goodName : goodNames) {
            long rowIndex = this.index.getRowNumberByIndex(goodName);
            if (rowIndex != -1) {
                Object p = this.readRow(rowIndex);
                result.add(p);
            }
        }

        return result;
    }

    /**
     * Search with regular expression. The search is performed in the index by the indexed field
     *
     * @param regexp The regular expression
     * @return List of found objects.
     * @throws DBException
     */
    public List<Object> searchWithRegexp(String regexp) throws DBException {
        DBServer.LOGGER.info("[GenericFileHandler] Search with regexp");
        List<Object> result = new ArrayList<>();

        Set<String> names = this.index.getIndexedValues();
        List<String> goodNames = new ArrayList<>();
        for (String storedName : names) {
            if (storedName.matches(regexp))
                goodNames.add(storedName);
        }
        // now we have all the names, get the records
        for (String goodName : goodNames) {
            long rowIndex = this.index.getRowNumberByIndex(goodName);
            if (rowIndex != -1) {
                Object p = this.readRow(rowIndex);
                result.add(p);
            }
        }

        DBServer.LOGGER.info("[GenericFileHandler] Search with regexp, done");

        return result;
    }
}
