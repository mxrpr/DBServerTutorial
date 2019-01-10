package com.mixer.raw.general;


import com.mixer.dbserver.DBServer;
import com.mixer.exceptions.DBException;
import com.mixer.exceptions.DuplicateNameException;
import com.mixer.util.Leveinshtein;
import com.mixer.util.OperationUnit;
import kotlin.text.Charsets;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class GenericFileHandler extends GenericBaseFileHandler {


    public GenericFileHandler(final String dbFileName) throws FileNotFoundException {
        super(dbFileName);
    }

    public GenericFileHandler(final RandomAccessFile randomAccessFile, final String dbFileName) {
        super(randomAccessFile, dbFileName);
    }


    public OperationUnit add(final Object object, boolean defragOperation) throws DuplicateNameException, DBException {


        OperationUnit ou = new OperationUnit();
        writeLock.lock();
        try {
            String _name = (String) object.getClass().getDeclaredField(this.indexByFieldName).get(object);

            if (GenericIndex.getInstance().hasInIndex(_name)) {
                throw new DuplicateNameException(String.format("Name '%s' already exists!", _name));
            }

            // seek to the end of the file
            long currentPositionToInsert = this.dbFile.length();
            this.dbFile.seek(currentPositionToInsert);

            // isTemporary byte
            // isDeleted byte
            // record length : int
            // name length : int
            // name
            // address length : int
            // address
            // carplatenumber length
            // carplatenum
            // description length : int
            // description

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
                    this.dbFile.write(((String) value).getBytes(Charsets.UTF_8));

                } else if (field.fieldType.equals("int")) {
                    this.dbFile.writeInt(((Integer) value).intValue());
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


    public Object readRow(long rowNumber) throws DBException {
        DBServer.LOGGER.info("[GenericFileHandler] Read row: " + rowNumber);
        readLock.lock();
        try {
            long bytePosition = GenericIndex.getInstance().getBytePosition(rowNumber);
            if (bytePosition == -1) {
                return null;
            }

            byte[] row = this.readRawRecord(bytePosition);

            DataInputStream stream = new DataInputStream(new ByteArrayInputStream(row));

            DBServer.LOGGER.info("[GenericFileHandler] Read done");

            return this.readFromByteStream(stream, this.zclass);
        } catch (IOException ioe) {
            throw new DBException(ioe.getMessage());
        } finally {
            readLock.unlock();
        }
    }


    public OperationUnit deleteRow(long rowNumber) throws DBException {
        DBServer.LOGGER.info("[GenericFileHandler] Delete row: " + rowNumber);
        writeLock.lock();
        try {
            long bytePositionOfRecord = GenericIndex.getInstance().getBytePosition(rowNumber);
            if (bytePositionOfRecord == -1) {
                throw new DBException("Row does not exists in Index");
            }

            this.dbFile.seek(bytePositionOfRecord);
            // it is temporary
            this.dbFile.writeBoolean(true);
            this.dbFile.seek(bytePositionOfRecord + 1);
            this.dbFile.writeBoolean(true);

            // update the index
            GenericIndex.getInstance().remove(rowNumber);
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

    public OperationUnit update(long rowNumber, final Object object) throws DuplicateNameException, DBException {
        DBServer.LOGGER.info("[GenericFileHandler] Update row: " + rowNumber);
        writeLock.lock();
        try {
            OperationUnit deleteoperation = this.deleteRow(rowNumber);
            OperationUnit addoperation = this.add(object, false);
            OperationUnit operation = new OperationUnit();
            operation.deletedRowPosition = deleteoperation.deletedRowPosition;
            operation.addedRowPosition = addoperation.addedRowPosition;
            operation.succesfullOperation = true;

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
            long rowNumber = GenericIndex.getInstance().getRowNumberByIndex(indexedFieldName);
            if (rowNumber == -1) {
                return new OperationUnit();
            }

            return this.update(rowNumber, object);
        } finally {
            writeLock.unlock();
            DBServer.LOGGER.info("[GenericFileHandler] Update row done");
        }
    }

    public Object search(String name) throws DBException {
        DBServer.LOGGER.info("[GenericFileHandler] Search by name: " + name);
        long rowNumber = GenericIndex.getInstance().getRowNumberByIndex(name);
        if (rowNumber == -1)
            return null;
        return this.readRow(rowNumber);
    }

    public List<Object> searchWithLeveinshtein(String indexedFieldName, int tolerance) throws DBException {
        List<Object> result = new ArrayList<>();

        Set<String> names = GenericIndex.getInstance().getIndexedValues();
        List<String> goodNames = new ArrayList<>();
        for (String storedName : names) {
            if (Leveinshtein.leveinshteinDistance(storedName, indexedFieldName) <= tolerance)
                goodNames.add(storedName);
        }
        // now we have all the names, get the records
        for (String goodName : goodNames) {
            long rowIndex = GenericIndex.getInstance().getRowNumberByIndex(goodName);
            if (rowIndex != -1) {
                Object p = this.readRow(rowIndex);
                result.add(p);
            }
        }

        return result;
    }

    public List<Object> searchWithRegexp(String regexp) throws DBException {
        DBServer.LOGGER.info("[GenericFileHandler] Search with regexp");
        List<Object> result = new ArrayList<>();

        Set<String> names = GenericIndex.getInstance().getIndexedValues();
        List<String> goodNames = new ArrayList<>();
        for (String storedName : names) {
            if (storedName.matches(regexp))
                goodNames.add(storedName);
        }
        // now we have all the names, get the records
        for (String goodName : goodNames) {
            long rowIndex = GenericIndex.getInstance().getRowNumberByIndex(goodName);
            if (rowIndex != -1) {
                Object p = this.readRow(rowIndex);
                result.add(p);
            }
        }

        DBServer.LOGGER.info("[GenericFileHandler] Search with regexp, done");

        return result;
    }
}
