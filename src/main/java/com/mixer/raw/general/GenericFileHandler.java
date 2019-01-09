package com.mixer.raw.general;

import com.mixer.dbserver.DB;
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
        writeLock.lock();
        OperationUnit ou = new OperationUnit();
        try {
            String _indexBy = this.schema.indexBy;
            if (_indexBy == null) {
                throw new DBException("indexBy is missing from the Schema");
            }

            String _name = (String)object.getClass().getDeclaredField(_indexBy).get(object);

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

            // calculate record length
            int recordLength = 0;

            for (Field field : this.schema.fields) {
                recordLength += getFieldLengthByType(field, object);
            }

            // is temporary
            if (defragOperation) {
                this.dbFile.writeBoolean(false);
            }
            else {
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

            return ou;

        } catch (NoSuchFieldException | IllegalAccessException | IOException e) {
            e.printStackTrace();
            throw new DBException("Field related problems during add " + e.getLocalizedMessage());
        } finally {
            writeLock.unlock();
        }
    }

    private int getFieldLengthByType(Field field, Object object) throws NoSuchFieldException, IllegalAccessException {
        int result = 0;

        switch (field.fieldType) {
            case "String": {
                Object value = object.getClass().getDeclaredField(field.fieldName).get(object);
                result += ((String) value).length();
                result += 4;
                break;
            }
            case "int": {
                result = 4;
                break;
            }
            case "long": {
                result += 4;
                break;
            }
            default: {
                throw new NoSuchFieldException(field.fieldType);
            }
            // TODO add more types
        }

        return result;
    }

    public Object readRow(long rowNumber) throws DBException {
        readLock.lock();
        try {
            long bytePosition = GenericIndex.getInstance().getBytePosition(rowNumber);
            if (bytePosition == -1) {
                return null;
            }

            byte[] row = this.readRawRecord(bytePosition);

            DataInputStream stream = new DataInputStream(new ByteArrayInputStream(row));

            return this.readFromByteStream(stream, this.zclass);
        }catch(IOException ioe) {
            throw new DBException(ioe.getMessage());
        } finally {
            readLock.unlock();
        }
    }


    public OperationUnit deleteRow(long rowNumber) throws DBException {
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

            return ou;
        }catch(IOException ioe) {
            throw new DBException(ioe.getMessage());
        } finally {
            writeLock.unlock();
        }
    }

    public OperationUnit update(long rowNumber, final Object object) throws DuplicateNameException, DBException {
        writeLock.lock();
        try {
            OperationUnit deleteoperation = this.deleteRow(rowNumber);
            OperationUnit addoperation = this.add(object, false);
            OperationUnit operation = new OperationUnit();
            operation.deletedRowPosition = deleteoperation.deletedRowPosition;
            operation.addedRowPosition = addoperation.addedRowPosition;
            operation.succesfullOperation = true;

            return operation;

        } finally {
            writeLock.unlock();
        }
    }

    public OperationUnit update(final String indexedFieldName, final Object object) throws
            DuplicateNameException, DBException {
        writeLock.lock();
        try {
            long rowNumber = GenericIndex.getInstance().getRowNumberByIndex(indexedFieldName);
            if (rowNumber == -1)
                return new OperationUnit();
            return this.update(rowNumber, object);
        } finally {
            writeLock.unlock();
        }
    }

    public Object search(String name) throws DBException {
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

        return result;
    }
}
