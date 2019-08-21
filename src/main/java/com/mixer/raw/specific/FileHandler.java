package com.mixer.raw.specific;

import com.mixer.exceptions.DuplicateNameException;
import com.mixer.raw.Person;
import com.mixer.util.Leveinshtein;
import com.mixer.util.OperationUnit;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class FileHandler extends BaseFileHandler {

    public FileHandler(final String dbFileName) throws FileNotFoundException {
        super(dbFileName);
    }

    public FileHandler(final RandomAccessFile randomAccessFile, final String dbFileName) {
        super(randomAccessFile, dbFileName);
    }


    public OperationUnit add(String name,
                       int age,
                       String address,
                       String carPlateNumber,
                       String description) throws IOException, DuplicateNameException {
        writeLock.lock();
        try {

            if (Index.getInstance().hasNameinIndex(name)) {
                throw new DuplicateNameException(String.format("Name '%s' already exists!", name));
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
            int length = 4 + // name length
                    name.length() +
                    4 + // age
                    4 + // address length
                    address.length() +
                    4 + // carplate length
                    carPlateNumber.length() +
                    4 + // description length;
                    description.length();

            // is temporary
            this.dbFile.writeBoolean(true);

            // it is deleted
            this.dbFile.writeBoolean(false);
            // record length
            this.dbFile.writeInt(length);

            // store the name
            this.dbFile.writeInt(name.length());
            this.dbFile.write(name.getBytes("UTF-8"));

            // store age
            this.dbFile.writeInt(age);

            // store the address
            this.dbFile.writeInt(address.length());
            this.dbFile.write(address.getBytes());

            // store the carplatenumber
            this.dbFile.writeInt(carPlateNumber.length());
            this.dbFile.write(carPlateNumber.getBytes());

            // store the description
            this.dbFile.writeInt(description.length());
            this.dbFile.write(description.getBytes());

//            Index.getInstance().add(currentPositionToInsert);
//            Index.getInstance().addNameToIndex(name, Index.getInstance().getTotalNumberOfRows() - 1);
            OperationUnit ou = new OperationUnit();
            ou.addedRowPosition = currentPositionToInsert;

            return ou;
        } finally {
            writeLock.unlock();
        }
    }

    public Person readRow(long rowNumber) throws IOException {
        readLock.lock();
        try {
            long bytePosition = Index.getInstance().getBytePosition(rowNumber);
            if (bytePosition == -1) {
                return null;
            }

            byte[] row = this.readRawRecord(bytePosition);

            DataInputStream stream = new DataInputStream(new ByteArrayInputStream(row));

            return this.readFromByteStream(stream);
        } finally {
            readLock.unlock();
        }
    }


    public OperationUnit deleteRow(long rowNumber) throws IOException {
        writeLock.lock();
        try {
            long bytePositionOfRecord = Index.getInstance().getBytePosition(rowNumber);
            if (bytePositionOfRecord == -1) {
                throw new IOException("Row does not exists in Index");
            }

            this.dbFile.seek(bytePositionOfRecord);
            // it is temporary
            this.dbFile.writeBoolean(true);
            this.dbFile.seek(bytePositionOfRecord + 1);
            this.dbFile.writeBoolean(true);

            // update the index
            //Index.getInstance().remove(rowNumber);
            OperationUnit ou = new OperationUnit();
            ou.deletedRowPosition = bytePositionOfRecord;

            return ou;

        } finally {
            writeLock.unlock();
        }
    }

    public OperationUnit update(long rowNumber, String name,
                                int age,
                                String address,
                                String carPlateNumber,
                                String description) throws IOException, DuplicateNameException {
        writeLock.lock();
        try {
            OperationUnit deleteoperation= this.deleteRow(rowNumber);
            OperationUnit addoperation = this.add(name, age, address, carPlateNumber, description);
            OperationUnit operation = new OperationUnit();
            operation.deletedRowPosition = deleteoperation.deletedRowPosition;
            operation.addedRowPosition = addoperation.addedRowPosition;
            operation.succesfullOperation = true;

            return operation;

        }finally {
            writeLock.unlock();
        }
    }

    public OperationUnit update(final String nameToModify, String name,
                       int age,
                       String address,
                       String carPlateNumber,
                       String description) throws IOException, DuplicateNameException
    {
        writeLock.lock();
        try {
            long rowNumber = Index.getInstance().getRowNumberByName(nameToModify);
            if (rowNumber == -1)
                return new OperationUnit();
            return this.update(rowNumber, name, age, address, carPlateNumber, description);
        }finally {
            writeLock.unlock();
        }
    }

    public Person search(String name) throws IOException {
        long rowNumber = Index.getInstance().getRowNumberByName(name);
        if (rowNumber == -1)
            return null;
        return this.readRow(rowNumber);
    }

    public List<Person> searchWithLeveinshtein(String name, int tolerance) throws IOException {
        List<Person> result = new ArrayList<>();

        Set<String> names = Index.getInstance().getNames();
        List<String> goodNames = new ArrayList<>();
        for (String storedName : names) {
            if (Leveinshtein.leveinshteinDistance(storedName, name) <= tolerance)
                goodNames.add(storedName);
        }
        // now we have all the names, get the records
        for (String goodName : goodNames) {
            long rowIndex = Index.getInstance().getRowNumberByName(goodName);
            if (rowIndex != -1) {
                Person p = this.readRow(rowIndex);
                result.add(p);
            }
        }

        return result;
    }

    public List<Person> searchWithRegexp(String regexp) throws IOException {
        List<Person> result = new ArrayList<>();

        Set<String> names = Index.getInstance().getNames();
        List<String> goodNames = new ArrayList<>();
        for (String storedName : names) {
            if (storedName.matches(regexp))
                goodNames.add(storedName);
        }
        // now we have all the names, get the records
        for (String goodName : goodNames) {
            long rowIndex = Index.getInstance().getRowNumberByName(goodName);
            if (rowIndex != -1) {
                Person p = this.readRow(rowIndex);
                result.add(p);
            }
        }

        return result;
    }
}
