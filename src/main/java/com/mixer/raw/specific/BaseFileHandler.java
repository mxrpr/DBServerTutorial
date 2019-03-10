package com.mixer.raw.specific;

import com.mixer.raw.Person;
import com.mixer.util.DebugRowInfo;
import kotlin.text.Charsets;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BaseFileHandler {

    RandomAccessFile dbFile;
    private final String dbFileName;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    final Lock readLock = readWriteLock.readLock();
    final Lock writeLock = readWriteLock.writeLock();
    private final static int HEADER_INFO_SPACE = 100;


    BaseFileHandler(final String dbFileName) throws FileNotFoundException {
        this.dbFileName = dbFileName;
        this.dbFile = new RandomAccessFile(dbFileName, "rw");
    }

    BaseFileHandler(final RandomAccessFile randomAccessFile, final String dbFileName) {
        this.dbFileName = dbFileName;
        this.dbFile = randomAccessFile;
    }

    public void initialise() throws IOException {
        if (this.dbFile.length() == 0) {
            this.setTableVersion();
        }
        else{
            String dbVersion = this.getTableVersion();
            System.out.println("DB version: " + dbVersion);
        }
    }

    public void loadAllDataToIndex() throws IOException {
        readLock.lock();
        try {
            if (this.dbFile.length() == 0)
                return;

            long currentPos = HEADER_INFO_SPACE;
            long rowNum = 0;
            long deletedRows = 0;
            long temporaryRows = 0;

            synchronized (this) {
                while (currentPos < this.dbFile.length()) {
                    this.dbFile.seek(currentPos);
                    boolean isTemporary = this.dbFile.readBoolean();
                    if (isTemporary)
                        temporaryRows += 1;

                    currentPos += 1;
                    this.dbFile.seek(currentPos);
                    boolean isDeleted = this.dbFile.readBoolean();

                    if (!isDeleted) {
                        Index.getInstance().add(currentPos-1);
                    } else
                        deletedRows++;

                    currentPos += 1;
                    this.dbFile.seek(currentPos);
                    int recordLength = this.dbFile.readInt();

                    currentPos += 4;
                    this.dbFile.seek(currentPos);
                    if (!isDeleted && !isTemporary) {
                        byte[] b = new byte[recordLength];
                        this.dbFile.read(b);
                        Person p = this.readFromByteStream(new DataInputStream(new ByteArrayInputStream(b)));
                        Index.getInstance().addNameToIndex(p.pname, rowNum);
                        rowNum++;
                    }
                    currentPos += recordLength;
                }
            }

            System.out.println("After startup: total row number in Database: " + rowNum);
            System.out.println("After startup: total deleted row number in Database: " + deletedRows);
            System.out.println("After startup: total temporary row number in Database: " + temporaryRows);
        } finally {
            readLock.unlock();
        }
    }

    Person readFromByteStream(final DataInputStream stream) throws IOException {
        Person person = new Person();

        int nameLength = stream.readInt();
        byte[] b = new byte[nameLength];
        stream.read(b);
        person.pname = new String(b, "UTF-8");

        // age
        person.age = stream.readInt();

        // address
        b = new byte[stream.readInt()];
        stream.read(b);
        person.address = new String(b);

        // carplatenum
        b = new byte[stream.readInt()];
        stream.read(b);
        person.carplatenumber = new String(b);


        // description
        b = new byte[stream.readInt()];
        stream.read(b);
        person.description = new String(b);

        return person;
    }

    byte[] readRawRecord(long bytePositionOfRow) throws IOException {
        readLock.lock();
        try {
            synchronized (this) {
                this.dbFile.seek(bytePositionOfRow);
                this.dbFile.readBoolean();

                this.dbFile.seek(bytePositionOfRow + 1);
                if (this.dbFile.readBoolean())
                    return new byte[0];

                this.dbFile.seek(bytePositionOfRow + 2);
                int recordLength = this.dbFile.readInt();

                this.dbFile.seek(bytePositionOfRow + 6);

                byte[] data = new byte[recordLength];
                this.dbFile.read(data);

                return data;
            }

        } finally {
            readLock.unlock();
        }
    }

    public void close() throws IOException {
        this.dbFile.close();
    }


    public List<DebugRowInfo> loadAllDataFromFile() throws IOException {
        readLock.lock();
        try {

            if (this.dbFile.length() == 0) {
                return new ArrayList<>();
            }
            synchronized (this) {
                ArrayList<DebugRowInfo> result = new ArrayList<>();
                long currentPosition = HEADER_INFO_SPACE;

                while (currentPosition < this.dbFile.length()) {
                    this.dbFile.seek(currentPosition);
                    boolean isTemporary = this.dbFile.readBoolean();

                    currentPosition += 1;
                    this.dbFile.seek(currentPosition);
                    boolean isDeleted = this.dbFile.readBoolean();

                    currentPosition += 1;
                    this.dbFile.seek(currentPosition);
                    int recordLength = this.dbFile.readInt();
                    currentPosition += 4;

                    byte[] b = new byte[recordLength];
                    this.dbFile.read(b);
                    Person p = this.readFromByteStream(new DataInputStream(new ByteArrayInputStream(b)));
                    result.add(new DebugRowInfo(p, isDeleted, isTemporary));
                    currentPosition += recordLength;
                }

                return result;
            }
        } finally {
            readLock.unlock();
        }
    }

    public boolean deleteFile() throws IOException {
        writeLock.lock();
        try {
            this.dbFile.close();
            if (new File(this.dbFileName).delete()) {
                System.out.println("File has been deleted");
                return true;
            } else {
                System.out.println("File has been NOT deleted");
                return false;
            }
        } finally {
            writeLock.unlock();
        }
    }

    public String getTableName() {
        return this.dbFileName;
    }

    public void commit(List<Long> newRows, List<Long> deletedRows) throws IOException {
        writeLock.lock();
        try {
            for (long position : newRows) {
                this.dbFile.seek(position);
                this.dbFile.writeBoolean(false); // it is not temporary
                // re-read the record
                byte[] b = this.readRawRecord(position);
                Person person = this.readFromByteStream(new DataInputStream(new ByteArrayInputStream(b)));
                // add it to the index
                Index.getInstance().addNameToIndex(person.pname, Index.getInstance().getTotalNumberOfRows());
                Index.getInstance().add(position);
            }

            // operate on deleted rows
            for (long position : deletedRows) {
                this.dbFile.seek(position);
                this.dbFile.writeBoolean(false);
                Index.getInstance().removeByFilePosition(position);
            }
        }finally {
            writeLock.unlock();
        }
    }

    public void rollback(List<Long> newRows, List<Long> deletedRows) throws IOException {
        writeLock.lock();
        try {
            for (long position : newRows) {
                this.dbFile.seek(position);
                // not temporary
                this.dbFile.writeBoolean(false);

                // deleted
                this.dbFile.seek(position+1);
                this.dbFile.writeBoolean(true);
                Index.getInstance().removeByFilePosition(position);
            }

            for(long position : deletedRows) {
                this.dbFile.seek(position);
                // not temporary
                this.dbFile.writeBoolean(false);

                // isdeleted
                this.dbFile.seek(position + 1);
                this.dbFile.writeBoolean(false);
                // re-read the record
                byte[] b = this.readRawRecord(position);
                Person person = this.readFromByteStream(new DataInputStream(new ByteArrayInputStream(b)));
                // add it to the index
                Index.getInstance().addNameToIndex(person.pname, Index.getInstance().getTotalNumberOfRows());
                Index.getInstance().add(position);
            }
        }finally {
            writeLock.unlock();
        }
    }

    private void setTableVersion() throws IOException {
        this.dbFile.seek(0);
        String VERSION = "0.1";
        this.dbFile.write(VERSION.getBytes(Charsets.UTF_8));
        char[] chars = new char[HEADER_INFO_SPACE - VERSION.length()];
        Arrays.fill(chars, ' ');
        this.dbFile.write(new String(chars).getBytes());
    }

    private String getTableVersion() throws IOException {
        readLock.lock();
        try {
            this.dbFile.seek(0);
            byte[] b = new byte[HEADER_INFO_SPACE];
            this.dbFile.read(b);

            return new String(b, "UTF-8").trim();
        }finally {
            readLock.unlock();
        }
    }
}
