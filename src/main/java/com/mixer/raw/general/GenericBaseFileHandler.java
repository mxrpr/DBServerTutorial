package com.mixer.raw.general;

import com.mixer.dbserver.DBServer;
import com.mixer.exceptions.DBException;
import com.mixer.util.DebugRowInfo;
import kotlin.text.Charsets;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class GenericBaseFileHandler {

    RandomAccessFile dbFile;
    private final String dbFileName;
    protected Schema schema;
    protected Class zclass;
    protected String indexByFieldName;

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    final Lock readLock = readWriteLock.readLock();
    final Lock writeLock = readWriteLock.writeLock();
    private final static int HEADER_INFO_SPACE = 100;


    GenericBaseFileHandler(final String dbFileName) throws FileNotFoundException {
        this.dbFileName = dbFileName;
        this.dbFile = new RandomAccessFile(dbFileName, "rw");
    }

    GenericBaseFileHandler(final RandomAccessFile randomAccessFile, final String dbFileName) {
        this.dbFileName = dbFileName;
        this.dbFile = randomAccessFile;
    }

    public void initialise() throws IOException {
        DBServer.LOGGER.info("[GenericBaseFileHandler] Initialise");
        if (this.dbFile.length() == 0) {
            this.setDBVersion();
        } else {
            String dbVersion = this.getDBVersion();
            System.out.println("DB version: " + dbVersion);
        }
        DBServer.LOGGER.info("[GenericBaseFileHandler] Initialisation done");
    }

    public void setSchema(final Schema schema) throws DBException {
        DBServer.LOGGER.info("[GenericBaseFileHandler] Set schema");
        this.schema = schema;
        if (this.indexByFieldName == null) {
            this.indexByFieldName = this.schema.indexBy;
            if (this.indexByFieldName == null) {
                throw new DBException("indexBy is missing from the Schema");
            }
        }
        DBServer.LOGGER.info("[GenericBaseFileHandler] Set schema, done");
    }

    public void setZClass(final Class zclass) {
        this.zclass = zclass;
    }


    public void loadAllDataToIndex(final Class zclass) throws DBException {
        DBServer.LOGGER.info("[GenericBaseFileHandler] Loading index data");
        long currentPos = HEADER_INFO_SPACE;
        long rowNum = 0;
        long deletedRows = 0;
        long temporaryRows = 0;

        readLock.lock();
        try {
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
                        GenericIndex.getInstance().add(currentPos - 1);
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
                        Object object = this.readFromByteStream(new DataInputStream(new ByteArrayInputStream(b)),
                                zclass);

                        String _name = (String) object.getClass().getDeclaredField(this.schema.indexBy).get(object);
                        GenericIndex.getInstance().addIndexedValue(_name, rowNum);
                        rowNum++;
                    }
                    currentPos += recordLength;
                }
            }

            System.out.println("After startup: total row number in Database: " + rowNum);
            System.out.println("After startup: total deleted row number in Database: " + deletedRows);
            System.out.println("After startup: total temporary row number in Database: " + temporaryRows);

        } catch (IllegalAccessException | NoSuchFieldException | IOException e) {
            e.printStackTrace();
            throw new DBException(e.getMessage());
        } finally {
            readLock.unlock();
            DBServer.LOGGER.info("[GenericBaseFileHandler] Loading index data, done");
        }
    }

    Object readFromByteStream(final DataInputStream stream, final Class zclass) throws IOException {
        Object result;
        try {
            result = Class.forName(zclass.getCanonicalName()).getDeclaredConstructor(new Class[]{}).newInstance();

            for (Field field : this.schema.fields) {
                if (field.fieldType.equalsIgnoreCase("String")) {
                    int fieldLength = stream.readInt();
                    byte[] b = new byte[fieldLength];
                    stream.read(b);
                    String value = new String(b, "UTF-8");
                    // set the field value to result object
                    result.getClass().getDeclaredField(field.fieldName).set(result, value);
                } else if (field.fieldType.equalsIgnoreCase("int")) {
                    int value = stream.readInt();
                    // set the field value to result object
                    result.getClass().getDeclaredField(field.fieldName).set(result, value);
                }
                // TODO implement other field types
            }
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException | NoSuchFieldException | NoSuchMethodException | InvocationTargetException e) {
            e.printStackTrace();
            throw new IOException(e.getMessage());
        }

        return result;
    }

    byte[] readRawRecord(long bytePositionOfRow) throws IOException {
        DBServer.LOGGER.info("[GenericBaseFileHandler] Read raw record, position: " + bytePositionOfRow);
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
            DBServer.LOGGER.info("[GenericBaseFileHandler] Read raw record, position, Done ");
        }
    }

    public void close() throws IOException {
        DBServer.LOGGER.info("[GenericBaseFileHandler] Closing");
        this.dbFile.close();
    }


    public List<DebugRowInfo> loadAllDataFromFile(final Class zclass) throws IOException {
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
                    Object p = this.readFromByteStream(new DataInputStream(new ByteArrayInputStream(b)), zclass);
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

    public String getDBName() {
        return this.dbFileName;
    }

    public void commit(List<Long> newRows, List<Long> deletedRows) throws DBException {
        DBServer.LOGGER.info("[GenericBaseFileHandler] Commit");
        writeLock.lock();
        try {
            for (long position : newRows) {
                this.dbFile.seek(position);
                this.dbFile.writeBoolean(false); // it is not temporary
                // re-read the record
                byte[] b = this.readRawRecord(position);
                Object object = this.readFromByteStream(new DataInputStream(new ByteArrayInputStream(b)), this.zclass);

                // add it to the index

                String _name = (String) object.getClass().getDeclaredField(this.schema.indexBy).get(object);
                GenericIndex.getInstance().addIndexedValue(_name, GenericIndex.getInstance().getTotalNumberOfRows());
                GenericIndex.getInstance().add(position);
            }

            // operate on deleted rows
            for (long position : deletedRows) {
                this.dbFile.seek(position);
                this.dbFile.writeBoolean(false);
                GenericIndex.getInstance().removeByFilePosition(position);
            }
        } catch (IllegalAccessException | NoSuchFieldException | IOException e) {
            throw new DBException(e.getMessage());
        } finally {
            writeLock.unlock();
            DBServer.LOGGER.info("[GenericBaseFileHandler] Commit, Done");
        }
    }

    public void rollback(List<Long> newRows, List<Long> deletedRows) throws DBException {
        DBServer.LOGGER.info("[GenericBaseFileHandler] Rollback");
        writeLock.lock();
        try {
            for (long position : newRows) {
                this.dbFile.seek(position);
                // not temporary
                this.dbFile.writeBoolean(false);

                // deleted
                this.dbFile.seek(position + 1);
                this.dbFile.writeBoolean(true);
                GenericIndex.getInstance().removeByFilePosition(position);
            }

            for (long position : deletedRows) {
                this.dbFile.seek(position);
                // not temporary
                this.dbFile.writeBoolean(false);

                // isdeleted
                this.dbFile.seek(position + 1);
                this.dbFile.writeBoolean(false);
                // re-read the record
                byte[] b = this.readRawRecord(position);
                Object object = this.readFromByteStream(new DataInputStream(new ByteArrayInputStream(b)), this.zclass);


                // add it to the index
                String _name = (String) object.getClass().getDeclaredField("pname").get(object);
                GenericIndex.getInstance().addIndexedValue(_name, GenericIndex.getInstance().getTotalNumberOfRows());
                GenericIndex.getInstance().add(position);
            }
        } catch (IllegalAccessException | NoSuchFieldException | IOException e) {
            e.printStackTrace();
            throw new DBException(e.getMessage());
        } finally {
            writeLock.unlock();
            DBServer.LOGGER.info("[GenericBaseFileHandler] Rollback, Done");
        }
    }

    private void setDBVersion() throws IOException {
        DBServer.LOGGER.info("[GenericBaseFileHandler] set DB version");
        this.dbFile.seek(0);
        String VERSION = "0.1";
        this.dbFile.write(VERSION.getBytes(Charsets.UTF_8));
        char[] chars = new char[HEADER_INFO_SPACE - VERSION.length()];
        Arrays.fill(chars, ' ');
        this.dbFile.write(new String(chars).getBytes());
    }

    public String getDBVersion() throws IOException {
        readLock.lock();
        try {
            this.dbFile.seek(0);
            byte[] b = new byte[HEADER_INFO_SPACE];
            this.dbFile.read(b);

            return new String(b, "UTF-8").trim();
        } finally {
            readLock.unlock();
        }
    }

    protected int getFieldLengthByType(Field field, Object object) throws NoSuchFieldException, IllegalAccessException {
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
                DBServer.LOGGER.info("[GenericBaseFileHandler] No field was found : " + field.fieldType);
                throw new NoSuchFieldException(field.fieldType);
            }
            // TODO add more types
        }

        return result;
    }
}
