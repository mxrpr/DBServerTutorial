package com.mixer.raw.general;

import com.mixer.dbserver.DBServer;
import com.mixer.exceptions.DBException;
import com.mixer.util.DebugRowInfo;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Class is responsible to handle the basic file operations, like read and write data into the file.
 * This class knows the structure of the file - and can read different 'special' information like the db version for example.
 */
class GenericBaseFileHandler {

    RandomAccessFile dbFile;
    private final String dbFileName;
    Schema schema;
    Class<?> zClass;
    String indexByFieldName;
    final GenericIndex index;

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    final Lock readLock = readWriteLock.readLock();
    final Lock writeLock = readWriteLock.writeLock();
    private final static int HEADER_INFO_SPACE = 100;
    final String VERSION = "0.1";


    /**
     * Constructor
     *
     * @param dbFileName Name of the database file
     * @param index Reference to a GenericIndex object
     * @throws FileNotFoundException
     */
    GenericBaseFileHandler(final String dbFileName,
                           final GenericIndex index) throws FileNotFoundException {
        this.index = index;
        this.dbFileName = dbFileName;
        this.dbFile = new RandomAccessFile(dbFileName, "rw");
    }

    /**
     * Constructor
     *
     * @param  randomAccessFile Reference to a RandomAccessFile object
     * @param dbFileName Name of the database file
     * @param index Reference to a GenericIndex object
     */
    GenericBaseFileHandler(final RandomAccessFile randomAccessFile,
                           final String dbFileName,
                           final GenericIndex index) {
        this.index = index;
        this.dbFileName = dbFileName;
        this.dbFile = randomAccessFile;
    }

    public void initialise() throws IOException {
        DBServer.LOGGER.info("[GenericBaseFileHandler] Initialise");
        if (this.dbFile.length() == 0) {
            this.setTableVersion();
        } else {
            String dbVersion = this.getTableVersion();
            System.out.println("DB version: " + dbVersion);
        }
        DBServer.LOGGER.info("[GenericBaseFileHandler] Initialisation done");
    }

    /**
     * Set schema of the table
     *
     * @param schema Schema object
     * @see Schema
     *
     * @throws DBException
     */
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

    public void setZClass(final Class<?> zClass) {
        this.zClass = zClass;
    }

    /**
     * Loads all data from the database to Index
     *
     * @param zClass Class of the strored object
     * @throws DBException
     */
    public void loadAllDataToIndex(final Class<?> zClass) throws DBException {
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
                        this.index.add(currentPos - 1);
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
                                zClass);

                        String _name = (String) object.getClass().getDeclaredField(this.schema.indexBy).get(object);
                        this.index.addIndexedValue(_name, rowNum);
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

    @SuppressWarnings("ResultOfMethodCallIgnored")
    Object readFromByteStream(final DataInputStream stream, final Class<?> zClass) throws IOException {
        Object result;
        try {
            result = Class.forName(zClass.getCanonicalName()).getDeclaredConstructor(new Class[]{}).newInstance();

            for (Field field : this.schema.fields) {
                if (field.fieldType.equalsIgnoreCase("String")) {
                    int fieldLength = stream.readInt();
                    byte[] b = new byte[fieldLength];
                    stream.read(b);
                    String value = new String(b, StandardCharsets.UTF_8);
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

    /**
     * Reads the raw record from the file from the given file position.
     *
     * @param bytePositionOfRow byte position of the given row in database/table
     * @return byte array
     * @throws IOException
     */
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

    /**
     * Only for debugging purpose. We can read the records from the table with all useful information
     * @see DebugRowInfo
     * @param zClass Class of the stored object
     * @return List of DebugRowInfo objects
     * @throws IOException
     */
    public List<DebugRowInfo> loadAllDataFromFile(final Class<?> zClass) throws IOException {
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
                    Object p = this.readFromByteStream(new DataInputStream(new ByteArrayInputStream(b)), zClass);
                    result.add(new DebugRowInfo(p, isDeleted, isTemporary));
                    currentPosition += recordLength;
                }

                return result;
            }
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Delete database file. Name of the db file is stored in the dbFileName variable.
     * @return true if successful
     * @throws IOException
     */
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

    /**
     * Get table name
     * @return String
     */
    public String getTableName() {
        return this.dbFileName;
    }

    /**
     * Commit operations on the database/table
     *
     * @param newRows list of new rows inserted
     * @param deletedRows list of deleted rows
     * @throws DBException
     */
    public void commit(List<Long> newRows, List<Long> deletedRows) throws DBException {
        DBServer.LOGGER.info("[GenericBaseFileHandler] Commit");
        writeLock.lock();
        try {
            for (long position : newRows) {
                this.dbFile.seek(position);
                this.dbFile.writeBoolean(false); // it is not temporary
                // re-read the record
                byte[] b = this.readRawRecord(position);
                Object object = this.readFromByteStream(new DataInputStream(new ByteArrayInputStream(b)), this.zClass);

                // add it to the index

                String _name = (String) object.getClass().getDeclaredField(this.schema.indexBy).get(object);
                this.index.addIndexedValue(_name, this.index.getTotalNumberOfRows());
                this.index.add(position);
            }

            // operate on deleted rows
            for (long position : deletedRows) {
                this.dbFile.seek(position);
                this.dbFile.writeBoolean(false);
                this.index.removeByFilePosition(position);
            }
        } catch (IllegalAccessException | NoSuchFieldException | IOException e) {
            throw new DBException(e.getMessage());
        } finally {
            writeLock.unlock();
            DBServer.LOGGER.info("[GenericBaseFileHandler] Commit, Done");
        }
    }

    /**
     * Rolling back operations on the database/table
     *
     * @param newRows list of new rows inserted
     * @param deletedRows list of deleted rows
     * @throws DBException
     */
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
                this.index.removeByFilePosition(position);
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
                Object object = this.readFromByteStream(new DataInputStream(new ByteArrayInputStream(b)), this.zClass);


                // add it to the index
                String _name = (String) object.getClass().getDeclaredField("pname").get(object);
                this.index.addIndexedValue(_name, this.index.getTotalNumberOfRows());
                this.index.add(position);
            }
        } catch (IllegalAccessException | NoSuchFieldException | IOException e) {
            e.printStackTrace();
            throw new DBException(e.getMessage());
        } finally {
            writeLock.unlock();
            DBServer.LOGGER.info("[GenericBaseFileHandler] Rollback, Done");
        }
    }

    /**
     * Set the table version
     *
     * @throws IOException
     */
    private void setTableVersion() throws IOException {
        DBServer.LOGGER.info("[GenericBaseFileHandler] set DB version");
        this.dbFile.seek(0);
        this.dbFile.write(VERSION.getBytes(StandardCharsets.UTF_8));
        char[] chars = new char[HEADER_INFO_SPACE - VERSION.length()];
        Arrays.fill(chars, ' ');
        this.dbFile.write(new String(chars).getBytes());
    }

    /**
     * Get table version
     * @return
     * @throws IOException
     */
    public String getTableVersion() throws IOException {
        readLock.lock();
        try {
            this.dbFile.seek(0);
            byte[] b = new byte[HEADER_INFO_SPACE];
            this.dbFile.read(b);

            return new String(b, StandardCharsets.UTF_8).trim();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Depending on field type, the field can have different length. This method
     * return the field length by its type
     *
     * @param field Field object
     * @param object Reference to the object which we would like to store in db/table
     * @return Length of the field
     * @see Field
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     */
    int getFieldLengthByType(Field field, Object object) throws NoSuchFieldException, IllegalAccessException {
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
