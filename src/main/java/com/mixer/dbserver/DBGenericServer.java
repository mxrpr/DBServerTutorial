package com.mixer.dbserver;

import com.google.gson.Gson;
import com.mixer.exceptions.DBException;
import com.mixer.exceptions.DuplicateNameException;
import com.mixer.raw.general.Field;
import com.mixer.raw.general.GenericFileHandler;
import com.mixer.raw.general.GenericIndex;
import com.mixer.raw.general.Schema;


import com.mixer.transaction.ITransaction;
import com.mixer.transaction.Transaction;
import com.mixer.util.DebugRowInfo;
import com.mixer.util.OperationUnit;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class DBGenericServer implements DBGeneric {

    private GenericFileHandler fileHandler;
    private Map<Long, ITransaction> transactions;
    private Schema schema;
    private Class zclass;

    private final Logger LOGGER = Logger.getLogger("DBServer");
    private static final String PROPERTY_FILE_NAME = "config.properties";
    private static final String LOG_LEVEL = "LOG_LEVEL";

    public DBGenericServer(final String dbFileName, final String schema, final Class zclass) throws  DBException {
        try {
            this.schema = this.readSchema(schema);
            this.zclass = zclass;

            this.fileHandler = new GenericFileHandler(dbFileName);
            this.fileHandler.setSchema(this.schema);
            this.fileHandler.setZClass(this.zclass);

            this.transactions = new LinkedHashMap<>();

            this.initialise();
        }catch(IOException e) {
            throw new DBException(e.getMessage());
        }
    }

    private Schema readSchema(final String schema) {
        Gson gson = new Gson();
        Schema tmpSchema = gson.fromJson(schema, Schema.class);
        for (Field field: tmpSchema.fields){
            LOGGER.info(field.toString());
        }

        return tmpSchema;
    }

    private void initialise() throws  IOException, DBException {
        GenericIndex.getInstance().initialize(this.schema);
        this.fileHandler.initialise();

        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream(PROPERTY_FILE_NAME)) {
            properties.load(fis);
        }

        boolean hasLogLevel = properties.containsKey(LOG_LEVEL);
        if (!hasLogLevel)
            LOGGER.setLevel(Level.SEVERE);
        else {
            String logLevel = (String) properties.get(LOG_LEVEL);
            if (logLevel.equalsIgnoreCase("SEVERE"))
                LOGGER.setLevel(Level.SEVERE);
            else if (logLevel.equalsIgnoreCase("INFO"))
                LOGGER.setLevel(Level.INFO);
            else if (logLevel.equalsIgnoreCase("ALL"))
                LOGGER.setLevel(Level.ALL);
        }
        this.fileHandler.loadAllDataToIndex(this.zclass);
    }

    @Override
    public void close() throws IOException {
        LOGGER.info("[" + this.getClass().getName() + "]" + "Closing DBServer");
        GenericIndex.getInstance().clear();
        this.fileHandler.close();
    }

    @Override
    public void add(Object object) throws DuplicateNameException, DBException {
        LOGGER.info("[" + this.getClass().getName() + "]" +"Adding object : " + object);
        OperationUnit ou = this.fileHandler.add(object, false);
        this.getTransaction().registerAdd(ou.addedRowPosition);
    }

    @Override
    public void delete(long rowNumber) throws DBException {
        if (rowNumber < 0) {
            LOGGER.info("[" + this.getClass().getName() + "]" + "Row Number is less then 0:  " + rowNumber);
            throw new DBException("Row number is less then 0");
        }
        LOGGER.info("[" + this.getClass().getName() + "]" + "Delete person with rowNumber: " + rowNumber);
        OperationUnit ou = this.fileHandler.deleteRow(rowNumber);
        this.getTransaction().registerDelete(ou.deletedRowPosition);
    }

    @Override
    public void update(long rowNumber, final Object object) throws DuplicateNameException, DBException {
        LOGGER.info("[" + this.getClass().getName() + "]" + "updating object. Row number " + rowNumber + " person: " + object);
        OperationUnit operationUnit = this.fileHandler.update(rowNumber, object);
        ITransaction transaction = this.getTransaction();
        transaction.registerDelete(operationUnit.deletedRowPosition);
        transaction.registerAdd(operationUnit.addedRowPosition);
    }

    @Override
    public void update(String indexedFieldName, Object object) throws DuplicateNameException, DBException {
        LOGGER.info("[" + this.getClass().getName() + "]" + "updateing object. Name: " + indexedFieldName + " object:" +
                " " + object);
        OperationUnit operationUnit = this.fileHandler.update(indexedFieldName, object);
        ITransaction transaction = this.getTransaction();
        transaction.registerDelete(operationUnit.deletedRowPosition);
        transaction.registerAdd(operationUnit.addedRowPosition);

    }

    @Override
    public Object read(long rowNumber) throws DBException {
        LOGGER.info("[" + this.getClass().getName() + "]" + "Reading row:" + rowNumber);
        Object object = this.fileHandler.readRow(rowNumber);
        this.logInfoObject(object);

        return object;
    }

    @Override
    public Object search(String indexedFieldName) throws DBException {
        LOGGER.info("[" + this.getClass().getName() + "]" + "Searching for object: " + indexedFieldName);
        final Object object =  this.fileHandler.search(indexedFieldName);
        this.logInfoObject(object);

        return object;
    }

    @Override
    public List<Object> searchWithLeveinshtein(String indexedFieldName, int tolerance) throws DBException {
        LOGGER.info("[" + this.getClass().getName() + "]" + "Search with Leveinshtein " + indexedFieldName + " tolerance:" + tolerance);
        final List<Object> result =  this.fileHandler.searchWithLeveinshtein(indexedFieldName, tolerance);
        this.logInfoObjectList(result);

        return result;
    }

    @Override
    public List<Object> searchWithRegexp(String regexp) throws DBException {
        LOGGER.info("[" + this.getClass().getName() + "]" + "Search with regexp " + regexp);
        final List<Object> result = this.fileHandler.searchWithRegexp(regexp);
        this.logInfoObjectList(result);

        return result;
    }

    private ITransaction getTransaction() {
        long threadID = Thread.currentThread().getId();
        LOGGER.info("[" + this.getClass().getName() + "]" + "Get transaction with id: " + threadID);
        return this.transactions.getOrDefault(threadID, null);
    }

    @Override
    public ITransaction beginTransaction() {
        long threadID = Thread.currentThread().getId();
        if (this.transactions.containsKey(threadID))
            return this.transactions.get(threadID);

        ITransaction transaction = new Transaction();
        this.transactions.put(threadID, transaction);
        return transaction;
    }

    @Override
    public void commit() throws DBException {
        ITransaction transaction = this.getTransaction();
        if (transaction == null) {//write out error in logs
            LOGGER.info("[" + this.getClass().getName() + "]" + "transaction was not found!!");
            return;
        }

        this.fileHandler.commit(transaction.getNewRows(), transaction.getDeletedRows());
        this.transactions.remove(Thread.currentThread().getId());
        transaction.clear();
        LOGGER.info("[" + this.getClass().getName() + "]" + " Commit DONE (" + transaction.getUid() + ")");
    }

    @Override
    public void rollback() throws DBException {
        ITransaction transaction  = this.getTransaction();
        if (transaction == null) //write out error in logs
            return;

        this.fileHandler.rollback(transaction.getNewRows(), transaction.getDeletedRows());
        this.transactions.remove(Thread.currentThread().getId());
        transaction.clear();
        LOGGER.info("[" + this.getClass().getName() + "]" + " Rollback DONE (" + transaction.getUid() + ")");
    }

    public List<DebugRowInfo> listAllRowsWithDebug() throws IOException {
        return this.fileHandler.loadAllDataFromFile(this.zclass);
    }

    public void defragmentDatabase() throws IOException, DuplicateNameException, DBException {
        LOGGER.info("[" + this.getClass().getName() + "]" + "Defragmenting database");
        File tmpFile = File.createTempFile("defrag", "dat");
        GenericIndex.getInstance().clear();

        // open temporary file
        GenericFileHandler defragFH = new GenericFileHandler(new RandomAccessFile(tmpFile, "rw"), tmpFile.getName());
        defragFH.setSchema(this.schema);
        defragFH.setZClass(this.zclass);
        defragFH.initialise();
        List<DebugRowInfo> debugRowInfos = this.fileHandler.loadAllDataFromFile(this.zclass);
        for (DebugRowInfo dri : debugRowInfos) {
            if (dri.isDeleted() || dri.isTemporary()) {
                continue;
            }
            Object p = dri.object();
            defragFH.add(p, true);
        }

        boolean wasDeleted = this.fileHandler.deleteFile();
        if (!wasDeleted) {
            tmpFile.delete();
            LOGGER.severe("[" + this.getClass().getName() + "]" + "Database file cannot be deleted during the defragmantation");
            this.initialise();
            throw new IOException("DB cannot be defragmented. Check the logs.");
        }

        this.fileHandler.close();
        String oldDatabaseName = this.fileHandler.getDBName();

        // copy temporary file back to db name
        Files.copy(tmpFile.toPath(), FileSystems.getDefault().getPath("", oldDatabaseName),
                StandardCopyOption.REPLACE_EXISTING);

        // close the temporary file
        defragFH.close();
        this.fileHandler = new GenericFileHandler(oldDatabaseName);
        this.fileHandler.setSchema(this.schema);
        this.fileHandler.setZClass(this.zclass);


        GenericIndex.getInstance().clear();

        // reinitialise the Index
        this.initialise();
        LOGGER.info("[" + this.getClass().getName() + "]" + "Database file has been defragmented");
    }

    private void logInfoObject(final Object object) {
        LOGGER.info("[" + this.getClass().getName() + "]" + "Read object: " + object);
    }

    private void logInfoObjectList(final List<Object> objects) {
        StringBuilder sb = new StringBuilder(300);

        for(Object object: objects) {
            sb.append(object.toString());
            sb.append(System.getProperty("line.separator"));
        }

        LOGGER.info("[" + this.getClass().getName() + "]" + "Read persons: " + sb);
    }

    @Override
    public long getTotalRecordNumber() {
        return GenericIndex.getInstance().getTotalNumberOfRows();
    }

    public String getDBVersion() throws IOException {
        return this.fileHandler.getDBVersion();
    }
}
