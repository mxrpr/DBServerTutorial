package com.mixer.raw.general;


import com.google.gson.Gson;
import com.mixer.dbserver.DBGenericServer;
import com.mixer.exceptions.DBException;
import com.mixer.exceptions.DuplicateNameException;
import com.mixer.query.SQLRegexp;
import com.mixer.query.sql.DBEntry;
import com.mixer.query.sql.ResultSet;
import com.mixer.transaction.ITransaction;
import com.mixer.transaction.Transaction;
import com.mixer.util.DebugRowInfo;
import com.mixer.util.OperationUnit;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * MxrTable represents a table in the database.
 * This objects contains implementation of all operations which
 * can be used to manipulate the database.
 */
public class MxrTable implements Table {

    private GenericFileHandler fileHandler;
    private Map<Long, ITransaction> transactions;
    private Schema schema;
    private Class zclass;
    private GenericIndex index;


    /**
     * Constructs a new MxrTable object.
     * 
     * @param dbFileName Name of the database file
     * @param schema	Schema of the object to be stored.Schema contains the object's field related information
     * @param zclass	Class of the stored object
     * @param indexPool	Reference to the used Index component
     * @throws DBException In case of error, DBException will be thrown
     */
    public MxrTable(final String dbFileName,
                    final String schema,
                    final Class zclass,
                    final GenericIndexPool indexPool) throws DBException {
        try {
            this.schema = this.readSchema(schema);
            this.zclass = zclass;
            this.index = indexPool.createIndex(dbFileName, this.schema);

            this.fileHandler = new GenericFileHandler(dbFileName, this.index);
            this.fileHandler.setSchema(this.schema);
            this.fileHandler.setZClass(this.zclass);

            this.transactions = new LinkedHashMap<>();
            this.initialise();

        }catch(IOException e) {
            throw new DBException(e.getMessage());
        }
    }
    
    /**
     * Initialise the object and the referenced objects, like the used FileHandler
     * 
     * @throws DBException If the initialisation fails, then we have to throw a
     * DBException to stop the construction process
     */
    private void initialise() throws DBException{
        try {
            this.fileHandler.initialise();
        } catch (IOException ioe) {
            throw new DBException(ioe.getMessage());
        }
        this.fileHandler.loadAllDataToIndex(this.zclass);
    }

    /**
     * Helper method to read the information stored in the schema
     * 
     * @param schema String
     * @return The parsed Schema object
     */
    private Schema readSchema(final String schema) {
        Gson gson = new Gson();
        Schema tmpSchema = gson.fromJson(schema, Schema.class);
        for (Field field: tmpSchema.fields){
            DBGenericServer.LOGGER.info(field.toString());
        }

        return tmpSchema;
    }

    /**
     * When a table is closed, then all referenced/used objects must be cleaned and closed.
     */
    @Override
    public void close() throws DBException {
        DBGenericServer.LOGGER.info("[" + this.getClass().getName() + "]" + "Closing DBServer");
        this.index.clear();
        try {
            this.fileHandler.close();
        } catch (IOException ioe) {
            throw new DBException(ioe.getMessage());
        }
    }

    @Override
    public void add(Object object) throws DuplicateNameException, DBException {
        DBGenericServer.LOGGER.info("[" + this.getClass().getName() + "]" +"Adding object : " + object);
        OperationUnit ou = this.fileHandler.add(object, false);
        this.getTransaction().registerAdd(ou.addedRowPosition);
    }

    @Override
    public void delete(long rowNumber) throws DBException {
        if (rowNumber < 0) {
            DBGenericServer.LOGGER.info("[" + this.getClass().getName() + "]" + "Row Number is less then 0:  " + rowNumber);
            throw new DBException("Row number is less then 0");
        }
        DBGenericServer.LOGGER.info("[" + this.getClass().getName() + "]" + "Delete person with rowNumber: " + rowNumber);
        OperationUnit ou = this.fileHandler.deleteRow(rowNumber);
        this.getTransaction().registerDelete(ou.deletedRowPosition);
    }

    @Override
    public void update(long rowNumber, final Object object) throws DuplicateNameException, DBException {
        DBGenericServer.LOGGER.info("[" + this.getClass().getName() + "]" + "updating object. Row number " + rowNumber + " person: " + object);
        OperationUnit operationUnit = this.fileHandler.update(rowNumber, object);
        ITransaction transaction = this.getTransaction();
        transaction.registerDelete(operationUnit.deletedRowPosition);
        transaction.registerAdd(operationUnit.addedRowPosition);
    }

    @Override
    public void update(String indexedFieldName, Object object) throws DuplicateNameException, DBException {
        DBGenericServer.LOGGER.info("[" + this.getClass().getName() + "]" + "updateing object. Name: " + indexedFieldName + " object:" +
                " " + object);
        OperationUnit operationUnit = this.fileHandler.update(indexedFieldName, object);
        ITransaction transaction = this.getTransaction();
        transaction.registerDelete(operationUnit.deletedRowPosition);
        transaction.registerAdd(operationUnit.addedRowPosition);

    }

    @Override
    public Object read(long rowNumber) throws DBException {
        DBGenericServer.LOGGER.info("[" + this.getClass().getName() + "]" + "Reading row:" + rowNumber);
        Object object = this.fileHandler.readRow(rowNumber);
        this.logInfoObject(object);

        return object;
    }

    @Override
    public Object search(String indexedFieldName) throws DBException {
        DBGenericServer.LOGGER.info("[" + this.getClass().getName() + "]" + "Searching for object: " + indexedFieldName);
        final Object object =  this.fileHandler.search(indexedFieldName);
        this.logInfoObject(object);

        return object;
    }

    @Override
    public List<Object> searchWithLeveinshtein(String indexedFieldName, int tolerance) throws DBException {
        DBGenericServer.LOGGER.info("[" + this.getClass().getName() + "]" + "Search with Leveinshtein " + indexedFieldName + " tolerance:" + tolerance);
        final List<Object> result =  this.fileHandler.searchWithLeveinshtein(indexedFieldName, tolerance);
        this.logInfoObjectList(result);

        return result;
    }

    @Override
    public List<Object> searchWithRegexp(String regexp) throws DBException {
        DBGenericServer.LOGGER.info("[" + this.getClass().getName() + "]" + "Search with regexp " + regexp);
        final List<Object> result = this.fileHandler.searchWithRegexp(regexp);
        this.logInfoObjectList(result);

        return result;
    }

    private ITransaction getTransaction() {
        long threadID = Thread.currentThread().getId();
        DBGenericServer.LOGGER.info("[" + this.getClass().getName() + "]" + "Get transaction with id: " + threadID);
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
            DBGenericServer.LOGGER.info("[" + this.getClass().getName() + "]" + "transaction was not found!!");
            return;
        }

        this.fileHandler.commit(transaction.getNewRows(), transaction.getDeletedRows());
        this.transactions.remove(Thread.currentThread().getId());
        transaction.clear();
        DBGenericServer.LOGGER.info("[" + this.getClass().getName() + "]" + " Commit DONE (" + transaction.getUid() + ")");
    }

    @Override
    public void rollback() throws DBException {
        ITransaction transaction  = this.getTransaction();
        if (transaction == null) //write out error in logs
            return;

        this.fileHandler.rollback(transaction.getNewRows(), transaction.getDeletedRows());
        this.transactions.remove(Thread.currentThread().getId());
        transaction.clear();
        DBGenericServer.LOGGER.info("[" + this.getClass().getName() + "]" + " Rollback DONE (" + transaction.getUid() + ")");
    }

    public List<DebugRowInfo> listAllRowsWithDebug() throws DBException {
        try {
            return this.fileHandler.loadAllDataFromFile(this.zclass);
        } catch (IOException ioe) {
            throw new DBException(ioe.getMessage());
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void defragmentDatabase() throws IOException, DuplicateNameException, DBException {
        DBGenericServer.LOGGER.info("[" + this.getClass().getName() + "]" + "Defragmenting database");
        File tmpFile = File.createTempFile("defrag", "dat");
        this.index.clear();

        // open temporary file
        GenericFileHandler defragFH = new GenericFileHandler(new RandomAccessFile(tmpFile, "rw"), tmpFile.getName(),
                this.index);
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
            DBGenericServer.LOGGER.severe("[" + this.getClass().getName() + "]" + "Database file cannot be deleted during the defragmantation");
            this.initialise();
            throw new IOException("DB cannot be defragmented. Check the logs.");
        }

        this.fileHandler.close();
        String oldDatabaseName = this.fileHandler.getTableName();

        // copy temporary file back to db name
        Files.copy(tmpFile.toPath(), FileSystems.getDefault().getPath("", oldDatabaseName),
                StandardCopyOption.REPLACE_EXISTING);

        // close the temporary file
        defragFH.close();
        this.fileHandler = new GenericFileHandler(oldDatabaseName, this.index);
        this.fileHandler.setSchema(this.schema);
        this.fileHandler.setZClass(this.zclass);


        this.index.clear();

        // reinitialise the Index
        this.initialise();
        DBGenericServer.LOGGER.info("[" + this.getClass().getName() + "]" + "Database file has been defragmented");
    }

    private void logInfoObject(final Object object) {
        DBGenericServer.LOGGER.info("[" + this.getClass().getName() + "]" + "Read object: " + object);
    }

    private void logInfoObjectList(final List<Object> objects) {
        StringBuilder sb = new StringBuilder(300);

        for(Object object: objects) {
            sb.append(object.toString());
            sb.append(System.getProperty("line.separator"));
        }

        DBGenericServer.LOGGER.info("[" + this.getClass().getName() + "]" + "Read persons: " + sb);
    }

    @Override
    public long getTotalRecordNumber() {
        return this.index.getTotalNumberOfRows();
    }

    public String getTableVersion() throws DBException {
        try {
            return this.fileHandler.getTableVersion();
        } catch (IOException ioe) {
            throw new DBException(ioe.getMessage());
        }
    }

    public String getTableName() {
        return this.fileHandler.getTableName();
    }

    @Override
    public ResultSet runQuery(final String query) throws DBException {
    	DBGenericServer.LOGGER.info("[" + this.getClass().getName() + "]" + "Running SQL query: " + query);
    	Set<String> indexedValues = this.index.getIndexedValues();
    	ArrayList<DBEntry> allObjects = new ArrayList<>();
    	// get all objects
        // TODO this is very slow, and consumes a lot of memory, modify it
    	for(String  value : indexedValues) {
    		long rowNumber = this.index.getRowNumberByIndex(value);
    		Object object = this.fileHandler.readRow(rowNumber);
    		allObjects.add(new DBEntry(object, rowNumber));
    	}
    	// run the query on these objects
        SQLRegexp sqlQuery = SQLRegexp.getInstance();


    	ResultSet resultSet = sqlQuery.runQuery(query, allObjects.toArray(new DBEntry[0]));

        if(sqlQuery.isDeleteOperation()) {
            for (Object dbEntry : resultSet) {
                this.performDeleteObject((DBEntry) dbEntry);
            }
        }
        else if (sqlQuery.isUpdateOperation()){
          for (Object o : resultSet) {
                this.performUpdateObject(((DBEntry)o));
            }

        }

        return resultSet.convertToPureObjects();
    }


    private void performDeleteObject(final DBEntry object) throws DBException {
        //get the indexed field name
        this.beginTransaction();
        this.delete(object.rowIndex);
        this.commit();
    }

   private void performUpdateObject(final DBEntry object) throws DBException {

       try {
           this.beginTransaction();
           this.update(object.rowIndex, object.object);
           this.commit();
       } catch (DuplicateNameException e) {
           this.rollback();
           throw new DBException(e.getMessage());
       }

    }

//    /**
//     * Get the row number by an object.
//     * @param object
//     *
//     * @return long row numnber in database
//     */
//    private long getRowNumberFromObject(final Object object) throws DBException {
//        String indexedByField = this.schema.indexBy;
//        try {
//            //get the indexed field name
//            String value = (String) object.getClass().getField(indexedByField).get(object);
//            return this.index.getRowNumberByIndex(value);
//        }catch (IllegalAccessException e) {
//            throw new DBException(e.getMessage());
//        } catch (NoSuchFieldException e) {
//            throw new DBException(e.getMessage());
//        }
//    }
}