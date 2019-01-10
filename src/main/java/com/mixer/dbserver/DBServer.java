package com.mixer.dbserver;

import com.mixer.exceptions.DuplicateNameException;
import com.mixer.raw.specific.FileHandler;
import com.mixer.raw.specific.Index;
import com.mixer.raw.Person;
import com.mixer.transaction.ITransaction;
import com.mixer.transaction.Transaction;
import com.mixer.util.DebugRowInfo;
import com.mixer.util.OperationUnit;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class DBServer implements DB {

    private FileHandler fileHandler;
    private Map<Long, ITransaction> transactions;

    public static final Logger LOGGER = Logger.getLogger("DBServer");
    private static final String PROPERTY_FILE_NAME = "config.properties";
    private static final String LOG_LEVEL = "LOG_LEVEL";

    public DBServer(final String dbFileName) throws IOException {
        this.fileHandler = new FileHandler(dbFileName);
        this.transactions = new LinkedHashMap<>();
        this.initialise();
    }

    private void initialise() throws IOException {
        this.fileHandler.initialise();

        Properties properties = new Properties();
        try(FileInputStream fis = new FileInputStream(PROPERTY_FILE_NAME)) {
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
        this.fileHandler.loadAllDataToIndex();
    }

    @Override
    public void close() throws IOException {
        LOGGER.info("[" + this.getClass().getName() + "]" + "Closing DBServer");
        Index.getInstance().clear();
        this.fileHandler.close();
    }

    @Override
    public void add(Person person) throws IOException, DuplicateNameException {
        LOGGER.info("[DBServer] Adding person : " + person);
        OperationUnit ou = this.fileHandler.add(person.pname, person.age, person.address, person.carplatenumber,
                person.description);
        this.getTransaction().registerAdd(ou.addedRowPosition);
    }

    @Override
    public void delete(long rowNumber) throws IOException {
        if (rowNumber < 0) {
            LOGGER.info("[DBServer] Row Number is less then 0:  " + rowNumber);
            throw new IOException("Row Number is less then 0");
        }
        LOGGER.info("[DBServer] Delete person with rowNumber: " + rowNumber);
        OperationUnit ou = this.fileHandler.deleteRow(rowNumber);
        this.getTransaction().registerDelete(ou.deletedRowPosition);
    }

    @Override
    public void update(long rowNumber, final Person person) throws IOException, DuplicateNameException {
        LOGGER.info("[DBServer] updating person. Row number " + rowNumber + " person: " + person);
        OperationUnit operationUnit = this.fileHandler.update(rowNumber, person.pname, person.age, person.address,
                person.carplatenumber, person.description);
        ITransaction transaction = this.getTransaction();
        transaction.registerDelete(operationUnit.deletedRowPosition);
        transaction.registerAdd(operationUnit.addedRowPosition);
    }

    @Override
    public void update(String name, Person person) throws IOException, DuplicateNameException {
        LOGGER.info("[[DBServer] updating person. Name: " + name + " person: " + person);
        OperationUnit operationUnit = this.fileHandler.update(name, person.pname, person.age, person.address,
                person.carplatenumber,
                person.description);
        ITransaction transaction = this.getTransaction();
        transaction.registerDelete(operationUnit.deletedRowPosition);
        transaction.registerAdd(operationUnit.addedRowPosition);

    }

    @Override
    public Person read(long rowNumber) throws IOException {
        LOGGER.info("[DBServer] Reading row:" + rowNumber);
        Person person = this.fileHandler.readRow(rowNumber);
        this.logInfoPerson(person);

        return person;
    }

    @Override
    public Person search(String name) throws IOException {
        LOGGER.info("[DBServer] Searching for person: " + name);
        final Person person =  this.fileHandler.search(name);
        this.logInfoPerson(person);

        return person;
    }

    @Override
    public List<Person> searchWithLeveinshtein(String name, int tolerance) throws IOException {
        LOGGER.info("[DBServer] Search with Leveinshtein " + name + " tolerance:" + tolerance);
        final List<Person> persons =  this.fileHandler.searchWithLeveinshtein(name, tolerance);
        this.logInfoPersonList(persons);

        return persons;
    }

    @Override
    public List<Person> searchWithRegexp(String regexp) throws IOException {
        LOGGER.info("[DBServer] Search with regexp " + regexp);
        final List<Person> persons = this.fileHandler.searchWithRegexp(regexp);
        this.logInfoPersonList(persons);

        return persons;
    }

    private ITransaction getTransaction() {
        long threadID = Thread.currentThread().getId();
        LOGGER.info("[DBServer] Get transaction with id: " + threadID);
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
    public void commit() throws IOException {
        ITransaction transaction = this.getTransaction();
        if (transaction == null) {//write out error in logs
            LOGGER.info("[DBServer] transaction was not found!!");
            return;
        }

        this.fileHandler.commit(transaction.getNewRows(), transaction.getDeletedRows());
        this.transactions.remove(Thread.currentThread().getId());
        transaction.clear();
        LOGGER.info("[DBServer]  Commit DONE (" + transaction.getUid() + ")");
    }

    @Override
    public void rollback() throws IOException {
        ITransaction transaction  = this.getTransaction();
        if (transaction == null) //write out error in logs
            return;

        this.fileHandler.rollback(transaction.getNewRows(), transaction.getDeletedRows());
        this.transactions.remove(Thread.currentThread().getId());
        transaction.clear();
        LOGGER.info("[DBServer] Rollback DONE (" + transaction.getUid() + ")");
    }

    public List<DebugRowInfo> listAllRowsWithDebug() throws IOException {
        return this.fileHandler.loadAllDataFromFile();
    }

    public void defragmentDatabase() throws IOException, DuplicateNameException {
        LOGGER.info("[DBServer] Defragmenting database");
        File tmpFile = File.createTempFile("defrag", "dat");
        Index.getInstance().clear();

        // open temporary file
        FileHandler defragFH = new FileHandler(new RandomAccessFile(tmpFile, "rw"), tmpFile.getName());
        List<DebugRowInfo> debugRowInfos = this.fileHandler.loadAllDataFromFile();
        for (DebugRowInfo dri : debugRowInfos) {
            if (dri.isDeleted() || dri.isTemporary()) {
                continue;
            }
            Person p = (Person)dri.object();
            defragFH.add(p.pname,
                    p.age,
                    p.address,
                    p.carplatenumber,
                    p.description);
        }

        boolean wasDeleted = this.fileHandler.deleteFile();
        if (!wasDeleted) {
            tmpFile.delete();
            LOGGER.severe("[DBServer] Database file cannot be deleted during the defragmantation");
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
        this.fileHandler = new FileHandler(oldDatabaseName);
        Index.getInstance().clear();

        // reinitialise the Index
        this.initialise();
        LOGGER.info("[DBServer] Database file has been defragmented");
    }

    private void logInfoPerson(final Person person) {
        LOGGER.info("[" + this.getClass().getName() + "]" + "Read person: " + person);
    }

    private void logInfoPersonList(final List<Person> persons) {
        StringBuilder sb = new StringBuilder(300);

        for(Person person: persons) {
            sb.append(person.toString());
            sb.append(System.getProperty("line.separator"));
        }

        LOGGER.info("[DBServer] Read persons: " + sb);
    }

    @Override
    public long getTotalRecordNumber() {
        return Index.getInstance().getTotalNumberOfRows();
    }
}
