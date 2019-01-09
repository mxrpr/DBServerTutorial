package com.mixer.testapp;

import com.mixer.dbserver.DB;
import com.mixer.dbserver.DBServer;
import com.mixer.exceptions.DuplicateNameException;
import com.mixer.raw.specific.Index;
import com.mixer.raw.Person;
import com.mixer.transaction.ITransaction;
import com.mixer.util.DebugRowInfo;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.stream.IntStream;


public class TestApp {
    final static String dbFile = "Dbserver.db";

    public static void main(String args[]) {
        new TestApp().performTest();
    }

    private void performTest() {
        try {
            deleteDatabase();
//            fragmentDatabase();
            addPersonWithTransaction();
//            removePersonWithTransaction();
            listAllRecords();
//            defragmentDB();
//
//            System.out.println("-----after defragmentation------");
//            listAllRecords();
//            deleteDatabase();
//            doMultipleThreadTest();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    void deleteDatabase() {
        File f = new File("Dbserver.db");
        if (f.exists())
            f.delete();
    }


    void doMultipleThreadTest() throws IOException {

        CountDownLatch cl = new CountDownLatch(3);

        try (DBServer db = new DBServer(dbFile)) {
            Runnable runnableAdd = () -> {

                while(true) {
                    int i = new Random().nextInt(4000);
                    Person p = new Person("John" + i, 44, "Berlin", "www-404", "This is a description");
                    try {
                        db.add(p);
                    } catch (IOException | DuplicateNameException e) {
                        e.printStackTrace();
                    }
                }
            };

            Runnable runnableUpdate = () -> {
                while(true) {
                    int i = new Random().nextInt(4000);
                    Person p = new Person("John" + i + "_updated", 44, "Berlin", "www-404", "This is a description");
                    try {
                        db.update("John" + i, p);
                    } catch (IOException | DuplicateNameException e) {
                        e.printStackTrace();
                    }
                }
            };

            Runnable runnableListAll = () -> {
                while(true) {
                    try {
                        db.listAllRowsWithDebug();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            };

            ExecutorService executorService = Executors.newFixedThreadPool(3);
            executorService.submit(runnableListAll);
            executorService.submit(runnableUpdate);
            executorService.submit(runnableAdd);

            cl.await();

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    void addPersonWithTransaction() throws IOException, DuplicateNameException {
        try (DBServer db = new DBServer(dbFile)) {
            ITransaction transaction = db.beginTransaction(); //  begin transaction
            Person p = new Person("John", 44, "Berlin", "www-404", "This is a description");
            db.add(p);
            db.commit(); // db.rollback()
//            db.rollback();
        }
    }

    void removePersonWithTransaction() throws IOException, DuplicateNameException {
        try (DBServer db = new DBServer(dbFile)) {
            db.beginTransaction(); //  begin transaction
            Person p = new Person("John", 44, "Berlin", "www-404", "This is a description");
            db.add(p);
            db.commit();
            db.beginTransaction(); //  begin transaction
            db.delete(0);
            db.commit();
        }
    }
    void defragmentDB() throws IOException, DuplicateNameException {
        try (DBServer db = new DBServer(dbFile)) {
            db.defragmentDatabase();
        }
    }


    void testSearchWithRegexp() throws IOException {
        try (DBServer db = new DBServer(dbFile)) {
            List<Person> result = db.searchWithRegexp("Jo.*");
            System.out.println("-------Search with regexp------");
            for (Person person : result) {
                System.out.println(person);
            }

        }
    }

    void testLeveinsthein() throws IOException {
        try (DBServer db = new DBServer(dbFile)) {
            List<Person> result = db.searchWithLeveinshtein("John", 0);
            System.out.println("-------Search with Leveinsthein------");
            for (Person person : result) {
                System.out.println(person);
            }
        }
    }

    void testSearch() throws IOException {
        try (DBServer db = new DBServer(dbFile)) {
            Person p = db.search("John1");
            System.out.println("Found person: " + p);
        }
    }

    void listAllRecords() throws Exception {
        try (DBServer db = new DBServer(dbFile)) {
            List<DebugRowInfo> result = db.listAllRowsWithDebug();
            System.out.println("Total row number: " + Index.getInstance().getTotalNumberOfRows());
            for (DebugRowInfo dri : result) {
                prittyPrintRow(dri);
            }
        }
    }

    private void prittyPrintRow(DebugRowInfo dri) {
        Person p = (Person)dri.object();
        boolean isDeleted = dri.isDeleted();
        boolean isTemporary = dri.isTemporary();
        String debugChar = isDeleted ? "-" : "+";
        String temporaryChar = isTemporary? "temp" : "final";
        String s = String.format(" %s %s name: %s, age: %d, address: %s, carplatenum: %s, desc: %s",
                temporaryChar,
                debugChar,
                p.pname,
                p.age,
                p.address,
                p.carplatenumber,
                p.description);
        System.out.println(s);
    }

    void delete(int number) throws Exception {
        try (DB db = new DBServer(dbFile)) {
            db.delete(number);
        }
    }

    void fillDB(int rowNumber) throws Exception {

        try (DB db = new DBServer(dbFile)) {
            for (int i = 0; i < rowNumber; i++) {
                Person p = new Person("John" + i, 44, "Berlin", "www-404", "This is a description");
                db.add(p);
            }
        }
    }

    public void fragmentDatabase() throws IOException, DuplicateNameException {

        try (DB db = new DBServer(dbFile)) {
            // add 100 rows

            for (int i : IntStream.range(0, 100).toArray()) {
                Person p = new Person("John" + i, 44, "Berlin", "www-404", "This is a description");
                db.add(p);
            }

            // update the rows
            for (int i : IntStream.range(0, 100).toArray()) {
                if (i % 2 == 0) {
                    db.update("John" + i, new Person("John" + i + "_updated", 44, "Berlin", "www-404", "This is a " +
                            "description"));
                }
            }
        }
    }


}
