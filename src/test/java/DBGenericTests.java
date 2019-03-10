import com.mixer.dbserver.*;
import com.mixer.exceptions.DBException;
import com.mixer.raw.Person;
import com.mixer.raw.general.GenericIndex;
import com.mixer.raw.general.Table;
import com.mixer.util.DebugRowInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class DBGenericTests {
    private final String dbFileName = "testgeneric.db";
    private final String dbFileNameForPerson = "testgenericperson.db";

    private static final String DOG_SCHEMA = "{\n"+
            "  \"version\":\"0.1\",\n"+
            "  \"fields\":[\n"+
            "    {\"fieldName\": \"pname\", \"fieldType\":\"String\"},\n"+
            "    {\"fieldName\": \"age\",\"fieldType\": \"int\" },\n"+
            "    {\"fieldName\": \"owner\", \"fieldType\":\"String\"}\n"+
            "  ], " +
            "  \"indexBy\": \"pname\"" +
            "}";

    private static final String PERSON_SCHEMA = "{\n" +
            "  \"version\":\"0.1\",\n" +
            "  \"fields\":[\n" +
            "    {\"fieldName\": \"pname\", \"fieldType\":\"String\"},\n" +
            "    {\"fieldName\": \"age\",\"fieldType\": \"int\" },\n" +
            "    {\"fieldName\": \"address\", \"fieldType\":\"String\"},\n" +
            "    {\"fieldName\": \"carplatenumber\", \"fieldType\":\"String\"},\n" +
            "    {\"fieldName\": \"description\", \"fieldType\":\"String\"}\n" +
            "  ],\n" +
            "   \"indexBy\":\"pname\"\n" +
            "}";

    private static final String PERSON_SCHEMA_WITHOUT_INDEX_INFO = "{\n" +
            "  \"version\":\"0.1\",\n" +
            "  \"fields\":[\n" +
            "    {\"fieldName\": \"pname\", \"fieldType\":\"String\"},\n" +
            "    {\"fieldName\": \"age\",\"fieldType\": \"int\" },\n" +
            "    {\"fieldName\": \"address\", \"fieldType\":\"String\"},\n" +
            "    {\"fieldName\": \"carplatenumber\", \"fieldType\":\"String\"},\n" +
            "    {\"fieldName\": \"description\", \"fieldType\":\"String\"}\n" +
            "  ] " +
            "}";
    @Test
    public void testAdd() {
        try(DBGeneric db = DBFactory.getGenericDB()) {
            Table table = db.useTable(dbFileName, DOG_SCHEMA, Dog.class);
            table.beginTransaction();
            Dog dog = new Dog("King", 2, "John");

            table.add(dog);
            table.commit();

            Assert.assertEquals(table.getTotalRecordNumber(), 1);

        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testRead() {
        try(DBGeneric db = DBFactory.getGenericDB()) {
            Table table = db.useTable(dbFileName, DOG_SCHEMA, Dog.class);
            Dog dog = new Dog("King", 2, "John");

            table.beginTransaction();
            table.add(dog);
            table.commit();

            Assert.assertEquals(table.getTotalRecordNumber(), 1);
            Dog result = (Dog)table.read(0);
            Assert.assertTrue(result.pname.equals("King"));
            Assert.assertTrue(result.age == 2 );
            Assert.assertTrue(result.owner.equals("John"));

        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testDelete() {
        try(DBGeneric db = DBFactory.getGenericDB()) {
            Dog dog = new Dog("King", 2, "John");
            Table table = db.useTable(dbFileName, DOG_SCHEMA, Dog.class);

            table.beginTransaction();
            table.add(dog);
            table.commit();
            Assert.assertEquals(table.getTotalRecordNumber(), 1);

            table.beginTransaction();
            table.delete(0);
            table.commit();
            Assert.assertEquals(table.getTotalRecordNumber(), 0);

        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void updateByName() {
        try(DBGeneric db = DBFactory.getGenericDB()) {
            Table table = db.useTable(dbFileName, DOG_SCHEMA, Dog.class);
            Dog dog = new Dog("King", 2, "John");

            table.beginTransaction();
            table.add(dog);
            table.commit();

            Dog dog2 = new Dog("King", 3, "John");

            table.beginTransaction();
            table.update("King", dog2);
            table.commit();

            Dog result = (Dog)table.read(0);
            Assert.assertEquals(result.pname, "King");
            Assert.assertTrue(result.age == 3);

        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void updateByRowNumber() {
        try(DBGeneric db = DBFactory.getGenericDB()) {
            Table table = db.useTable(dbFileName, DOG_SCHEMA, Dog.class);
            
            Dog dog = new Dog("King", 2, "John");
            table.beginTransaction();
            table.add(dog);
            table.commit();

            Dog dog2 = new Dog("King2", 2, "John");
            table.beginTransaction();
            table.update(0, dog2);
            table.commit();

            Dog result = (Dog)table.read(0);
            Assert.assertEquals(result.pname, "King2");

        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testSearch() {
        try(DBGeneric db = DBFactory.getGenericDB()) {
            Table table = db.useTable(dbFileName, DOG_SCHEMA, Dog.class);
            table.beginTransaction();
            Dog dog = new Dog("King", 2, "John");
            table.add(dog);
            Dog dog2 = new Dog("King2", 2, "John");
            table.add(dog2);
            table.commit();

            Dog result = (Dog)table.search("King");
            Assert.assertEquals("King", result.pname);
            Assert.assertEquals(2, result.age);
            Assert.assertEquals("John", result.owner);

        }catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testSearchWithLeveinshteinWith_0_tolerance() {
        try(DBGeneric db = DBFactory.getGenericDB()) {
            Table table = db.useTable(dbFileName, DOG_SCHEMA, Dog.class);
            table.beginTransaction();

            Dog dog = new Dog("King", 2, "John");
            table.add(dog);
            Dog dog2 = new Dog("King1", 2, "John");
            table.add(dog2);
            table.commit();

            List<Object> result = table.searchWithLeveinshtein("King", 0);
            Assert.assertEquals(result.size(), 1);
            Assert.assertEquals(((Dog)result.get(0)).pname, "King");

        }catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testSearchWithLeveinshteinWith_1_tolerance() {
        try(DBGeneric db = DBFactory.getGenericDB()) {
            Table table = db.useTable(dbFileName, DOG_SCHEMA, Dog.class);
            table.beginTransaction();
            Dog dog = new Dog("King", 2, "John");
            table.add(dog);
            Dog dog2 = new Dog("King1", 2, "John");
            table.add(dog2);
            table.commit();

            List<Object> result = table.searchWithLeveinshtein("King", 1);
            Assert.assertEquals(result.size(), 2);

        }catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testWithRegexp() {
        try(DBGeneric db = DBFactory.getGenericDB()) {
            Table table = db.useTable(dbFileName, DOG_SCHEMA, Dog.class);
            table.beginTransaction();

            Dog dog = new Dog("King", 2, "John");
            table.add(dog);
            Dog dog2 = new Dog("King1", 2, "John");
            table.add(dog2);
            table.commit();

            List<Object> result = table.searchWithRegexp("Ki.*");
            Assert.assertEquals(result.size(), 2);

        }catch (Exception e) {
            Assert.fail();
        }
    }


    @Test
    public void transactionTest_COMMIT() {
        try(DBGeneric db = DBFactory.getGenericDB()) {
            Table table = db.useTable(dbFileName, DOG_SCHEMA, Dog.class);
            table.beginTransaction();

            Dog dog = new Dog("King", 2, "John");
            table.add(dog);
            table.commit();

            List<Object> result = table.searchWithRegexp("Ki.*");
            Assert.assertEquals(result.size(), 1);
            Dog findDog = (Dog)result.get(0);
            Assert.assertEquals(findDog.pname
                    , "King");

        }catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void transactionTest_ROLLBACK() {
        try(DBGeneric db = DBFactory.getGenericDB()) {
            Table table = db.useTable(dbFileName, DOG_SCHEMA, Dog.class);
            table.beginTransaction();

            Dog dog = new Dog("King", 2, "John");
            table.add(dog);
            table.rollback();

            List<Object> result = table.searchWithRegexp("Jo.*");
            Assert.assertEquals(result.size(), 0);
            List<DebugRowInfo> infos = table.listAllRowsWithDebug();
            Assert.assertEquals(infos.size(), 1);


            DebugRowInfo dri = infos.get(0);
            Assert.assertEquals(dri.isTemporary(), false);
            Assert.assertEquals(dri.isDeleted(), true);

        }catch (Exception e) {
            Assert.fail();
        }
    }


    @Test
    public void transactionTest_COMMIT_with_multiple_begin() {
        try(DBGeneric db = DBFactory.getGenericDB()) {
            Table table = db.useTable(dbFileName, DOG_SCHEMA, Dog.class);
            table.beginTransaction();

            Dog dog = new Dog("King", 2, "John");
            table.add(dog);
            table.beginTransaction();
            table.commit();

            List<Object> result = table.searchWithRegexp("Ki.*");
            Assert.assertEquals(result.size(), 1);
            Dog searchResult = (Dog)result.get(0);
            Assert.assertEquals(searchResult.pname
                    , "King");

        }catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void transactionTest_ROLLBACK_with_multiple_begin() {
        try(DBGeneric db = DBFactory.getGenericDB()) {
            Table table = db.useTable(dbFileName, DOG_SCHEMA, Dog.class);
            Dog dog = new Dog("King", 2, "John");
            Dog dog2 = new Dog("King2", 2, "John");

            table.beginTransaction();
            table.add(dog);

            table.beginTransaction();
            table.add(dog2);
            table.rollback();

            List<Object> result = table.searchWithRegexp("Ki.*");
            Assert.assertEquals(result.size(), 0);
            List<DebugRowInfo> infos = table.listAllRowsWithDebug();
            Assert.assertEquals(infos.size(), 2);

            DebugRowInfo dri = infos.get(0);
            Assert.assertEquals(dri.isTemporary(), false);
            Assert.assertEquals(dri.isDeleted(), true);

        }catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testAddPerson() {
        try(DBGeneric db = DBFactory.getGenericDB()) {
            Table table = db.useTable(dbFileNameForPerson, PERSON_SCHEMA, Person.class);

            table.beginTransaction();
            Person p = new Person("John",44, "Berlin", "www-404","This is a description");

            table.add(p);
            table.commit();

            Assert.assertEquals(table.getTotalRecordNumber(), 1);

            Object result = table.search("John");
            Assert.assertNotNull(result);
            Assert.assertTrue(result.getClass().getName().equalsIgnoreCase("com.mixer.raw.Person"));
            Assert.assertEquals(((Person)result).pname, "John");


        } catch (Exception e) {
            Assert.fail();
        }
    }


    @Test
    public void testDBVersion() {
        try(DBGeneric db = DBFactory.getGenericDB()) {
            Table table = db.useTable(dbFileNameForPerson, PERSON_SCHEMA, Person.class);
           String version = table.getTableVersion();
           Assert.assertEquals(version, "0.1");

        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testLoadDataToIndex() {
        try(DBGeneric db = DBFactory.getGenericDB()) {
            Table table = db.useTable(dbFileNameForPerson, PERSON_SCHEMA, Person.class);
            table.beginTransaction();
            Person p1 = new Person("John",44, "Berlin", "www-404","This is a description");
            Person p2 = new Person("John1",44, "Berlin", "www-404","This is a description");
            table.add(p1);
            table.add(p2);
            table.commit();

            Assert.assertEquals(table.getTotalRecordNumber(), 2);
        } catch (Exception e) {
            Assert.fail();
        }

        // reopen database
        try(DBGeneric db = DBFactory.getGenericDB()) {

            Table table = db.useTable(dbFileNameForPerson, PERSON_SCHEMA, Person.class);
            long recNumber = table.getTotalRecordNumber();
            Assert.assertEquals(recNumber, 2);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testDefragmentation() {
        try(DBGeneric db = DBFactory.getGenericDB()) {
            Table table = db.useTable(dbFileNameForPerson, PERSON_SCHEMA, Person.class);
            table.beginTransaction();
            Person p1 = new Person("John",44, "Berlin", "www-404","This is a description");
            Person p2 = new Person("John1",44, "Berlin", "www-404","This is a description");
            Person p3 = new Person("John2",44, "Berlin", "www-404","This is a description");
            table.add(p1);
            table.add(p2);
            table.add(p3);
            table.commit();

            // delete the first record
            table.beginTransaction();
            table.delete(0);
            table.commit();

            // call the defragmentation
            table.defragmentDatabase();
            long recNum = table.getTotalRecordNumber();
            Assert.assertEquals(recNum, 2);
            List<DebugRowInfo> debugList = table.listAllRowsWithDebug();
            Assert.assertEquals(debugList.size(), 2);

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }


    @Test
    public void testROLLBACK_DELETE() {
        try(DBGeneric db = DBFactory.getGenericDB()) {
            Table table = db.useTable(dbFileNameForPerson, PERSON_SCHEMA, Person.class);
            table.beginTransaction();
            Person p1 = new Person("John",44, "Berlin", "www-404","This is a description");
            table.add(p1);
            table.commit();

            // delete the first record
            table.beginTransaction();
            table.delete(0);
            table.rollback();

            // call the defragmentation
            table.defragmentDatabase();
            long recNum = table.getTotalRecordNumber();
            Assert.assertEquals(recNum, 1);

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testNoIndexInformationIsSet() {
        try (DBGeneric db = DBFactory.getGenericDB()) {
            Table table = db.useTable(dbFileNameForPerson, PERSON_SCHEMA_WITHOUT_INDEX_INFO, Person.class);
        }catch(IOException e) {
            Assert.fail();
        }catch (DBException dbe) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void useMultipleTables() {
        try(DBGeneric db = DBFactory.getGenericDB()) {
            Table table = db.useTable(dbFileName, DOG_SCHEMA, Dog.class);
            table.beginTransaction();
            Dog dog = new Dog("King", 2, "John");
            table.add(dog);
            table.commit();

            Table table2 = db.useTable(dbFileNameForPerson, PERSON_SCHEMA, Person.class);
            table2.beginTransaction();
            Person p1 = new Person("John",44, "Berlin", "www-404","This is a description");
            table2.add(p1);
            table2.commit();

            Assert.assertEquals(table.getTotalRecordNumber(), 1);
            Assert.assertEquals(table2.getTotalRecordNumber(), 1);

        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void deleteTableTest() {
        try(DBGeneric db = DBFactory.getGenericDB()) {
            Table table = db.useTable(dbFileName, DOG_SCHEMA, Dog.class);
            table.beginTransaction();
            Dog dog = new Dog("King", 2, "John");
            table.add(dog);
            table.commit();

            boolean result = db.dropCurrentTable();
            Assert.assertTrue(result);

            result = db.tableExists(dbFileName);
            Assert.assertFalse(result);

        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void deleteTableWithNameTest() {
        try(DBGeneric db = DBFactory.getGenericDB()) {
            Table table = db.useTable(dbFileName, DOG_SCHEMA, Dog.class);
            table.beginTransaction();
            Dog dog = new Dog("King", 2, "John");
            table.add(dog);
            table.commit();

            boolean result = db.dropTable(dbFileName);
            Assert.assertTrue(result);

            result = db.tableExists(dbFileName);
            Assert.assertFalse(result);

        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void exportTableToSCVTest() {
        try(DBGeneric db = DBFactory.getGenericDB()) {
            Table table = db.useTable(dbFileName, DOG_SCHEMA, Dog.class);
            table.beginTransaction();
            Dog dog = new Dog("King", 2, "John");
            table.add(dog);
            table.commit();

            String result = db.exportCurrentTableToSCV();
            Assert.assertEquals("King, 2, John", result);

        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void exportTableToSCVWithNameTest() {
        try(DBGeneric db = DBFactory.getGenericDB()) {
            Table table = db.useTable(dbFileName, DOG_SCHEMA, Dog.class);
            table.beginTransaction();
            Dog dog = new Dog("King", 2, "John");
            table.add(dog);
            table.commit();

            String result = db.exportTableToCSV(dbFileName, DOG_SCHEMA, Dog.class);
            Assert.assertEquals("King, 2, John", result);

        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Before
    public void setup() {
        File file = new File(dbFileName);
        if (file.exists())
            file.delete();
        File file2 = new File(dbFileNameForPerson);
        if (file2.exists())
            file2.delete();
    }
}
