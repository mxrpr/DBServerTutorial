
import com.mixer.dbserver.DB;
import com.mixer.dbserver.DBFactory;
import com.mixer.dbserver.DBServer;
import com.mixer.raw.Person;
import com.mixer.raw.specific.Index;
import com.mixer.util.DebugRowInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;


public class DBBasicTests {
    private final String dbFileName = "testdb.db";


    @Test
    public void testAdd() {
        try(DB db = DBFactory.getSpecificDB(dbFileName)) {
            db.beginTransaction();
            Person p =  new Person("Jonh",44, "Berlin", "www-404","This is a description");
            db.add(p);
            db.commit();
            Assert.assertEquals(Index.getInstance().getTotalNumberOfRows(), 1);

        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testRead() {
        try(DB db = DBFactory.getSpecificDB(dbFileName)) {
            Person p =new Person("John",44, "Berlin", "www-404","This is a description");

            db.beginTransaction();
            db.add(p);
            db.commit();

            Assert.assertEquals(Index.getInstance().getTotalNumberOfRows(), 1);
            Person person = db.read(0);
            Assert.assertTrue(person.pname.equals("John"));
            Assert.assertTrue(person.address.equals("Berlin") );
            Assert.assertTrue(person.carplatenumber.equals("www-404"));
            Assert.assertTrue(person.description.equals("This is a description"));

        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testDelete() {
        try(DB db = DBFactory.getSpecificDB(dbFileName)) {
            Person p = new Person("Jonh",44, "Berlin", "www-404","This is a description");
            db.beginTransaction();
            db.add(p);
            db.commit();
            Assert.assertEquals(Index.getInstance().getTotalNumberOfRows(), 1);
            db.beginTransaction();
            db.delete(0);
            db.commit();
            Assert.assertEquals(Index.getInstance().getTotalNumberOfRows(), 0);

        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void updateByName() {
        try(DB db = DBFactory.getSpecificDB(dbFileName)) {
            Person p = new Person("John",44, "Berlin", "www-404","This is a description");
            db.beginTransaction();
            db.add(p);
            db.commit();

            Person p2 = new Person("John2",44, "Berlin", "www-404","This is a description");
            db.beginTransaction();
            db.update("John", p2);
            db.commit();

            Person result = db.read(1);
            Assert.assertEquals(result.pname, "John2");

        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void updateByRowNumber() {
        try(DB db = DBFactory.getSpecificDB(dbFileName)) {
            Person p = new Person("John",44, "Berlin", "www-404","This is a description");
            db.beginTransaction();
            db.add(p);
            db.commit();

            Person p2 = new Person("John2",44, "Berlin", "www-404","This is a description");
            db.beginTransaction();
            db.update(0, p2);
            db.commit();

            Person result = db.read(1);
            Assert.assertEquals(result.pname, "John2");

        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testSearch() {
        try(DB db = DBFactory.getSpecificDB(dbFileName)) {
            db.beginTransaction();
            Person p = new Person("John",44, "Berlin", "www-404","This is a description");
            db.add(p);
            Person p2 = new Person("John1",45, "Berlin1", "www-404","This is a description");
            db.add(p2);
            db.commit();

            Person result = db.search("John1");
            Assert.assertEquals("John1", result.pname);
            Assert.assertEquals(45, result.age);
            Assert.assertEquals("Berlin1", result.address);

        }catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testSearchWithLeveinshteinWith_0_tolerance() {
        try(DB db = DBFactory.getSpecificDB(dbFileName)) {
            db.beginTransaction();
            Person p = new Person("John",44, "Berlin", "www-404","This is a description");
            db.add(p);
            Person p2 = new Person("John1",45, "Berlin1", "www-404","This is a description");
            db.add(p2);
            db.commit();

            List<Person> result = db.searchWithLeveinshtein("John", 0);
            Assert.assertEquals(result.size(), 1);
            Assert.assertEquals(result.get(0).pname, "John");

        }catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testSearchWithLeveinshteinWith_1_tolerance() {
        try(DB db = DBFactory.getSpecificDB(dbFileName)) {
            db.beginTransaction();
            Person p = new Person("John",44, "Berlin", "www-404","This is a description");
            db.add(p);
            Person p2 = new Person("John1",45, "Berlin1", "www-404","This is a description");
            db.add(p2);
            db.commit();

            List<Person> result = db.searchWithLeveinshtein("John", 1);
            Assert.assertEquals(result.size(), 2);

        }catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testWithRegexp() {
        try(DB db = DBFactory.getSpecificDB(dbFileName)) {
            Person p = new Person("John",44, "Berlin", "www-404","This is a description");
            db.beginTransaction();
            db.add(p);
            Person p2 = new Person("John1",45, "Berlin1", "www-404","This is a description");
            db.add(p2);
            db.commit();

            List<Person> result = db.searchWithRegexp("Jo.*");
            Assert.assertEquals(result.size(), 2);

        }catch (Exception e) {
            Assert.fail();
        }
    }


    @Test
    public void transactionTest_COMMIT() {
        try(DB db = DBFactory.getSpecificDB(dbFileName)) {
            Person p = new Person("John",44, "Berlin", "www-404","This is a description");
            db.beginTransaction();
            db.add(p);
            db.commit();

            List<Person> result = db.searchWithRegexp("Jo.*");
            Assert.assertEquals(result.size(), 1);
            Person person = result.get(0);
            Assert.assertEquals(person.pname
                    , "John");

        }catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void transactionTest_ROLLBACK() {
        try(DB db = DBFactory.getSpecificDB(dbFileName)) {
            Person p = new Person("John",44, "Berlin", "www-404","This is a description");
            db.beginTransaction();
            db.add(p);
            db.rollback();

            List<Person> result = db.searchWithRegexp("Jo.*");
            Assert.assertEquals(result.size(), 0);
            List<DebugRowInfo> infos = ((DBServer) db).listAllRowsWithDebug();
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
        try(DB db = DBFactory.getSpecificDB(dbFileName)) {
            Person p = new Person("John",44, "Berlin", "www-404","This is a description");
            db.beginTransaction();
            db.add(p);
            db.beginTransaction();
            db.commit();

            List<Person> result = db.searchWithRegexp("Jo.*");
            Assert.assertEquals(result.size(), 1);
            Person person = result.get(0);
            Assert.assertEquals(person.pname
                    , "John");

        }catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void transactionTest_ROLLBACK_with_multiple_begin() {
        try(DB db = DBFactory.getSpecificDB(dbFileName)) {
            Person p = new Person("John",44, "Berlin", "www-404","This is a description");
            Person p2 = new Person("John",44, "Berlin", "www-404","This is a description");

            db.beginTransaction();
            db.add(p);
            db.beginTransaction();
            db.add(p2);
            db.rollback();

            List<Person> result = db.searchWithRegexp("Jo.*");
            Assert.assertEquals(result.size(), 0);
            List<DebugRowInfo> infos = ((DBServer)db).listAllRowsWithDebug();
            Assert.assertEquals(infos.size(), 2);

            DebugRowInfo dri = infos.get(0);
            Assert.assertEquals(dri.isTemporary(), false);
            Assert.assertEquals(dri.isDeleted(), true);

        }catch (Exception e) {
            Assert.fail();
        }
    }


    @Before
    public void setup() {
        File file = new File(dbFileName);
        if (file.exists())
            file.delete();
    }
}
