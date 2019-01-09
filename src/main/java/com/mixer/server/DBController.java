package com.mixer.server;

import com.mixer.dbserver.DB;
import com.mixer.dbserver.DBServer;
import com.mixer.raw.Person;
import com.mixer.util.DebugRowInfo;
import com.mixer.util.JSONRep;
import io.javalin.Handler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


public final class DBController {
    private static DB DATABASE;

    static {
        try {
            DATABASE = new DBServer("test.db");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void shutdown() throws IOException {
        DATABASE.close();
    }

    public static final Handler fetchAllRecords = ctx -> {
        ArrayList<String> result = new ArrayList<>();

        List<DebugRowInfo> debugResultList= ((DBServer)DATABASE).listAllRowsWithDebug();
        for (DebugRowInfo dri : debugResultList) {
            if (!dri.isDeleted() || !dri.isTemporary())
                result.add(((JSONRep)dri.object()).toJSON());
        }
        ctx.json(result);
    };


    // add?name=test&age=45&address=france&carplate=qqq-123&description=test_entry

    public static final Handler addPerson = ctx -> {

        String name = ctx.queryParam("name");
        int age = Integer.parseInt(ctx.queryParam("age"));
        String address = ctx.queryParam("address");
        String carplate = ctx.queryParam("carplate");
        String description = ctx.queryParam("description");
        if (name == null || address == null || carplate == null || description == null) {
            ctx.json("{\"Error\": \"Parameter is missing\"}");
            return;
        }
        Person p = new Person(name, age, address, carplate, description);
        DATABASE.beginTransaction();
        DATABASE.add(p);
        DATABASE.commit();

        ctx.json(true);
    };

    // searchLevenshtein?name=test
    public static final Handler searchLevenshtein = ctx -> {
        if (ctx.queryParam("name") == null) {
            ctx.json("{\"Error\": \"Mandatory parameters are missing\"}");
        }
        String name = ctx.queryParam("name");
        List<Person> persons =  DATABASE.searchWithLeveinshtein(name, 1);
        LinkedList<String> result = new LinkedList<>();

        persons.forEach(i->{
            result.add(i.toJSON());
        });

        ctx.json(result);
    };

}
