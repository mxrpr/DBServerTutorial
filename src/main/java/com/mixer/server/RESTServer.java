package com.mixer.server;

import io.javalin.Javalin;
import io.javalin.JavalinEvent;


final class RESTServer {
    private final Javalin app;

    private RESTServer() {
        app = Javalin.create().port(7001);
        app.event(JavalinEvent.SERVER_STOPPING, DBController::shutdown);
    }

    private void doJob() {
        app.start();
        app.get("/listall", DBController.fetchAllRecords);
        app.get("/add", DBController.addPerson);
        app.get("/searchlevenshtein", DBController.searchLevenshtein);
    }


    public static void main(String[] args) {
        new RESTServer().doJob();
    }

}
