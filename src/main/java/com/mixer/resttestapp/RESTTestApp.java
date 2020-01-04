package com.mixer.resttestapp;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SuppressWarnings({"InfiniteLoopStatement", "unused", "UnnecessaryCallToStringValueOf"})
final class RESTTestApp {

    private static class ParameterBuilder {
        String getStringParameters(Map<String, String> parameters) throws UnsupportedEncodingException {
            StringBuilder sb = new StringBuilder(500);
            for (Map.Entry<String, String> entry : parameters.entrySet()) {
                sb.append(URLEncoder.encode(entry.getKey(), "UTF-8"));
                sb.append("=");
                sb.append(URLEncoder.encode(entry.getValue(), "UTF-8"));
                sb.append("&");
            }
            return sb.toString();
        }
    }

    public static void main(String[] args) {
        RESTTestApp app = new RESTTestApp();
        // infinite loop
        while (true) {
            app.performTest();
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void performTest() {
        CountDownLatch latch = new CountDownLatch(3);
        ExecutorService executorService = Executors.newFixedThreadPool(3);

        Runnable searchThread = () -> {
            while (true) {
                try {
                    performSearchTest();
                    Thread.sleep(500);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };

        Runnable listAllRecordsThread = () -> {
            while (true) {
                try {
                    listAllRecors();
                    Thread.sleep(500);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };

        Runnable addPerson = () -> {
            while (true) {
                try {
                    addPerson();
                    Thread.sleep(500);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };

        executorService.submit(searchThread);
//        executorService.submit(listAllRecordsThread);
        executorService.submit(addPerson);

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void doRequest(final String path, Map<String, String> parameters) throws IOException {

        URL url;
        if (parameters != null)
            url =
                    new URL("http://localhost:7001/" + path + "?" + new ParameterBuilder().getStringParameters(parameters));
        else
            url = new URL("http://localhost:7001/" + path);


        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setReadTimeout(1000);
        connection.setDoOutput(false);

        // read the response
        int responseCode = connection.getResponseCode();
        System.out.println("Response code:" + responseCode);
        if (responseCode == 200) {
            System.out.println(readFromStream(connection.getInputStream()));
        }
        connection.disconnect();
    }


    private void addPerson() throws IOException {
        Map<String, String> parameters = new HashMap<>();
        Random rand = new Random();
        int randNumber = rand.nextInt(100000);
        parameters.put("name", "test" + Integer.toString(randNumber));
        parameters.put("age", "33");
        parameters.put("address", "London street 23");
        parameters.put("carplate", "yyy-3344");
        parameters.put("description", "This is a test description");

        this.doRequest("add", parameters);
    }


    private void listAllRecors() throws IOException {
        this.doRequest("listall", null);
    }

    private void performSearchTest() throws IOException {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("name", "test1");

        this.doRequest("searchlevenshtein", parameters);
    }

    private String readFromStream(final InputStream inputStream) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        StringBuilder sb = new StringBuilder(500);
        while ((line = bufferedReader.readLine()) != null) {
            sb.append(line);
        }
        return sb.toString();
    }
}
