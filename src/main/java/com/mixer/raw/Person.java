package com.mixer.raw;

import com.google.gson.JsonObject;
import com.mixer.raw.general.ICSVRepresentation;
import com.mixer.util.JSONRep;

public class Person implements JSONRep, ICSVRepresentation {
    public String pname;
    public int age;
    public String address;
    public String carplatenumber;
    public String description;

    public Person() {
    }

    public Person(final String name,
                  final int age,
                  final String address,
                  final String carPlateNumber,
                  final String description) {
        this.pname = name;
        this.age = age;
        this.address = address;
        this.carplatenumber = carPlateNumber;
        this.description = description;
    }


    @Override
    public String toString() {
        return String.format("name: %s, age: %d, address:%s, carplate: %s, description: %s",
                this.pname,
                this.age,
                this.address,
                this.carplatenumber, this.description);
    }

    public String toJSON() {
        JsonObject json = new JsonObject();
        json.addProperty("name", this.pname);
        json.addProperty("age", this.age);
        json.addProperty("address", this.address);
        json.addProperty("carplatenumber", this.carplatenumber);
        json.addProperty("description", this.description);

        return json.toString();
    }

    @Override
    public String toSCV() {
        return String.format("%s, %d, %s, %s, %s", this.pname, this.age, this.address, this.carplatenumber, this.description);
    }
}
