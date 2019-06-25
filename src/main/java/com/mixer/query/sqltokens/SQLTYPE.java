package com.mixer.query.sqltokens;

public enum SQLTYPE {

    SELECT("Select"),
    WHERE("where"),
    AND("and"),
    OR("or"),
    VALUES("values"),
    UPDATE("Update"),
    DELETE("Delete");

    private final String text;

    SQLTYPE(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
}