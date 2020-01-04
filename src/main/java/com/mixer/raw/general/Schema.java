package com.mixer.raw.general;

import java.util.LinkedList;

/**
 * An Schema object represents what type of object we would like to
 * store in database. Contains the fields, the field name which has to be
 * used to index the table and the version.
 */
@SuppressWarnings("unused")
final class Schema {
    public String version;
    public LinkedList<Field> fields;
    public String indexBy;
}
