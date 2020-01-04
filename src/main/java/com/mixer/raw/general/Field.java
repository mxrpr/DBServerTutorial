package com.mixer.raw.general;

/**
 * Field class represents a field in the object which we would like to store in the
 * database. A field object contains the name of the field and the field type.
 * These information are required in order to can store the properties of the given object into
 * the database
 */
@SuppressWarnings("unused")
final class Field {
    public String fieldName;
    public String fieldType;

    @Override
    public String toString() {
        return String.format("Field name: %s, field type: %s", fieldName, fieldType);
    }
}
