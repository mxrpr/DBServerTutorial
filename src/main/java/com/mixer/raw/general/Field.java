package com.mixer.raw.general;

public final class Field {
    public String fieldName;
    public String fieldType;

    @Override
    public String toString() {
        return String.format("Field name: %s, field type: %s", fieldName, fieldType);
    }
}
