package com.mixer.util;

public final class DebugRowInfo {
    private final Object object;
    private final boolean isDeleted;
    private final boolean isTemporary;

    public DebugRowInfo(Object object, boolean isDeleted, boolean isTemporary) {
        this.object = object;
        this.isDeleted = isDeleted;
        this.isTemporary = isTemporary;
    }

    public Object object() {
        return this.object;
    }

    public boolean isDeleted() {
        return this.isDeleted;
    }

    public boolean isTemporary() {
        return this.isTemporary;
    }
}
