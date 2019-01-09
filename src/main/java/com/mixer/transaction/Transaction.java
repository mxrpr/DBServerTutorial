package com.mixer.transaction;

import java.util.LinkedList;
import java.util.List;

public final class Transaction implements ITransaction {
    private final double uid;
    private final LinkedList<Long> newRows;
    private final LinkedList<Long> deletedRows;

    public Transaction() {
        this.uid = getRandomNumber();
        this.newRows = new LinkedList<>();
        this.deletedRows = new LinkedList<>();
    }

    @Override
    public double getUid() {
        return this.uid;
    }

    @Override
    public void registerAdd(long position) {
        this.newRows.add(position);
    }

    @Override
    public void registerDelete(long position) {
        this.deletedRows.add(position);
    }

    @Override
    public List<Long> getNewRows() {
        return this.newRows;
    }

    @Override
    public List<Long> getDeletedRows() {
        return this.deletedRows;
    }

    @Override
    public void clear() {
        this.deletedRows.clear();
        this.newRows.clear();
    }


    private double getRandomNumber() {
        long max = Long.MAX_VALUE;
        long min = 1;
        return (Math.random() * ((max - min) + 1)) + min;
    }
}
