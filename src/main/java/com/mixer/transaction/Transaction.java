package com.mixer.transaction;

import java.util.LinkedList;
import java.util.List;

/**
 * Class which represents a transaction. When we handle transactions in the database we use
 * this object. This object stores all added and deleted rows during the db operations.
 * When we commit the changes, the transaction object knows which rows has been added or deleted,
 * it knows the exact places of the rows in the file, so it can "finalize" the operation.
 * It is also possible to "undo" the operation - this is just a boolean in the row structure.
 */
public final class Transaction implements ITransaction {
    // each transaction must have a unique id.
    private final double uid;
    // the added new rows
    private final LinkedList<Long> newRows;
    // the deleted rows
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
