package com.mixer.util;

/**
 * OperationUnit class has role in transaction handling. Each time we update/insert/delete from
 * the database, the deletedRow or the added row must be known.
 * During the transaction we store all modified records in a list - in other words, we have a list of OperationUnits.
 * @see com.mixer.transaction.ITransaction
 */
public final class OperationUnit {
    public long deletedRowPosition;
    public long addedRowPosition;
}
