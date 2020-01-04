package com.mixer.transaction;

import java.util.List;

/**
 * This interface represents a transaction.
 * The class which implements this interface must store the added and deleted records
 *
 */
public interface ITransaction {

    double getUid();

    void registerAdd(long position);

    void registerDelete(long position);

    List<Long> getNewRows();

    List<Long> getDeletedRows();

    void clear();
}
