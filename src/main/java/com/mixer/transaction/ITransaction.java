package com.mixer.transaction;

import java.util.List;

public interface ITransaction {

    double getUid();

    void registerAdd(long position);

    void registerDelete(long position);

    List<Long> getNewRows();

    List<Long> getDeletedRows();

    void clear();
}
