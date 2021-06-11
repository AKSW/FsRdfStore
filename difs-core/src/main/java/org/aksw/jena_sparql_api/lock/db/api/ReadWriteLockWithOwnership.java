package org.aksw.jena_sparql_api.lock.db.api;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

public interface ReadWriteLockWithOwnership
    extends ReadWriteLock
{
    Lock getMgmtLock();

    boolean ownsReadLock();
    boolean ownsWriteLock();

    /** Convenience method to test whether at least one of the locks is owned */
    default boolean isLockedHere() {
        boolean ownsReadLock = ownsReadLock();
        boolean ownsWriteLock = ownsWriteLock();
        boolean result = ownsWriteLock || ownsReadLock;
        return result;
    }
}
