package org.aksw.jena_sparql_api.lock.db.api;

import java.io.IOException;
import java.util.stream.Stream;

public interface ResourceLock<O> {
	/**
	 * Get an exclusive lock on the lock which prevents concurent modification
	 * of the state of the read locks or the write lock.
	 * 
	 * This means that the {@link #acquireReadLock(Object)} and {@link #acquireWriteLock(Object)}
	 * methods both need to acquire the management lock first
	 * 
	 * @return
	 */
	O getMgmtLockOwnerKey();
	
	ReadWriteLockWithOwnership get(O ownerKey);
	
	/** Owner to lock 
	 * @throws IOException */
	Stream<O> streamReadLockOwnerKeys() throws IOException;
	
	/** null if the write lock is not owned */
	O getWriteLockOwnerKey() throws IOException;
}

