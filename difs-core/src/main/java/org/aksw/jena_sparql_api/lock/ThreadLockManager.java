package org.aksw.jena_sparql_api.lock;

import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * Map of some resource to a lock for that resource.
 * 
 * @author Claus Stadler
 *
 * @param <T>
 */
public class ThreadLockManager<T>
	implements LockManager<T>
{
	protected Map<T, ReadWriteLock> resourceToLock = Collections.synchronizedMap(new WeakHashMap<>());
	
	@Override
	public Lock getLock(T resource, boolean write) {
		ReadWriteLock tmp = resourceToLock.computeIfAbsent(resource, r -> new ReentrantReadWriteLock());
		Lock result = write ? tmp.writeLock() : tmp.readLock();
		return result;
	}

}
