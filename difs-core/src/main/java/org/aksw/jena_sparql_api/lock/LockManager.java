package org.aksw.jena_sparql_api.lock;

import java.util.concurrent.locks.Lock;

public interface LockManager<T> {
	Lock getLock(T resource, boolean write);
}
