package org.aksw.jena_sparql_api.lock.db.impl;

import java.util.concurrent.locks.Lock;

import org.aksw.jena_sparql_api.lock.db.api.ReadWriteLockWithOwnership;

public class LockOwnerDummy
	implements ReadWriteLockWithOwnership
{
	@Override
	public Lock readLock() {
		return DummyLock.INSTANCE;
	}

	@Override
	public Lock writeLock() {
		return DummyLock.INSTANCE;
	}

	@Override
	public Lock getMgmtLock() {
		return DummyLock.INSTANCE;
	}

	@Override
	public boolean ownsReadLock() {
		return true;
	}

	@Override
	public boolean ownsWriteLock() {
		return true;
	}
}
