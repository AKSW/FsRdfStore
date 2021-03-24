package org.aksw.jena_sparql_api.lock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public abstract class LockBase
	implements Lock
{
	@Override
	public void lock() {
		boolean success = tryLock();
		if (!success) {
			throw new RuntimeException("Could not acquire lock");
		}
	}

	@Override
	public boolean tryLock() {
		boolean result;
		try {
			result = tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		return result;
	}

	@Override
	public void lockInterruptibly() throws InterruptedException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Condition newCondition() {
		throw new UnsupportedOperationException();
	}
}
