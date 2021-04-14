package org.aksw.jena_sparql_api.lock.db.impl;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.aksw.jena_sparql_api.lock.LockBase;

public class DummyLock
	extends LockBase
{
	public static final Lock INSTANCE = new DummyLock();
	
	@Override
	public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
		return true;
	}

	@Override
	public void unlock() {
	}
}
