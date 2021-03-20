package org.aksw.jena_sparql_api.lock;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class CompoundLock
	implements Lock
{
	protected List<? extends Lock> locks;

	protected int heldLocks = 0;	
	
	public CompoundLock(List<? extends Lock> locks) {
		super();
		this.locks = locks;
		this.heldLocks = 0;
	}

	@Override
	public void lock() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean tryLock() {
		// TODO Auto-generated method stub
		return false;
	}

	
	// FIXME The locksHeld stuff is not properly implemented yet
	@Override
	public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
		long start = System.nanoTime();
		
		long allowedTime = TimeUnit.NANOSECONDS.convert(time, unit);
		try {
			for (int i = 0; i < locks.size(); ++i) {
				boolean isLastLock = i + 1 == locks.size();
				Lock lock = locks.get(i);
				
				boolean success = lock.tryLock(time, unit);
				
				long elapsedTime = System.nanoTime() - allowedTime;
				if (!success || (elapsedTime > allowedTime && !isLastLock)) {
					for (int j = 0; i <= i; ++i) {
						locks.get(j).unlock();
					}
				}
				
				++i;
			}
		}		
		
	}

	@Override
	public void unlock() {
		synchronized (this) {
			for (int i = 0; i < heldLocks; ++i) {
				Lock lock = locks.get(i);
				lock.unlock();
			}
			
			heldLocks = 0;
		}
	}

	@Override
	public Condition newCondition() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void lockInterruptibly() throws InterruptedException {
		throw new UnsupportedOperationException();
	}
}
