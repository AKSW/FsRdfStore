package org.aksw.jena_sparql_api.lock;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * A lock that is made up of multiple locks.
 * For {@link #tryLock(long, TimeUnit)} to succeed this method must succeed for all child locks.
 * On failure any acquired locks are unlocked again.
 * 
 * @author raven
 *
 */
public class CompoundLock
	extends LockBase
{
	// The list of locks must not change after init
	protected List<? extends Lock> locks;

	protected int heldLocks = 0;	
	
	public CompoundLock(List<? extends Lock> locks) {
		super();
		if (locks.isEmpty()) {
			throw new IllegalArgumentException("The set of locks must not be empty");
		}
		
		this.locks = locks;
		this.heldLocks = 0;
	}
	
	// TODO More synchronization is needed; otherwise the heldLocks stuff may brake under concurrent access
	@Override
	public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
		boolean result;
		long startTime = System.nanoTime();
		
		long allowedTime = TimeUnit.NANOSECONDS.convert(time, unit);
		try {
			for (int i = 0; i < locks.size(); ++i) {
				boolean isLastLock = i + 1 == locks.size();
				Lock lock = locks.get(i);
				
				boolean success = lock.tryLock(time, unit);
				++heldLocks;
				
				long elapsedTime = System.nanoTime() - startTime;
				if (!success || (elapsedTime > allowedTime && !isLastLock)) {
					unlock();
					break;
				}
			}
		} catch (Exception e) {
			unlock();
			// throw new RuntimeException(e);
		}

		result = heldLocks == locks.size();
		
		return result;
	}

	@Override
	public void unlock() {
		synchronized (this) {
			for (int i = heldLocks - 1; i >= 0; --i) {
				Lock lock = locks.get(i);
				lock.unlock();
			}
			
			heldLocks = 0;
		}
	}
}
