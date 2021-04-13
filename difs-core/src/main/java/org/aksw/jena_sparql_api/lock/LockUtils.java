package org.aksw.jena_sparql_api.lock;

import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;

public interface LockUtils {
	/**
	 * Perform an action which requires acquisition of a lock first.
	 * Repeated attempts are made to acquire the lock. If the lock cannot be acquired then the action
	 * is not run.
	 * 
	 * 
	 * @param <T>
	 * @param retryCount
	 * @param delayInMs
	 * @param lockSupplier
	 * @param action
	 * @return
	 */
	public static <T> T repeatWithLock(
			int retryCount,
			int delayInMs,
			Supplier<? extends Lock> lockSupplier,
			Callable<T> action) {
		T result = RetryUtils.simpleRetry(retryCount, delayInMs, () -> runWithLock(lockSupplier, action));
		return result;
	}
	
	/**
	 * Perform an action which requires acquisition of a lock first.
	 * An attempt is made to acquire the lock. If this fails then the action is not run.
	 * Upon completion of the action (successful or exceptional) the lock is released again.
	 * 
	 */
	public static <T> T runWithLock(Supplier<? extends Lock> lockSupplier, Callable<T> action) {
		T result = null;
		Lock lock = lockSupplier.get();
		try {
			lock.lock();
			result = action.call();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			lock.unlock();
		}
		return result;
	}

}
