package org.aksw.jena_sparql_api.lock;

import java.util.concurrent.TimeUnit;

public abstract class LockBaseRepeat
	extends LockBase
{
	@Override
	public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
		long ms = unit.toMillis(time);
		long retryIntervalInMs = 100;
		long retryCount = (ms / retryIntervalInMs) + (ms % retryIntervalInMs == 0 ? 0 : 1);
		
		boolean result = RetryUtils.simpleRetry(retryCount, retryIntervalInMs, () -> {
			return singleLockAttempt();
		});
		
		return result;
	}

	protected boolean runLockAttempt() { return false; }
	protected abstract boolean singleLockAttempt() throws InterruptedException;
}
