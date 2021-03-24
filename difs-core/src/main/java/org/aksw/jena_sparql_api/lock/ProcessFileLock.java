package org.aksw.jena_sparql_api.lock;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class ProcessFileLock
	implements Lock
{
	protected Path path;
	
	// The thread that owns the lock (if any)
	protected transient Thread thread;

	
	// public ProcessFileLock(LockManager<Path> lockManager, Path relPath) {
	public ProcessFileLock(Path path) {
		super();
		this.thread = null;
	}

	@Override
	public void lock() {
		tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
	}

	/**
	 * First, attempt to create the process lock file.
	 * If the manager already owns it then this step succeeds immediately without further waiting.
	 * 
	 * Afterwards, attempt to get the thread lock
	 * 
	 */
	@Override
	public boolean tryLock(long time, TimeUnit unit) {
		Thread currentThread = Thread.currentThread();
		
		boolean result;
		if (thread == null) {		
			result = LockManagerPath.tryCreateLockFile(path, time, unit);
			thread = Thread.currentThread();
		} else if (thread == currentThread) {
			result = true;
		} else {
			throw new RuntimeException("Attempt to re-lock a lock instance from a different thread");
		}
		
		return result;
	}

	@Override
	public void unlock() {
		if (thread != null) {
			try {
				Files.delete(path);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			thread = null;
		}
	}

	
	@Override
	public void lockInterruptibly() throws InterruptedException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean tryLock() {
		throw new UnsupportedOperationException();
	}


	@Override
	public Condition newCondition() {
		throw new UnsupportedOperationException();
	}
}
