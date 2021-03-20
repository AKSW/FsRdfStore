package org.aksw.jena_sparql_api.lock;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.aksw.commons.util.exception.ExceptionUtilsAksw;
import org.apache.commons.lang3.time.StopWatch;

public class ProcessFileLock
	implements Lock
{
	// protected LockManager lockManager;
	protected Path path;
	
	// The thread that owns the lock (if any)
	protected transient Thread thread;

	
	public ProcessFileLock(Path path) {
		super();
		this.path = path;
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
		lockManager.tryCreateLockFile(path, time, unit);
	}

	@Override
	public void unlock() {
		try {
			Files.delete(path);
		} catch (IOException e) {
			throw new RuntimeException(e);
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
