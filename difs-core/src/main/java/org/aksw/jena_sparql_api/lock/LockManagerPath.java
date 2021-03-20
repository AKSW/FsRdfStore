package org.aksw.jena_sparql_api.lock;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.aksw.commons.util.exception.ExceptionUtilsAksw;
import org.apache.commons.lang3.time.StopWatch;

public class LockManagerPath
	implements LockManager<Path>
{
	protected Map<Path, ProcessFileLock> pathToLock = new ConcurrentHashMap<>();
	
	public Lock getLock(Path path, boolean write) {
		return pathToLock.computeIfAbsent(path, p -> new ProcessFileLock(p));
	}
	
	public static boolean tryCreateLockFile(Path path, long time, TimeUnit unit) {
		boolean result;
		
		// TODO Check if the path is already locked by this thread on the manager
		Path parentPath = path.getParent();
		
		StopWatch sw = StopWatch.createStarted();
		for (;;) {
			try {
				Files.createDirectories(parentPath);
			} catch (IOException e2) {
				throw new RuntimeException(e2);
			}
			
			try {
				Files.createFile(path);
				result = true;
			} catch (IOException e) {
				ExceptionUtilsAksw.rethrowUnless(e, ExceptionUtilsAksw.isRootCauseInstanceOf(FileAlreadyExistsException.class));

				long elapsed = sw.getTime(unit);
				if (elapsed >= time) {
					result = false;
					break;
				}
				
				long retryIntervalInMs = 100;
				long timeInMs = TimeUnit.MILLISECONDS.convert(time, unit);
				long remainingTimeInMs = Math.min(retryIntervalInMs, timeInMs);
				try {
					Thread.sleep(remainingTimeInMs);
				} catch (InterruptedException e1) {
					throw new RuntimeException(e1);
				}
			}			
		}

		return result;
	}

}
