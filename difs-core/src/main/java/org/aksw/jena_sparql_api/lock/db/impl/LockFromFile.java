package org.aksw.jena_sparql_api.lock.db.impl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.aksw.commons.lambda.throwing.ThrowingConsumer;
import org.aksw.jena_sparql_api.lock.LockBase;
import org.aksw.jena_sparql_api.lock.LockManagerPath;

// Remove in favor of LockFromLink?
public class LockFromFile
	extends LockBase
{
	protected Path path;
	protected ThrowingConsumer<? super Path> fileCreator;
	
	public LockFromFile(Path path) {
		super();
		this.path = path;
		this.fileCreator = p -> Files.createFile(p); //, StandardOpenOption.CREATE, StandardOpenOption.DSYNC);
	}

	public LockFromFile(Path path, ThrowingConsumer<Path> fileCreator) {
		super();
		this.path = path;
		this.fileCreator = fileCreator;
	}
	
	public Path getPath() {
		return path;
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
		boolean result = LockManagerPath.tryCreateLockFile(path, time, unit);		
		return result;
	}

	@Override
	public void unlock() {
		try {
			Files.deleteIfExists(path);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}	
}
