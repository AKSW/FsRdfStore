package org.aksw.jena_sparql_api.lock.db.impl;

import java.io.FileNotFoundException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Function;

import org.aksw.common.io.util.symlink.SymbolicLinkStrategy;
import org.aksw.jena_sparql_api.txn.FileUtilsX;

public class LockFromLink
//	The semantic of this class is not that of a lock but of a DAO - it adds/removes lock entries to the store
	// extends LockBase
{
	protected SymbolicLinkStrategy linkStrategy;
	protected Path path;
	protected String ownerKey;
	protected Function<String, Path> ownerKeyToTarget;
	protected Function<Path, String> targetToOwnerKey;
	
	public LockFromLink(
			SymbolicLinkStrategy linkStrategy,
			Path path,
			String ownerKey,
			Function<String, Path> ownerKeyToTarget,
			Function<Path, String> targetToOwnerKey) {
		super();
		this.linkStrategy = linkStrategy;
		this.path = path;
		this.ownerKey = ownerKey;
		this.ownerKeyToTarget = ownerKeyToTarget;
		this.targetToOwnerKey = targetToOwnerKey;
	}
	
	
	public Path getPath() {
		return path;
	}

//	@Override
//	public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
//		long ms = unit.toMillis(time);
//		long retryIntervalInMs = 100;
//		long retryCount = (ms / retryIntervalInMs) + (ms % retryIntervalInMs == 0 ? 0 : 1);
//		
//		boolean result = RetryUtils.simpleRetry(retryCount, retryIntervalInMs, () -> {
//			return singleLockAttempt();
//		});
//		
//		return result;
//	}

	public boolean singleLockAttempt() {
		boolean result[] = {false};

		// Try to create the lock file
		try {
			FileUtilsX.ensureParentFolderExists(path, p -> {
				Path targetPath = ownerKeyToTarget.apply(ownerKey);
				try {
					linkStrategy.createSymbolicLink(p, targetPath);
					result[0] = true;
				} catch (FileAlreadyExistsException e) {
					String currentOwnerKey = readOwnerKey();
					
					if (ownerKey.equals(currentOwnerKey)) {
						result[0] = true;
						// Nothing todo; we already own the lock
					} else {
						throw new RuntimeException("Cannot lock for " + ownerKey + " because it is owned by " + currentOwnerKey);
					}
				}
			});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		return result[0];		
	}
	
	// @Override
	public void unlock() {
		String currentOwnerKey = readOwnerKey();
		if (currentOwnerKey != null) {
			if (!ownerKey.equals(currentOwnerKey)) {
				throw new RuntimeException("Cannot unlock for " + ownerKey + " because it is owned by " + currentOwnerKey);
			}
			
			forceUnlock();
		}
	}
	
	// Returns true if an unlock occurred
	public boolean forceUnlock() {
		try {
			boolean result = Files.deleteIfExists(path);
			return result;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public String readOwnerKey() {
		String result;
		try {
			Path target = linkStrategy.readSymbolicLink(path);
			result = targetToOwnerKey.apply(target);
		} catch (FileNotFoundException e) {
			result = null;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return result;
	}
	
	public boolean isOwned() {
		String currentOwnerKey = readOwnerKey();
		boolean result = ownerKey.equals(currentOwnerKey);
		return result;
	}
	
}
