package org.aksw.jena_sparql_api.lock;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.locks.Lock;

import org.checkerframework.checker.units.qual.m;

public class TxnLockImpl {
	// protected Path txnFolder;
	
	
	
	/**
	 * Try to acquire a read or write lock (depending on the argument) for a certain resource.
	 * This process creates a short-lived management lock first: Other processes are assumed to not modify the
	 * set of read and write locks while the management lock is held.
	 * 
	 * @param lockMgr
	 * @param txnFolder
	 * @param resourceName
	 * @param write
	 */
	public static void tryLock(LockManager<Path> lockMgr, Path txnFolder, String resourceName, boolean write) {
		

		Path resourceShadowPath = null; // TODO Resolve the resource shadow
		
		Path resourceRelPath = resourceFolder.relativize(txnFolder);
		
		
		Path linkFile = null; // TODO generate another id
		
		
		// Register that the transaction wants to access the resourceShadow
		Files.createSymbolicLink(linkFile, resourceShadow);
		
		
		// Try to aquire the management lock
		Path mgmtLockPath = resourceShadowPath.resolve("mgmt.lock");
		

		for (;;) {
			Lock mgmtLock = null;
			try {
				mgmtLock = lockMgr.getLock(mgmtLockPath, true);
			
				Path writeLockPath = resourceShadowPath.resolve("write.lock");
				if (Files.exists(writeLockPath)) {
					continue;
				}

				// Get read locks
				// Path readLockFolder = resourceShadowPath;
				// boolean existsReadLock = 
				
				if (!write) { // read lock requested
				    // TODO add another read lock entry that points to the txn
				} else {
					boolean existsReadLock = true;
				    if (existsReadLock) {
				    	continue;
				    } else {
				    	Files.createFile(resourceShadowPath.resolve("write.lock"), null);
				    }
				}
				
			} finally {
				if (mgmtLock != null) {
					mgmtLock.unlock();
				}
			}
			
			// TODO Delay before next iteration
			// TODO Abort if timeout or retry limit reached
		}

		// We now own a process file lock on the resource
		
		
		// Point back from the resource shadow to the transaction		
//		String txnId = txnFolder.getFileName().getFileName().toString();
//		Files.createSymbolicLink(resourceShadow.resolve(txnId), txnFolder);
	}
		
}
