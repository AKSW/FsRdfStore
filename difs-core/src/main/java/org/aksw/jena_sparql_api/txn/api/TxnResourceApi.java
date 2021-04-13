package org.aksw.jena_sparql_api.txn.api;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.time.Instant;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.aksw.jena_sparql_api.txn.FileSync;
import org.aksw.jena_sparql_api.txn.FileUtilsX;

//public interface TxnResourceApi {
//	public Instant getLastModifiedDate() throws IOException {
//		return fileSync.getLastModifiedTime();
//	}
//	
//	public Path getResFilePath() {
//		return resFilePath;
//	};
//	
//
//	public boolean isVisible() {
//		boolean result;
//		
//		if (isLockedHere()) {
//			result = true;
//		} else {				
//			Instant txnTime = getCreationInstant();
//			Instant resTime;
//			try {
//				resTime = fileSync.getLastModifiedTime();
//			} catch (IOException e) {
//				throw new RuntimeException(e);
//			}
//
//			// If the resource's modified time is null then it did not exist yet
//			result = resTime != null && resTime.isBefore(txnTime); 
//		}
//
//		return result;
//	}
//	
//	public boolean isPersistent() {
//		return true;
//	}
//	
//	public void declareAccess() {
//		if (isPersistent()) {
//			declareAccessCore();
//		}
//	}
//	
//	public void declareAccessCore() {
//		// Path actualLinkTarget = txnFolder.relativize(resShadowAbsPath);
//		Path actualLinkTarget = txnFolder.relativize(resFileAbsPath);
//		try {
//			if (Files.exists(journalEntryFile, LinkOption.NOFOLLOW_LINKS)) {
//				// Verify
//				Path link = txnMgr.symlinkStrategy.readSymbolicLink(journalEntryFile);
//				if (!link.equals(actualLinkTarget)) {
//					throw new RuntimeException(String.format("Validation failed: Attempted to declare access to %s but a different %s already existed ", actualLinkTarget, link));
//				}
//				
//			} else {
//				logger.debug("Declaring access from " + journalEntryFile + " to " + actualLinkTarget);
//				txnMgr.symlinkStrategy.createSymbolicLink(journalEntryFile, actualLinkTarget);
//			}
//		} catch (IOException e) {
//			throw new RuntimeException(e);
//		}			
//	}
//
//	public void undeclareAccess() {
//		if (isPersistent()) {
//			undeclareAccessCore();
//		}
//	}
//
//	public void undeclareAccessCore() {
//		try {
//			// TODO Use delete instead and log an exception?
//			Files.deleteIfExists(journalEntryFile);
//		} catch (IOException e) {
//			throw new RuntimeException(e);
//		}			
//	}
//
//	public boolean ownsWriteLock() {
//		boolean result;
//		try {
//			Path txnLink = txnMgr.symlinkStrategy.readSymbolicLink(writeLockFile);
//			Path txnAbsLink = writeLockFile.getParent().resolve(txnLink).toAbsolutePath().normalize();
//			
//			result = txnAbsLink.equals(txnFolder);
//		} catch (NoSuchFileException e) {
//			result = false;
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}
//		return result;
//	}
//
//	public boolean ownsReadLock() {
//		boolean result;
//		try {
//			Path txnLink = txnMgr.symlinkStrategy.readSymbolicLink(readLockFile);
//			Path txnAbsLink = readLockFile.getParent().resolve(txnLink).toAbsolutePath().normalize();
//			
//			result = txnAbsLink.equals(txnFolder);
//		} catch (NoSuchFileException e) {
//			result = false;
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}
//		return result;
//	}
//
//	public Lock getMgmtLock() {
//		Lock result = txnMgr.lockMgr.getLock(mgmtLockPath, true);
//		return result;
//		
//	}
//	
//	// Whether this txn owns the lock
//	public boolean isLockedHere() {
//		boolean result = ownsWriteLock() || ownsReadLock();			
//		return result;
//	}
//
//	
//	public Stream<Path> getReadLocks() throws IOException {
//        PathMatcher pathMatcher = resShadowBasePath.getFileSystem().getPathMatcher("glob:*.read.lock");
//        		
//	     return Files.exists(resShadowAbsPath)
//	    		? Files.list(resShadowAbsPath).filter(pathMatcher::matches)
//	    		: Stream.empty();
//	}
//	
//	public void lock(boolean write) {
//		// Check whether we already own the lock
//		boolean ownsR = ownsReadLock();
//		boolean ownsW = ownsWriteLock();
//
//
//		boolean needLock = true;
//		
//		if (ownsR) {
//			if (write) {
//				unlock();
//			} else {
//				needLock = false;
//			}
//		} else if (ownsW) {
//			needLock = false;
//		}
//
//		if (needLock) {
//			repeatWithLock(10, 100, this::getMgmtLock, () -> {
//				// Path writeLockPath = resShadowAbsPath.resolve("write.lock");
//				if (Files.exists(writeLockFile)) {
//					throw new RuntimeException("Write lock already exitsts at " + writeLockFile);
//				}				
//				
//				if (!write) { // read lock requested
//				    // TODO add another read lock entry that points to the txn
//					// String readLockFileName = "txn-" + txnId + ".read.lock";
//					// Path readLockFile = resShadowPath.resolve(readLockFileName);
//					
//					// Use the read lock to link back to the txn that owns it
//			    	Files.createDirectories(readLockFile.getParent());
//			    	txnMgr.symlinkStrategy.createSymbolicLink(readLockFile, readLockFile.getParent().relativize(txnFolder));					
//				} else {
//					boolean existsReadLock;
//					try (Stream<Path> stream = getReadLocks()) {
//						existsReadLock = stream.findAny().isPresent();
//					}
//					
//				    if (existsReadLock) {
//						throw new RuntimeException("Read lock already exitsts at " + writeLockFile);
//				    } else {
//				    	// Create a write lock file that links to the txn folder
//				    	Files.createDirectories(writeLockFile.getParent());
//				    	txnMgr.symlinkStrategy.createSymbolicLink(writeLockFile, writeLockFile.getParent().relativize(txnFolder));
//				    	// Files.createFile(resourceShadowPath.resolve("write.lock"), null);
//				    }
//				}
//				return null;
//			});
//		}
//	}
//	
//	public void unlock() {
//		repeatWithLock(10, 100, this::getMgmtLock, () -> {
//			Files.deleteIfExists(writeLockFile);
//			Files.deleteIfExists(readLockFile);
//			return null;
//		});
//		
//		// If the resource shadow folder is empty try to delete the folder
//		FileUtilsX.deleteEmptyFolders(resShadowAbsPath, resShadowBasePath);
//	}
//	
//	public FileSync getFileSync() {
//		return fileSync;
//	}
//
////	public InputStream openContent() {
////		fileSync.
////	}
//	
//	public void putContent(Consumer<OutputStream> handler) throws IOException {
//		throw new UnsupportedOperationException();
//	}
//
//	@Override
//	public void preCommit() throws Exception {
//		fileSync.preCommit();
//	}
//
//	@Override
//	public void finalizeCommit() throws Exception {
//		fileSync.finalizeCommit();
//	}
//
//	@Override
//	public void rollback() throws Exception {
//		fileSync.rollback();
//	}
//}
