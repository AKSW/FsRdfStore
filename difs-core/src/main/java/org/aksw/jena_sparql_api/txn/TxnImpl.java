package org.aksw.jena_sparql_api.txn;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.aksw.commons.util.strings.StringUtils;

public class TxnImpl {
	protected TxnMgr txnMgr;
	protected String txnId;
	protected Path txnFolder;
	
//	protected String preCommitFilename = ".precommit";
	protected String commitFilename = ".commit";
	protected String rollbackFilename = ".rollback";
	
//	protected transient Path preCommitFile;
//	protected transient Path finalizeCommitFile;
	protected transient Path commitFile;
	protected transient Path rollbackFile;
	
	
	public TxnImpl(TxnMgr txnMgr, String txnId, Path txnFolder) {
		super();
		this.txnMgr = txnMgr;
		this.txnId = txnId;
		this.txnFolder = txnFolder;
		// this.txnFolder = txnMgr.txnBasePath.resolve(txnId);
		
		
		this.commitFile = txnFolder.resolve(commitFilename);
		this.rollbackFile = txnFolder.resolve(rollbackFilename);
	}
	

	public ResourceApi getResourceApi(String resourceName) {
		return new ResourceApi(resourceName);
	}

	public boolean isWrite() {
		boolean result = Files.exists(txnFolder.resolve("write"));
		return result;
	}
	
	/**
	 * Declare that the resource was accessed by the transaction
	 * Upon recovery the resource's state must be checked for whether any actions
	 * need to be taken.
	 * Note, that declaration of access to a resource does not lock it
	 * 
	 * @param resourceName
	 * @throws IOException 
	 */
//	public void declareAccess(String resourceName) throws IOException {
//		String resFilename = StringUtils.urlEncode(resourceName);
//
//		Path resFilePath = txnMgr.resRepo.getRelPath(resourceName);
//		Path resShadowPath = txnMgr.resShadow.getRelPath(resourceName);
//
//		// Declare an access attempt to the resource in the txn's journal
//		Path journalEntryFile = txnFolder.resolve(resFilename);
//
//		Path resShadowBasePath = txnMgr.resShadow.getRootPath();
//		Path resShadowAbsPath = resShadowBasePath.resolve(resShadowPath);
//
//		/// Path journalResTgt = txnJournalFolder.relativize(journalEntryName); // TODO generate another id
//		Files.createSymbolicLink(journalEntryFile, resShadowAbsPath.relativize(txnFolder));
//	}
	
	
//	public Lock getResourceLock(String resourceName, boolean isWrite) throws IOException {
//		String resFilename = StringUtils.urlEncode(resourceName);
//
//		Path resFilePath = txnMgr.resRepo.getRelPath(resourceName);
//		Path resShadowPath = txnMgr.resShadow.getRelPath(resourceName);
//		
//		// Path txnFile = txnFolder.resolve(resFilename);
//	}
	
	
	public void addCommit() throws IOException {
		Files.createFile(commitFile);		
	}

		
	public void addRollback() throws IOException {
		Files.createFile(rollbackFile);		
	}

	
	/** Stream the resources to which access has been declared */
	public Stream<Path> streamAccessedEntries() throws IOException {
//        PathMatcher pathMatcher = txnFolder.getFileSystem().getPathMatcher("glob:**/.*");
//        		
//	    List<Path> tmp = Files.list(txnFolder).filter(p -> {
//	    	boolean r = pathMatcher.matches(p);
//	    	return r;
//	    })
//	    .collect(Collectors.toList());
	    	
	    return Files.list(txnFolder)
	    		.filter(path -> path.getFileName().toString().startsWith("."));
	}

	public Stream<String> streamAccessedResources() throws IOException {
		return streamAccessedEntries()
			.map(path -> path.getFileName().toString())
			.map(name -> name.substring(1)) // Remove leading '.'
			.map(StringUtils::urlDecode);
	}

	
	public static <T> T repeatWithLock(
			int retryCount,
			int delayInMs,
			Supplier<? extends Lock> lockSupplier,
			Callable<T> action) {
		
		T result = null;
		int retryAttempt ;
		for (retryAttempt = 0; retryAttempt < retryCount; ++retryAttempt) {
			try {
				result = runWithLock(lockSupplier, action);
				break;
			} catch (Exception e) {
				if (retryAttempt + 1 == retryCount) {
					throw new RuntimeException(e);
				} else {
					try {
						Thread.sleep(delayInMs);
					} catch (Exception e2) {
						throw new RuntimeException(e2);
					}
				}
			}
		}
		return result;
	}
	
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

	
	/*
	public class LockImpl
		extends LockBase
	{
		protected String resourceName;
		protected boolean isWrite;
		
		public LockImpl(String resourceName, boolean isWrite) {
			super();
			this.resourceName = resourceName;
			this.isWrite = isWrite;
		}

		@Override
		public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
			Path resFilePath = txnMgr.resRepo.getRelPath(resourceName);
			Path resShadowPath = txnMgr.resShadow.getRelPath(resourceName);
			
			String resFilename = StringUtils.urlEncode(resourceName);
			
			// Path txnFile = txnFolder.resolve(resFilename);
			TxnLockImpl.tryLock(
					txnMgr.lockMgr,
					resFilename,
					txnId,
					resFilePath,
					txnMgr.txnBasePath,
					resShadowPath,
					txnMgr.resRepo.getRootPath(),
					txnMgr.resShadow.getRootPath(),
					isWrite);
		}

		@Override
		public void unlock() {
			TxnLockImpl.unlock			
		}
	}
	*/
		

	
	public class ResourceApi
		implements TxnComponent
	{
		protected String resourceName;
		protected String resFilename;
		
		protected Path resFilePath;
		protected Path resShadowPath;

		// Declare an access attempt to the resource in the txn's journal
		protected Path journalEntryFile;

		protected Path resShadowBasePath;
		protected Path resShadowAbsPath;
		
		protected Path mgmtLockPath;
		protected Path writeLockFile;
		protected Path readLockFile;
		
		protected FileSync fileSync;
		
		public ResourceApi(String resourceName) {
			this.resourceName = resourceName;
			
			resFilePath = txnMgr.resRepo.getRelPath(resourceName);
			resShadowPath = txnMgr.resShadow.getRelPath(resourceName);
			
			resFilename = StringUtils.urlEncode(resourceName);
			
			journalEntryFile = txnFolder.resolve("." + resFilename);

			resShadowBasePath = txnMgr.resShadow.getRootPath();
			resShadowAbsPath = resShadowBasePath.resolve(resShadowPath);
			
			mgmtLockPath = resShadowPath.resolve("mgmt.lock");
			
			String readLockFileName = "txn-" + txnId + "read.lock";
			readLockFile = resShadowAbsPath.resolve(readLockFileName);

			
			writeLockFile = resShadowAbsPath.resolve("write.lock");
			
			fileSync = FileSync.create(resFilePath);
		}
		
		public void declareAccess() {
			Path resShadowAbsPath = resShadowBasePath.resolve(resShadowPath);

			/// Path journalResTgt = txnJournalFolder.relativize(journalEntryName); // TODO generate another id
			
			try {
				Files.createSymbolicLink(journalEntryFile, txnFolder.relativize(resShadowAbsPath));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}			
		}
		
		public boolean ownsWriteLock() {
			try {
				Path txnLink = Files.readSymbolicLink(writeLockFile);
				Path txnAbsLink = writeLockFile.getParent().resolve(txnLink).toAbsolutePath().normalize();
				
				boolean result = txnAbsLink.equals(txnFolder);
				return result;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		
		public Lock getMgmtLock() {
			Lock result = txnMgr.lockMgr.getLock(mgmtLockPath, true);
			return result;
			
		}
		
		// Whether this txn owns the lock
		public boolean isLockedHere() throws Exception {
			Path txnLink = Files.readSymbolicLink(writeLockFile);
			
			boolean result = txnLink.equals(txnFolder);
			
			if (!result) {
				txnLink = Files.readSymbolicLink(readLockFile);
				result = txnLink.equals(txnFolder);
			}
			
			return result;
		}

		
		public Stream<Path> getReadLocks() throws IOException {
	        PathMatcher pathMatcher = resShadowBasePath.getFileSystem().getPathMatcher("glob:*.read.lock");
	        		
		     return Files.exists(resShadowAbsPath)
		    		? Files.list(resShadowAbsPath).filter(pathMatcher::matches)
		    		: Stream.empty();
		}
		
		public void lock(boolean write) {
			repeatWithLock(10, 100, this::getMgmtLock, () -> {
				// Path writeLockPath = resShadowAbsPath.resolve("write.lock");
				if (Files.exists(writeLockFile)) {
					throw new RuntimeException("Write lock already exitsts at " + writeLockFile);
				}				
				
				if (!write) { // read lock requested
				    // TODO add another read lock entry that points to the txn
					// String readLockFileName = "txn-" + txnId + ".read.lock";
					// Path readLockFile = resShadowPath.resolve(readLockFileName);
					
					// Use the read lock to link back to the txn that own it
					Files.createSymbolicLink(readLockFile, txnFolder);					
				} else {
					boolean existsReadLock;
					try (Stream<Path> stream = getReadLocks()) {
						existsReadLock = stream.findAny().isPresent();
					}
					
				    if (existsReadLock) {
						throw new RuntimeException("Read lock already exitsts at " + writeLockFile);
				    } else {
				    	// Create a write lock file that links to the txn folder
				    	Files.createDirectories(writeLockFile.getParent());
						Files.createSymbolicLink(writeLockFile, writeLockFile.getParent().relativize(txnFolder));
				    	// Files.createFile(resourceShadowPath.resolve("write.lock"), null);
				    }
				}
				return null;
			});
			
		}
		
		public void unlock() {
			repeatWithLock(10, 100, this::getMgmtLock, () -> {
				Files.deleteIfExists(writeLockFile);
				Files.deleteIfExists(readLockFile);
				return null;
			});
			
		}
		
		public FileSync getFileSync() {
			return fileSync;
		}

//		public InputStream openContent() {
//			fileSync.
//		}
		
		public void putContent(Consumer<OutputStream> handler) throws IOException {
			fileSync.putContent(handler);
		}

		@Override
		public void preCommit() throws Exception {
			fileSync.preCommit();
		}

		@Override
		public void finalizeCommit() throws Exception {
			fileSync.finalizeCommit();
		}

		@Override
		public void rollback() throws Exception {
			fileSync.rollback();
		}
	}
}
	
	
//	
//	/**
//	 * Try to acquire a read or write lock (depending on the argument) for a certain resource.
//	 * This process creates a short-lived management lock first: Other processes are assumed to not modify the
//	 * set of read and write locks while the management lock is held.
//	 * 
//	 * @param lockMgr
//	 * @param txnId Used to link back from a resource to the txn that owns a read or write lock
//	 * @param txnFolder
//	 * @param resourceName
//	 * @param write
//	 * @throws IOException 
//	 */
//	public static Runnable tryLock(
//			long timeout,
//			TimeUnit timeUnit,
//			boolean write) throws IOException {
//
//		Path resPhysAbsPath = resPath.resolve(resPhysBasePath);
//		Path resRel = resPhysBasePath.relativize(resPhysAbsPath);
//
//		Path resTxnRelPath = txnBasePath.resolve(journalEntryName); // TODO Turn the resource name or resource rel path into a shadow file / path
//		Path resTxnAbsPath = resPath.relativize(txnJournalFolder);
//
//		
//		Path resShadowPath = null;
//		Path resShadowAbsPath = resShadowBasePath.resolve(resShadowPath);
//		
//				
//		// Declare an access attempt to the resource in the txn's journal
//		// Path journalEntryFile = txnJournalFolder.resolve(journalEntryName);
//		/// Path journalResTgt = txnJournalFolder.relativize(journalEntryName); // TODO generate another id
//
//		Files.createSymbolicLink(journalEntryFile, resShadowAbsPath.relativize(txnJournalFolder));
//
//		// Try to acquire the management lock on the resource
//		Path mgmtLockPath = resShadowPath.resolve("mgmt.lock");
//
//
//		int retryAttempt ;
//		for (retryAttempt = 0; retryAttempt < 100; ++retryAttempt) {
//			Lock mgmtLock = null;
//			try {
//				mgmtLock = lockMgr.getLock(mgmtLockPath, true);
//
//				Path writeLockPath = resShadowPath.resolve("write.lock");
//				if (Files.exists(writeLockPath)) {
//					continue;
//				}
//				
//
//				// Get read locks
//				// Path readLockFolder = resourceShadowPath;
//				// boolean existsReadLock = 
//				
//				if (!write) { // read lock requested
//				    // TODO add another read lock entry that points to the txn
//					String readLockFileName = "txn-" + txnId + "read.lock";
//					Path readLockFile = resShadowPath.resolve(readLockFileName);
//					
//					// Use the read lock to link back to the txn that own it
//					Files.createSymbolicLink(readLockFile, txnJournalFolder);					
//				} else {
//					boolean existsReadLock = true;
//				    if (existsReadLock) {
//				    	continue;
//				    } else {
//				    	// Create a write lock file that links to the txn folder
//						Files.createSymbolicLink(writeLockPath, txnJournalFolder);
//
//						
//						Runnable unlockAction = () -> {
//							runWithLock(mgmtLockSuplier, () -> {
//								
//								if (write) {
//									Files.delete(writeLockPath);
//								}
//
//								
//								// Finally remove the link from the txn to the lock
//								Files.delete()
//							});								
//						};
//
//				    	// Files.createFile(resourceShadowPath.resolve("write.lock"), null);
//				    }
//				}
//				
//			} catch (Exception e) {
//				// TODO Decide whether to retry or abort
//				throw new RuntimeException(e);
//			}
//			finally {
//				if (mgmtLock != null) {
//					mgmtLock.unlock();
//				}
//			}
//			
//			// TODO Delay before next iteration
//			// TODO Abort if timeout or retry limit reached
//			try {
//				Thread.sleep(100);
//			} catch (Exception e) {
//				throw new RuntimeException(e);
//			}
//		}
//
//		// We now own a process file lock on the resource
//		
//				
//		// Point back from the resource shadow to the transaction		
////		String txnId = txnFolder.getFileName().getFileName().toString();
////		Files.createSymbolicLink(resourceShadow.resolve(txnId), txnFolder);
//	}
//		
//
//}
//

