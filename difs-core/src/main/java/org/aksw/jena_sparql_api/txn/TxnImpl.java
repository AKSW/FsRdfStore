package org.aksw.jena_sparql_api.txn;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class TxnImpl {
	private static final Logger logger = LoggerFactory.getLogger(TxnImpl.class);
	
	protected TxnMgr txnMgr;
	protected String txnId;
	protected Path txnFolder;
	
//	protected String preCommitFilename = ".precommit";
	protected String commitFilename = "commit";
	protected String finalizeFilename = "finalize";
	protected String rollbackFilename = "rollback";
	
//	protected transient Path preCommitFile;
//	protected transient Path finalizeCommitFile;
	protected transient Path commitFile;
	protected transient Path finalizeFile;
	protected transient Path rollbackFile;	
	
	protected LoadingCache<Path, ResourceApi> containerCache = CacheBuilder.newBuilder()
			.maximumSize(1000)
			.build(new CacheLoader<Path, ResourceApi>() {
				@Override
				public ResourceApi load(Path key) throws Exception {
					return new ResourceApi(key);
				}		
			});
			
	
	public TxnImpl(TxnMgr txnMgr, String txnId, Path txnFolder) {
		super();
		this.txnMgr = txnMgr;
		this.txnId = txnId;
		this.txnFolder = txnFolder;
		// this.txnFolder = txnMgr.txnBasePath.resolve(txnId);
		
		
		this.commitFile = txnFolder.resolve(commitFilename);
		this.finalizeFile = txnFolder.resolve(finalizeFilename);
		this.rollbackFile = txnFolder.resolve(rollbackFilename);
	}

	
	public Stream<ResourceApi> listVisibleFiles() {
        
		// TODO This pure listing of file resources should probably go to the repository
		PathMatcher pathMatcher = txnMgr.getResRepo().getRootPath().getFileSystem().getPathMatcher("glob:**/*.trig");

	    List<ResourceApi> result;
	    try (Stream<Path> tmp = Files.walk(txnMgr.getResRepo().getRootPath())) {
	    	// TODO Filter out graphs that were created after the transaction start
	        result = tmp
		            .filter(pathMatcher::matches)
		            // We are interested in the folder - not the file itself: Get the parent
		            .map(path -> path.getParent())
		            .map(path -> txnMgr.resRepo.getRootPath().relativize(path))
		            .map(relPath -> getResourceApi(relPath))
		            .filter(ResourceApi::isVisible)
	        		.collect(Collectors.toList());
	    } catch (IOException e1) {
	    	throw new RuntimeException(e1);
		}
	    // paths.stream().map(path -> )
	    
	    return result.stream();
	}    


	public Instant getCreationInstant() {
		try {
		    BasicFileAttributes attr = Files.readAttributes(txnFolder, BasicFileAttributes.class);
		    FileTime fileTime = attr.creationTime();
		    return fileTime.toInstant();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public ResourceApi getResourceApi(String resourceName) {
		Path relRelPath = txnMgr.getResRepo().getRelPath(resourceName);
		ResourceApi result = getResourceApi(relRelPath);
		return result;
	}

	public ResourceApi getResourceApi(Path resRelPath) {
		ResourceApi result;
		try {
			result = containerCache.get(resRelPath);
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		}
		return result;
	}


//	public ResourceApi getResourceApi(Path relPath) {
//		// return new ResourceApi(resourceName);
//		try {
//			return containerCache.get(relPath);
//		} catch (ExecutionException e) {
//			throw new RuntimeException(e);
//		}
//	}

//	public ResourceApi getResourceApi(String resourceName) {
//		// return new ResourceApi(resourceName);
//		try {
//			return containerCache.get(resourceName);
//		} catch (ExecutionException e) {
//			throw new RuntimeException(e);
//		}
//	}

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
	

	public void cleanUpTxn() throws IOException {
		try {
			Files.deleteIfExists(commitFile);
		} finally {
			try {
				Files.deleteIfExists(finalizeFile);
			} finally {
				try {
					Files.deleteIfExists(rollbackFile);
				} finally {
					try {
						Files.deleteIfExists(txnFolder.resolve("write"));
					} finally {
						FileUtilsX.deleteEmptyFolders(txnFolder, txnMgr.txnBasePath);
					}
				}
			}
		}
	}
	
	public void addCommit() throws IOException {
		Files.createFile(commitFile);		
	}

	public void addFinalize() throws IOException {
		Files.createFile(finalizeFile);		
	}

		
	public void addRollback() throws IOException {
		Files.createFile(rollbackFile);		
	}
	
	public boolean isFinalize() throws IOException {
		return Files.exists(finalizeFile);
	}

	public boolean isCommit() throws IOException {
		return Files.exists(commitFile);
	}

	public boolean isRollback() throws IOException {
		return Files.exists(rollbackFile);
	}
	
	

	
	/**
	 * Stream the resources to which access has been declared
	 * The returned stream must be closed!
	 */
	public Stream<Path> streamAccessedEntries() throws IOException {
//        PathMatcher pathMatcher = txnFolder.getFileSystem().getPathMatcher("glob:**/.*");
//        		
//	    List<Path> tmp = Files.list(txnFolder).filter(p -> {
//	    	boolean r = pathMatcher.matches(p);
//	    	return r;
//	    })
//	    .collect(Collectors.toList());
	    	
	    return Files.list(txnFolder)
	    		.map(path -> txnFolder.resolve(path).toAbsolutePath())
	    		.filter(path -> path.getFileName().toString().startsWith("."));
	}

//	public Stream<String> streamAccessedResources() throws IOException {
//		return streamAccessedEntries()
//			.map(path -> path.getFileName().toString())
//			.map(name -> name.substring(1)) // Remove leading '.'
//			.map(StringUtils::urlDecode);
//	}
	
	public Path getRelPathForJournalEntry(Path txnPath) {
		try {
			Path txnToRes = txnMgr.symlinkStrategy.readSymbolicLink(txnPath);
			Path resAbsPath = txnPath.resolveSibling(txnToRes).normalize();
			Path resRelPath = txnMgr.resRepo.getRootPath().relativize(resAbsPath);
			return resRelPath;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}		
	}

	public Stream<Path> streamAccessedResourcePaths() throws IOException {
		return streamAccessedEntries()
			.map(this::getRelPathForJournalEntry);
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
//		protected String resourceName;
		protected String resFilename;
		
		protected Path resFilePath;
		protected Path resFileAbsPath;
		protected Path resShadowPath;

		// Declare an access attempt to the resource in the txn's journal
		protected Path journalEntryFile;

		protected Path resShadowBasePath;
		protected Path resShadowAbsPath;
		
		protected Path mgmtLockPath;
		protected Path writeLockFile;
		protected Path readLockFile;
		
		protected FileSync fileSync;
		
		//		public ResourceApi(String resourceName) {
			//this.resourceName = resourceName;
		public ResourceApi(Path resFilePath) {
			this.resFilePath = resFilePath;
			//resFilePath = txnMgr.resRepo.getRelPath(resourceName);
			resFileAbsPath = txnMgr.resRepo.getRootPath().resolve(resFilePath);
			
			String containerName = resFilePath.toString();
			
//			resShadowPath = txnMgr.resShadow.getRelPath(resourceName);			
//			resFilename = StringUtils.urlEncode(resourceName);

			resShadowPath = txnMgr.resShadow.getRelPath(containerName);			
			resFilename = resShadowPath.getFileName().toString(); // StringUtils.urlEncode(containerName);

			journalEntryFile = txnFolder.resolve("." + resFilename);

			resShadowBasePath = txnMgr.resShadow.getRootPath();
			resShadowAbsPath = resShadowBasePath.resolve(resShadowPath);
			
			mgmtLockPath = resShadowAbsPath.resolve("mgmt.lock");
			
			String readLockFileName = "txn-" + txnId + "read.lock";
			readLockFile = resShadowAbsPath.resolve(readLockFileName);

			
			writeLockFile = resShadowAbsPath.resolve("write.lock");
			
			
			// TODO HACK - the data.trig should not probably come from elsewhere
			fileSync = FileSync.create(resFileAbsPath.resolve("data.trig"));
		}
		
		public Instant getLastModifiedDate() throws IOException {
			return fileSync.getLastModifiedTime();
		}
		
		public Path getResFilePath() {
			return resFilePath;
		};
		

		public boolean isVisible() {
			boolean result;
			
			if (isLockedHere()) {
				result = true;
			} else {				
				Instant txnTime = getCreationInstant();
				Instant resTime;
				try {
					resTime = fileSync.getLastModifiedTime();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}

				// If the resource's modified time is null then it did not exist yet
				result = resTime != null && resTime.isBefore(txnTime); 
			}

			return result;
		}
		
		public void declareAccess() {
			// Path actualLinkTarget = txnFolder.relativize(resShadowAbsPath);
			Path actualLinkTarget = txnFolder.relativize(resFileAbsPath);
			try {
				if (Files.exists(journalEntryFile, LinkOption.NOFOLLOW_LINKS)) {
					// Verify
					Path link = txnMgr.symlinkStrategy.readSymbolicLink(journalEntryFile);
					if (!link.equals(actualLinkTarget)) {
						throw new RuntimeException(String.format("Validation failed: Attempted to declare access to %s but a different %s already existed ", actualLinkTarget, link));
					}
					
				} else {
					logger.debug("Declaring access from " + journalEntryFile + " to " + actualLinkTarget);
					txnMgr.symlinkStrategy.createSymbolicLink(journalEntryFile, actualLinkTarget);
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}			
		}

		public void undeclareAccess() {
			try {
				// TODO Use delete instead and log an exception?
				Files.deleteIfExists(journalEntryFile);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}			
		}

		public boolean ownsWriteLock() {
			boolean result;
			try {
				Path txnLink = txnMgr.symlinkStrategy.readSymbolicLink(writeLockFile);
				Path txnAbsLink = writeLockFile.getParent().resolve(txnLink).toAbsolutePath().normalize();
				
				result = txnAbsLink.equals(txnFolder);
			} catch (NoSuchFileException e) {
				result = false;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			return result;
		}

		public boolean ownsReadLock() {
			boolean result;
			try {
				Path txnLink = txnMgr.symlinkStrategy.readSymbolicLink(readLockFile);
				Path txnAbsLink = readLockFile.getParent().resolve(txnLink).toAbsolutePath().normalize();
				
				result = txnAbsLink.equals(txnFolder);
			} catch (NoSuchFileException e) {
				result = false;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			return result;
		}

		public Lock getMgmtLock() {
			Lock result = txnMgr.lockMgr.getLock(mgmtLockPath, true);
			return result;
			
		}
		
		// Whether this txn owns the lock
		public boolean isLockedHere() {
			boolean result = ownsWriteLock() || ownsReadLock();			
			return result;
		}

		
		public Stream<Path> getReadLocks() throws IOException {
	        PathMatcher pathMatcher = resShadowBasePath.getFileSystem().getPathMatcher("glob:*.read.lock");
	        		
		     return Files.exists(resShadowAbsPath)
		    		? Files.list(resShadowAbsPath).filter(pathMatcher::matches)
		    		: Stream.empty();
		}
		
		public void lock(boolean write) {
			// Check whether we already own the lock
			boolean ownsR = ownsReadLock();
			boolean ownsW = ownsWriteLock();


			boolean needLock = true;
			
			if (ownsR) {
				if (write) {
					unlock();
				} else {
					needLock = false;
				}
			} else if (ownsW) {
				needLock = false;
			}

			if (needLock) {
				repeatWithLock(10, 100, this::getMgmtLock, () -> {
					// Path writeLockPath = resShadowAbsPath.resolve("write.lock");
					if (Files.exists(writeLockFile)) {
						throw new RuntimeException("Write lock already exitsts at " + writeLockFile);
					}				
					
					if (!write) { // read lock requested
					    // TODO add another read lock entry that points to the txn
						// String readLockFileName = "txn-" + txnId + ".read.lock";
						// Path readLockFile = resShadowPath.resolve(readLockFileName);
						
						// Use the read lock to link back to the txn that owns it
				    	Files.createDirectories(readLockFile.getParent());
				    	txnMgr.symlinkStrategy.createSymbolicLink(readLockFile, readLockFile.getParent().relativize(txnFolder));					
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
					    	txnMgr.symlinkStrategy.createSymbolicLink(writeLockFile, writeLockFile.getParent().relativize(txnFolder));
					    	// Files.createFile(resourceShadowPath.resolve("write.lock"), null);
					    }
					}
					return null;
				});
			}
		}
		
		public void unlock() {
			repeatWithLock(10, 100, this::getMgmtLock, () -> {
				Files.deleteIfExists(writeLockFile);
				Files.deleteIfExists(readLockFile);
				return null;
			});
			
			// If the resource shadow folder is empty try to delete the folder
			FileUtilsX.deleteEmptyFolders(resShadowAbsPath, resShadowBasePath);
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

