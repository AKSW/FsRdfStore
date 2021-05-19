package org.aksw.jena_sparql_api.txn;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.aksw.commons.io.util.PathUtils;
import org.aksw.commons.util.array.Array;
import org.aksw.jena_sparql_api.txn.api.TxnResourceApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TxnSerializable
	extends TxnReadUncommitted
{
	private static final Logger logger = LoggerFactory.getLogger(TxnSerializable.class);
	
	protected TxnMgrImpl txnMgr;
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

	@Override
	public String getId() {
		return txnId;
	}

//	protected IsolationLevel isolationLevel;

	@Override
	protected TxnResourceApi createResourceApi(String[] key) {
		return new TxnResourceApiSerializable(this, key);
	}

//	protected LockStore<String[], String> lockStore;
	
//	protected LoadingCache<String[], TxnResourceApi> containerCache = CacheBuilder.newBuilder()
//			.maximumSize(1000)
//			.build(new CacheLoader<String[], TxnResourceApi>() {
//				@Override
//				public TxnResourceApi load(String[] key) throws Exception {
//					return new TxnResourceApiSerializable(key);
//				}		
//			});
//			
	
	public TxnSerializable(
			TxnMgrImpl txnMgr,
			String txnId,
			Path txnFolder) {
		super(txnMgr, txnId);
		this.txnMgr = txnMgr;
		this.txnId = txnId;
		this.txnFolder = txnFolder;
		// this.txnFolder = txnMgr.txnBasePath.resolve(txnId);
		
		
		this.commitFile = txnFolder.resolve(commitFilename);
		this.finalizeFile = txnFolder.resolve(finalizeFilename);
		this.rollbackFile = txnFolder.resolve(rollbackFilename);
		
		
//		lockStore = new LockStoreImpl(txnMgr.symlinkStrategy, txnMgr.lockRepo, txnMgr.resRepo);

	}
	
	
//	@Override
//	public Stream<TxnResourceApi> listVisibleFiles() {
//        
//		// TODO This pure listing of file resources should probably go to the repository
//		PathMatcher pathMatcher = txnMgr.getResRepo().getRootPath().getFileSystem().getPathMatcher("glob:**/*.trig");
//
//	    List<TxnResourceApi> result;
//	    try (Stream<Path> tmp = Files.walk(txnMgr.getResRepo().getRootPath())) {
//	    	// TODO Filter out graphs that were created after the transaction start
//	        result = tmp
//		            .filter(pathMatcher::matches)
//		            // We are interested in the folder - not the file itself: Get the parent
//		            .map(Path::getParent)
//		            .map(path -> txnMgr.resRepo.getRootPath().relativize(path))
//		            .map(PathUtils::getPathSegments)
//		            .map(this::getResourceApi)
//		            .filter(TxnResourceApi::isVisible)
//	        		.collect(Collectors.toList());
//	    } catch (IOException e1) {
//	    	throw new RuntimeException(e1);
//		}
//	    // paths.stream().map(path -> )
//	    
//	    return result.stream();
//	}    


	@Override
	public Instant getCreationDate() {
		try {
		    BasicFileAttributes attr = Files.readAttributes(txnFolder, BasicFileAttributes.class);
		    FileTime fileTime = attr.creationTime();
		    
		    if (fileTime == null) {
		    	logger.warn("Failed to obtain creation time of " + txnFolder + " falling back to last modified date");
		    	fileTime = Files.getLastModifiedTime(txnFolder);
		    }
		    
		    return fileTime.toInstant();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public TxnResourceApi getResourceApi(String resourceName) {
		String[] relRelPath = txnMgr.getResRepo().getPathSegments(resourceName);
		TxnResourceApi result = getResourceApi(relRelPath);
		return result;
	}

	@Override
	public TxnResourceApi getResourceApi(String[] resRelPath) {
		TxnResourceApi result;
		try {
			result = containerCache.get(Array.wrap(resRelPath));
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

	@Override
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
		Files.newOutputStream(commitFile, StandardOpenOption.CREATE).close();
//		Files.createFile(commitFile);		
	}

	public void addFinalize() throws IOException {
		Files.newOutputStream(finalizeFile, StandardOpenOption.CREATE).close();
//		Files.createFile(finalizeFile);		
	}

		
	public void addRollback() throws IOException {
		Files.newOutputStream(rollbackFile, StandardOpenOption.CREATE).close();
		//Files.createFile(rollbackFile);		
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
	
	public String[] getRelPathForJournalEntry(Path txnPath) {
		try {
			Path txnToRes = txnMgr.symlinkStrategy.readSymbolicLink(txnPath);
			Path resAbsPath = txnPath.resolveSibling(txnToRes).normalize();
			Path resRelPath = txnMgr.resRepo.getRootPath().relativize(resAbsPath);
			String[] result = PathUtils.getPathSegments(resRelPath);
			return result;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}		
	}

	@Override
	public Stream<String[]> streamAccessedResourcePaths() throws IOException {
		return streamAccessedEntries()
			.map(this::getRelPathForJournalEntry);
	}

	
	@Override
	public Instant getActivityDate() throws IOException {
		FileTime timestamp = Files.getLastModifiedTime(txnFolder, LinkOption.NOFOLLOW_LINKS);
		Instant result = timestamp.toInstant();
		return result;
	}
	
	@Override
	public void setActivityDate(Instant instant) throws IOException {
		FileTime timestamp = FileTime.from(instant);
		Files.setLastModifiedTime(txnFolder, timestamp);
	}
	
	
	@Override
	public boolean isStale() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void claim() {
		throw new UnsupportedOperationException();
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

	
//	/**
//	 * Api to a resource w.r.t. a transaction.
//	 * 
//	 * 
//	 * @author raven
//	 *
//	 */
//	public class TxnResourceApiReadUncommitted
//		implements TxnResourceApi
//	{
//		protected String[] resKey;
//		// protected String resFilename;
//		
//		protected Path resFilePath;
//		protected Path resFileAbsPath;
//
//		// Declare an access attempt to the resource in the txn's journal
//		protected Path journalEntryFile;
//
//
//		protected ResourceLock<String> resourceLock;
//		protected LockOwner txnResourceLock;
//
//		protected FileSync fileSync;
//		
//		//		public ResourceApi(String resourceName) {
//			//this.resourceName = resourceName;
//		public TxnResourceApiReadUncommitted(String[] resKey) {// Path resFilePath) {
//			this.resKey = resKey;
//			// this.resFilePath = resFilePath;
//			//resFilePath = txnMgr.resRepo.getRelPath(resourceName);
//			
//			String resKeyStr = PathUtils.join(resKey);
//			resourceLock = txnMgr.lockStore.getLockForResource(resKeyStr);
//			txnResourceLock = resourceLock.get(txnId);
//			
//			resFileAbsPath = PathUtils.resolve(txnMgr.resRepo.getRootPath(), resKey);
//
//			
//			String[] resLockKey = txnMgr.lockRepo.getPathSegments(resKeyStr);
//			String resLockKeyStr = PathUtils.join(resLockKey);
//			
//			
////			resShadowPath = txnMgr.resShadow.getRelPath(resourceName);			
////			resFilename = StringUtils.urlEncode(resourceName);
//
//			journalEntryFile = txnFolder.resolve("." + resLockKeyStr);
//			// String readLockFileName = "txn-" + txnId + "read.lock";
//			
//			// TODO HACK - the data.trig should probably come from elsewhere
//			fileSync = FileSync.create(resFileAbsPath.resolve("data.trig"));
//		}
//		
//		@Override
//		public LockOwner getTxnResourceLock() {
//			return new LockOwnerDummy();
//		}
//		
//		@Override
//		public Instant getLastModifiedDate() throws IOException {
//			return fileSync.getLastModifiedTime();
//		}
//
//		@Override
//		public String[] getResourceKey() {
//			return resKey;
//		}
//		
////		public Path getResFilePath() {
////			return resFilePath;
////		};
//		
//
//		@Override
//		public boolean isVisible() {
//			return true;
//		}
//
//		@Override
//		public void declareAccess() {
//		}
//
//
//		@Override
//		public void undeclareAccess() {
//		}
//
//		@Override
//		public FileSync getFileSync() {
//			return fileSync;
//		}
//		
//		public void putContent(Consumer<OutputStream> handler) throws IOException {
//			fileSync.putContent(handler);
//		}
//
//		@Override
//		public void preCommit() throws Exception {
//			fileSync.preCommit();
//		}
//
//		@Override
//		public void finalizeCommit() throws Exception {
//			fileSync.finalizeCommit();
//		}
//
//		@Override
//		public void rollback() throws Exception {
//			fileSync.rollback();
//		}
//	}
//	
//	

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

// Whether this txn owns the lock
//public boolean isLockedHere() {
//	boolean result = txnResourceLock.ownsWriteLock() || txnResourceLock.ownsReadLock();			
//	return result;
//}


//public Stream<Path> getReadLocks() throws IOException {
//    PathMatcher pathMatcher = resShadowBasePath.getFileSystem().getPathMatcher("glob:*.read.lock");
//    		
//     return Files.exists(resShadowAbsPath)
//    		? Files.list(resShadowAbsPath).filter(pathMatcher::matches)
//    		: Stream.empty();
//}