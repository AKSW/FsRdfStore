package org.aksw.jena_sparql_api.lock.db.impl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.aksw.commons.io.util.PathUtils;
import org.aksw.commons.io.util.symlink.SymbolicLinkStrategy;
import org.aksw.jena_sparql_api.lock.db.api.ReadWriteLockWithOwnership;
import org.aksw.jena_sparql_api.lock.db.api.LockStore;
import org.aksw.jena_sparql_api.lock.db.api.ResourceLock;
import org.aksw.jena_sparql_api.txn.ResourceRepository;


/**
 * File System based implementation of a {@link LockStore}.
 * 
 * For each resource subject to locking a folder is created in the lock repository.
 * Each read/write lock is represented as a file which then links to a path that represents the owner.
 * Typically the owner-path is the folder where transaction resources are managed.
 * 
 * @author Claus Stadler
 */
public class LockStoreImpl
	implements LockStore<String[], String>
{
	// The pattern matches all of a resource's read locks
	protected String readLockFilenamePattern = "*.read.lock";
	
	// There can only be a single write lock per resource
	protected String writeLockFilename = "write.lock";

	protected String mgmtLockFilename = "mgmt.lock";

	// Strategy for creating files that link to other locations
	// May use native symbolic links or write files containing the links
	protected SymbolicLinkStrategy symbolicLinkStrategy;

	/** Root folder of the lock db */
	protected ResourceRepository<String> lockRepo;

	/** The owner of locks are usually transactions */
	protected Function<String, Path> ownerRepoFactory;

	/** 
	 * Mapping of resources to store folders which enables creation of lock files that are
	 * link to other folders that contain the data being locked 
	 */
	protected ResourceRepository<String> storeRepo;
	

	public LockStoreImpl(
			SymbolicLinkStrategy symbolicLinkStrategy,
			ResourceRepository<String> lockRepo,
			ResourceRepository<String> storeRepo,
			Function<String, Path> ownerRepoFactory) {
		super();
		this.symbolicLinkStrategy = symbolicLinkStrategy;
		this.lockRepo = lockRepo;
		this.storeRepo = storeRepo;
		this.ownerRepoFactory = ownerRepoFactory;
	}

	
	
	@Override
	public ResourceLock<String> getLockForResource(String resource) {
		String[] storeKey = storeRepo.getPathSegments(resource);
		
		String tmpKey = Arrays.asList(storeKey).stream().collect(Collectors.joining("/")); 
		
		String[] lockKey = lockRepo.getPathSegments(tmpKey);
		return getLockByKey(lockKey);
	}
	
	@Override
	public ResourceLock<String> getLockByKey(String[] lockKey) {
		return new ResourceLockImpl(lockKey);
	}
	
//	public ResourceLock<O> getResourceLock(String[] resourceKey) {
//		return new ResourceLockImpl(resourceKey);
//	}

	@Override
	public Stream<ResourceLock<String>> streamResourceLocks() throws IOException {
        // PathMatcher pathMatcher = lockRepo.getRootPath().getFileSystem().getPathMatcher("glob:**");
		
	    return Files.walk(lockRepo.getRootPath())
	        // .filter(pathMatcher::matches)
	        .map(PathUtils::getPathSegments)
	    	.map(this::getLockByKey);
	    		 //.map(rootFolder::relativize)
	    		 
	}
	

	public class ResourceLockImpl
		implements ResourceLock<String>
	{
		/// protected Path resShBasePath; // rootFolder
		protected String[] lockKey;
		protected Path lockAbsPath;
		
		/** The management lock file which when exists prevents modification of
		 *  the read/write lock ownerships by other processes */
		protected Path mgmtLockPath;
		protected Path writeLockPath;

		public ResourceLockImpl(String[] lockKey) {
			super();
			this.lockKey = lockKey; //lockRepo.getPathSegments(resource);
			
			lockAbsPath = PathUtils.resolve(lockRepo.getRootPath(), lockKey);			
			//mgmtLock = new LockFromFile(lockAbsPath.resolve(mgmtLockFilename));
			mgmtLockPath = lockAbsPath.resolve(mgmtLockFilename);
			writeLockPath = lockAbsPath.resolve(writeLockFilename);
		}
		
		@Override
		public String getMgmtLockOwnerKey() {
			String result = getOwnerKey(mgmtLockPath);
			return result;
		}
		
		
//		@Override
//		public LockFromFile getMgmtLock() {
//			return mgmtLock;
//		}

		public Path getWriteLock() {
			Path writeLockFile = lockAbsPath.resolve(writeLockFilename);
			return writeLockFile;
		}

		@Override
		public ReadWriteLockWithOwnership get(String ownerKey) {
			// return new LockOwnerImpl(ownerKey);
			return createLockOwner(ownerKey);
		}
		
		public Stream<Path> streamReadLockPaths() throws IOException {
	        PathMatcher pathMatcher = lockRepo.getRootPath().getFileSystem().getPathMatcher("glob:" + readLockFilenamePattern);
			
		     return Files.exists(lockAbsPath)
		    		? Files.list(lockAbsPath).filter(pathMatcher::matches)
		    		: Stream.empty();
		}

		public String linkTargetToKey(Path path) {
			String result = path.getFileName().toString();
			return result;
		}
		
		public String getOwnerKey(Path linkFile) {
			String result;
			try {
				Path target = symbolicLinkStrategy.readSymbolicLink(linkFile);
				result = linkTargetToKey(target);
			} catch (FileNotFoundException e) {
				result = null;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			return result;
		}
		
		@Override
		public Stream<String> streamReadLockOwnerKeys() throws IOException {
			return streamReadLockPaths()
					// .map(path -> path)
					//.map(FileUtilsX::getPathSegments)
					.map(this::getOwnerKey);
		}

		@Override
		public String getWriteLockOwnerKey() throws IOException {
			String result = getOwnerKey(writeLockPath);
			return result;
		}

		protected LockFromLockStore createLockOwner(String ownerKey) {
			// ownerKey = ownerToKey.apply(owner);
			String readLockFileName = ownerKey + ".read.lock";

			Path readLockPath = lockAbsPath.resolve(readLockFileName);
			
			LockFromLink readLock = new LockFromLink(
					symbolicLinkStrategy,
					readLockPath,
					ownerKey,
					ownerRepoFactory,
					// ownerPath -> ownerPath.getFileName().toString()
					ResourceLockImpl.this::linkTargetToKey);

			LockFromLink writeLock = new LockFromLink(
					symbolicLinkStrategy,
					writeLockPath,
					ownerKey,
					// ownerRepo.getRootPath(), Collections.singletonList(key)
					ownerRepoFactory,
					// ownerPath -> ownerPath.getFileName().toString()
					ResourceLockImpl.this::linkTargetToKey
					);

			LockFromLink mgmtLock = new LockFromLink(
					symbolicLinkStrategy,
					lockAbsPath.resolve(mgmtLockFilename),
					ownerKey,
					ownerRepoFactory,
					// ownerPath -> ownerPath.getFileName().toString()
					ResourceLockImpl.this::linkTargetToKey
					);
			
			return new LockFromLockStore(this, ownerKey, mgmtLock, readLock, writeLock);
		}

		
		/**
		 * Owner-centric lock API
		 * 
		 * Acquire read / write locks for a specific owner and test whether a lock is owned
		 * by the owner that corresponds to this instance.
		 */
//		public class LockOwnerImpl
//			extends LockFromLockStore
//			implements LockOwner {
//
//			public LockOwnerImpl(String ownerKey) {
//
//			}
//
//		}
	}
}
