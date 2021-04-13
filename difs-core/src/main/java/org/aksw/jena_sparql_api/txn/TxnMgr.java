package org.aksw.jena_sparql_api.txn;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import org.aksw.common.io.util.symlink.SymbolicLinkStrategy;
import org.aksw.jena_sparql_api.lock.LockManager;
import org.aksw.jena_sparql_api.lock.db.api.LockStore;
import org.aksw.jena_sparql_api.lock.db.impl.LockStoreImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Path-based transaction manager:
 * Implements a protocol for that uses folders and symlinks to manage locks on paths (files and directories).
 * The protocol is aimed at allowing multiple processes to run transactions on a common pool of paths
 * with serializable isolation level (the highest).
 * 
 * Conceptually, three folder structures are involved:
 * The ./store contains the actual data files.
 * The ./txns folder holds a subfolder for each active transaction.
 *   Whenever a resource is accessed within a transaction a symlink from the txn folder to the accessed' resource's shadow is created.
 * The ./shadow is the 'shadow' of the store. For each resource in the store that is involved in a transaction
 *   a folder is created. Upon read or write access lock files are created in the shadow that link to the transaction folder
 *   that owns the lock.
 *  
 * The purpose of the shadow is two fold:
 *   (1) It prevents writing all the management files into the store which causes pollution in case a process
 *       running a transaction exits (or loses connection to the store) without clean up.
 *   (2) While resources in ./store can be arbitrarily nested in subfolders, in ./shadow those entries are 'flattened'
 *       by url encoding their relative path. This makes management somewhat easier because locking resources does not have
 *       to deal with corner cases where one process wants to create a sub folder of an empty folder X and another process wants to
 *       clean up and remove X.
 * 
 * store/org/example/data.nt
 * 
 * shadow/.org-example-data/data.nt -&gt store/org/example/data.nt
 * shadow/.org-example-data/txn-12345.lock -&gt txns/txn-12345
 *
 *  
 * txns/txn-12345/
 * 
 * 
 * @author Claus Stadler
 *
 */
public class TxnMgr {
	private static final Logger logger = LoggerFactory.getLogger(TxnMgr.class);
	
	protected LockManager<Path> lockMgr;
	protected Path txnBasePath;
	protected ResourceRepository<String> resRepo;
	protected ResourceRepository<String> lockRepo;
	
	protected LockStore<String[], String> lockStore;

	
	protected SymbolicLinkStrategy symlinkStrategy;

	public TxnMgr(
			LockManager<Path> lockMgr,
			Path txnBasePath,
			ResourceRepository<String> resRepo,
			ResourceRepository<String> lockRepo,
			SymbolicLinkStrategy symlinkStrategy) {
		super();
		this.lockMgr = lockMgr;
		this.txnBasePath = txnBasePath;
		this.resRepo = resRepo;
		this.lockRepo = lockRepo;
		this.symlinkStrategy = symlinkStrategy;
		
		lockStore = new LockStoreImpl(symlinkStrategy, lockRepo, resRepo, txnId -> txnBasePath.resolve(txnId));
	}
	
	/**
	 * Build a bipartite graph between dependencies and locks; i.e.
	 * 
	 * Read the locks of the given transactions
	 */
	public void buildLockGraph() {
		
	}
	
	
	public SymbolicLinkStrategy getSymlinkStrategy() {
		return symlinkStrategy;
	}



	public ResourceRepository<String> getResRepo() {
		return resRepo;
	}

	public TxnImpl newTxn(boolean isWrite) {
		String txnId = "txn-" + new Random().nextLong();

		Path txnFolder = txnBasePath.resolve(txnId);

		try {
			Files.createDirectories(txnFolder);

			if (isWrite) {
				Files.createFile(txnFolder.resolve("write"));
			}
		} catch (IOException e) {
			throw new RuntimeException("Failed to lock txn folder");
		}

		logger.debug("Allocated txn folder" + txnFolder);
		return new TxnImpl(this, txnId, txnFolder);
	}
}
