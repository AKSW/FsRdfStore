package org.aksw.jena_sparql_api.difs.main;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.aksw.jena_sparql_api.lock.LockManager;
import org.aksw.jena_sparql_api.lock.LockManagerCompound;
import org.aksw.jena_sparql_api.lock.LockManagerPath;
import org.aksw.jena_sparql_api.lock.ThreadLockManager;
import org.aksw.jena_sparql_api.txn.TxnMgr;
import org.apache.jena.sparql.core.DatasetGraph;

public class DifsFactory {
	protected Path repoRootPath;
	
	public static DifsFactory newInstance() {
		return new DifsFactory();
	}
	
	public DifsFactory setPath(Path repoRootPath) {
		this.repoRootPath = repoRootPath;
		return this;
	}
	
	public DatasetGraph connect() throws IOException {
		// If the repo does not yet exist then run init which
		// creates default conf files
		
		Files.createDirectories(repoRootPath);
		Path txnStore = Files.createDirectories(repoRootPath.resolve("txns"));
		
		LockManager<Path> processLockMgr = new LockManagerPath(repoRootPath);
		LockManager<Path> threadLockMgr = new ThreadLockManager<>();
		
		LockManager<Path> lockMgr = new LockManagerCompound<>(Arrays.asList(processLockMgr, threadLockMgr));
		
		ResourceRepoImpl resStore = new ResourceRepoImpl(repoRootPath.resolve("store"));
		ResourceRepoImpl resShadow = new ResourceRepoImpl(repoRootPath.resolve("shadow"));
		
		TxnMgr txnMgr = new TxnMgr(lockMgr, txnStore, resStore, resShadow);

		return new DatasetGraphFromTxnMgr(txnMgr);
		// TODO Read configuration file if it exists
		// return DatasetGraphFromFileSystem.create(repoRootPath, lockMgr);
	}
}
