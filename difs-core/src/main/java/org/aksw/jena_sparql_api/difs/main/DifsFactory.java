package org.aksw.jena_sparql_api.difs.main;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.function.Function;

import org.aksw.jena_sparql_api.dataset.file.DatasetGraphIndexPlugin;
import org.aksw.jena_sparql_api.dataset.file.DatasetGraphIndexerFromFileSystem;
import org.aksw.jena_sparql_api.lock.LockManager;
import org.aksw.jena_sparql_api.lock.LockManagerCompound;
import org.aksw.jena_sparql_api.lock.LockManagerPath;
import org.aksw.jena_sparql_api.lock.ThreadLockManager;
import org.aksw.jena_sparql_api.txn.ResourceRepository;
import org.aksw.jena_sparql_api.txn.TxnMgr;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.DatasetGraph;

public class DifsFactory {
	protected Path repoRootPath;
	protected Collection<DatasetGraphIndexPlugin> indexers;
	
	public static DifsFactory newInstance() {
		return new DifsFactory();
	}
	
	public DifsFactory setPath(Path repoRootPath) {
		this.repoRootPath = repoRootPath;
		this.indexers = new LinkedHashSet<>();
		return this;
	}
	
	public DifsFactory addIndex(Node predicate, String name, Function<Node, Path> objectToPath) throws IOException {
//        raw, DCTerms.identifier.asNode(),
//        path = Paths.get("/tmp/graphtest/index/by-id"),
//        DatasetGraphIndexerFromFileSystem::mavenStringToToPath
		Path indexFolder = repoRootPath.resolve("index").resolve(name);
		Files.createDirectories(indexFolder);
		
		ResourceRepository<String> resStore = ResourceRepoImpl.createWithUriToPath(repoRootPath.resolve("store"));

		indexers.add(new DatasetGraphIndexerFromFileSystem(
				resStore,
				predicate,
				indexFolder,
				objectToPath));
		
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
		
		ResourceRepository<String> resStore = ResourceRepoImpl.createWithUriToPath(repoRootPath.resolve("store"));
		ResourceRepository<String> resShadow = ResourceRepoImpl.createWithUrlEncode(repoRootPath.resolve("shadow"));
		
		TxnMgr txnMgr = new TxnMgr(lockMgr, txnStore, resStore, resShadow);

		return new DatasetGraphFromTxnMgr(txnMgr, indexers);
		// TODO Read configuration file if it exists
		// return DatasetGraphFromFileSystem.create(repoRootPath, lockMgr);
	}
}
