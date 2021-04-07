package org.aksw.difs.builder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.aksw.common.io.util.symlink.SymlinkStrategy;
import org.aksw.common.io.util.symlink.SymlinkStrategyStandard;
import org.aksw.difs.index.api.RdfTermIndexerFactory;
import org.aksw.difs.sys.vocab.jena.DIFS;
import org.aksw.difs.system.domain.IndexDefinition;
import org.aksw.difs.system.domain.StoreDefinition;
import org.aksw.jena_sparql_api.dataset.file.DatasetGraphIndexPlugin;
import org.aksw.jena_sparql_api.dataset.file.DatasetGraphIndexerFromFileSystem;
import org.aksw.jena_sparql_api.difs.main.DatasetGraphFromTxnMgr;
import org.aksw.jena_sparql_api.difs.main.ResourceRepoImpl;
import org.aksw.jena_sparql_api.lock.LockManager;
import org.aksw.jena_sparql_api.lock.LockManagerCompound;
import org.aksw.jena_sparql_api.lock.LockManagerPath;
import org.aksw.jena_sparql_api.lock.ThreadLockManager;
import org.aksw.jena_sparql_api.txn.ResourceRepository;
import org.aksw.jena_sparql_api.txn.TxnMgr;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.DatasetGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Streams;

/**
 * Builder for a dataset-in-filesystem.
 * 
 * @author raven
 *
 */
public class DifsFactory {
	private static final Logger logger = LoggerFactory.getLogger(DifsFactory.class);

	protected SymlinkStrategy symlinkStrategy;

	protected Path repoRootPath;	
	protected Path storeRelPath;
	protected Path indexRelPath;
	protected Collection<DatasetGraphIndexPlugin> indexers;

	
	public static DifsFactory newInstance() {
		return new DifsFactory();
	}

	public static Stream<Resource> listResources(Model model, Collection<Property> properties) {
		return properties.stream()
			.flatMap(p ->
				Streams.stream(model.listResourcesWithProperty(p)));
	}
	
	public DifsFactory loadFromRdf(String filenameOrIri) {
		Model model = RDFDataMgr.loadModel(filenameOrIri);
		List<Property> mainProperties = Arrays.asList(DIFS.storePath, DIFS.indexPath, DIFS.index);

		Set<Resource> resources = listResources(model, mainProperties).collect(Collectors.toSet());
		
		if (resources.isEmpty()) {
			// Log a warning?
			logger.info("No config resources found in " + filenameOrIri);
		} else if (resources.size() == 1) {
			StoreDefinition def = resources.iterator().next().as(StoreDefinition.class);
			loadFrom(def);
		} else {
			throw new RuntimeException("Multiple configurations detected");
		}
		
		return this;
	}
	
	public DifsFactory loadFrom(StoreDefinition storeDef) {
		if (storeDef.getStorePath() != null) {
			storeRelPath = Paths.get(storeDef.getStorePath());
		}

		if (storeDef.getIndexPath() != null) {
			indexRelPath = Paths.get(storeDef.getIndexPath());
		}

		for (IndexDefinition idxDef : storeDef.getIndexDefinition()) {
			loadIndexDefinition(idxDef);
		}
		
		return this;
	}
	
	public DifsFactory loadIndexDefinition(IndexDefinition idxDef) {
		try {
			Node p = idxDef.getPredicate();
			String folderName = idxDef.getPath();
			String className = idxDef.getMethod();
			Class<?> clz = Class.forName(className);
			Object obj = clz.newInstance();
			RdfTermIndexerFactory indexer = (RdfTermIndexerFactory)obj;		
			addIndex(p, folderName, indexer.getMapper());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return this;
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

		Objects.requireNonNull(symlinkStrategy);
		indexers.add(new DatasetGraphIndexerFromFileSystem(
				symlinkStrategy,
				resStore,
				predicate,
				indexFolder,
				objectToPath));
		
		return this;
	}
	
	
	
	public SymlinkStrategy getSymlinkStrategy() {
		return symlinkStrategy;
	}

	public DifsFactory setSymlinkStrategy(SymlinkStrategy symlinkStrategy) {
		this.symlinkStrategy = symlinkStrategy;
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

		SymlinkStrategy effSymlinkStrategy = symlinkStrategy != null ? symlinkStrategy : new SymlinkStrategyStandard(); 

		TxnMgr txnMgr = new TxnMgr(lockMgr, txnStore, resStore, resShadow, effSymlinkStrategy);
		
		return new DatasetGraphFromTxnMgr(txnMgr, indexers);
		// TODO Read configuration file if it exists
		// return DatasetGraphFromFileSystem.create(repoRootPath, lockMgr);
	}
}
