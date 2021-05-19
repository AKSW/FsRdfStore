package org.aksw.difs.builder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.aksw.common.io.util.symlink.SymbolicLinkStrategy;
import org.aksw.common.io.util.symlink.SymbolicLinkStrategyStandard;
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
import org.aksw.jena_sparql_api.txn.TxnMgrImpl;
import org.aksw.jena_sparql_api.txn.api.TxnMgr;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
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

	protected SymbolicLinkStrategy symbolicLinkStrategy;

	protected Path configFile;	
	protected Path storeRelPath;
	protected Path indexRelPath;
	protected boolean createIfNotExists;
	
	protected StoreDefinition storeDefinition;
	
	protected boolean useJournal = true;

	public DifsFactory() {
		super();
	}
	
	public static DifsFactory newInstance() {
		DifsFactory result = new DifsFactory();
		return result;
	}
	
	public boolean isCreateIfNotExists() {
		return createIfNotExists;
	}

	public DifsFactory setCreateIfNotExists(boolean createIfNotExists) {
		this.createIfNotExists = createIfNotExists;
		return this;
	}

	public static Stream<Resource> listResources(Model model, Collection<Property> properties) {
		return properties.stream()
			.flatMap(p ->
				Streams.stream(model.listResourcesWithProperty(p)));
	}
	
	public StoreDefinition loadStoreDefinition(Path confFilePath) throws IOException {// String filenameOrIri) {
		// TODO Handle local files vs urls 
		/// Path confFilePath = Paths.get(filenameOrIri);
		// repoRootPath = confFilePath.getParent().toAbsolutePath();
		
		String filenameOrIri = confFilePath.getFileName().toString();
		Lang lang = RDFDataMgr.determineLang(filenameOrIri, null, null);
		Model model = ModelFactory.createDefaultModel();
		try (InputStream in = Files.newInputStream(confFilePath)) {
			 RDFDataMgr.read(model, in, lang);
		}
		List<Property> mainProperties = Arrays.asList(DIFS.storePath, DIFS.indexPath, DIFS.index);

		Set<Resource> resources = listResources(model, mainProperties).collect(Collectors.toSet());
		
		StoreDefinition result;
		
		if (resources.isEmpty()) {
			// Log a warning?
			logger.info("No config resources found in " + filenameOrIri);
			result = null;
		} else if (resources.size() == 1) {
			result = resources.iterator().next().as(StoreDefinition.class);
		} else {
			throw new RuntimeException("Multiple configurations detected");
		}
		
		return result;
	}
	
//	public DifsFactory loadFrom(StoreDefinition storeDef) {
//		if (storeDef.getStorePath() != null) {
//			storeRelPath = Paths.get(storeDef.getStorePath());
//		}
//
//		if (storeDef.getIndexPath() != null) {
//			indexRelPath = Paths.get(storeDef.getIndexPath());
//		}
//
//		for (IndexDefinition idxDef : storeDef.getIndexDefinition()) {
//			loadIndexDefinition(idxDef);
//		}
//		
//		return this;
//	}
	
	public DatasetGraphIndexerFromFileSystem loadIndexDefinition(IndexDefinition idxDef) {
		try {
			Node p = idxDef.getPredicate();
			String folderName = idxDef.getPath();
			String className = idxDef.getMethod();
			Class<?> clz = Class.forName(className);
			Object obj = clz.getDeclaredConstructor().newInstance();
			RdfTermIndexerFactory indexer = (RdfTermIndexerFactory)obj;		
			DatasetGraphIndexerFromFileSystem result = addIndex(p, folderName, indexer.getMapper());
			return result;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public DifsFactory setUseJournal(boolean useJournal) {
		this.useJournal = useJournal;
		return this;
	}
	
	public boolean isUseJournal() {
		return useJournal;
	}

		
	public DifsFactory setConfigFile(Path configFile) {
		this.configFile = configFile;
		return this;
	}
	
	public Path getConfigFile() {
		return configFile;
	}
	
	public StoreDefinition getStoreDefinition() {
		return storeDefinition;
	}
	
	public DifsFactory setStoreDefinition(StoreDefinition storeDefinition) {
		this.storeDefinition = storeDefinition;
		return this;
	}
	
	/**
	 * Create a fresh store definition resource (blank node) and pass it to the mutator
	 * 
	 * @param mutator
	 * @return
	 */
	public DifsFactory setStoreDefinition(Consumer<StoreDefinition> mutator) {
		this.storeDefinition = ModelFactory.createDefaultModel().createResource().as(StoreDefinition.class);
		mutator.accept(this.storeDefinition);
		return this;
	}

	
	public DatasetGraphIndexerFromFileSystem addIndex(Node predicate, String name, Function<Node, String[]> objectToPath) throws IOException {
//        raw, DCTerms.identifier.asNode(),
//        path = Paths.get("/tmp/graphtest/index/by-id"),
//        DatasetGraphIndexerFromFileSystem::mavenStringToToPath
		
		Path repoRootPath = configFile.getParent();
		
		Path indexFolder = repoRootPath.resolve("index").resolve(name);
		// Files.createDirectories(indexFolder);
		
		ResourceRepository<String> resStore = ResourceRepoImpl.createWithUriToPath(repoRootPath.resolve("store"));

		Objects.requireNonNull(symbolicLinkStrategy);
		DatasetGraphIndexerFromFileSystem result = new DatasetGraphIndexerFromFileSystem(
				symbolicLinkStrategy,
				resStore,
				predicate,
				indexFolder,
				objectToPath);

		return result;
	}
	
	
	
	public SymbolicLinkStrategy getSymbolicLinkStrategy() {
		return symbolicLinkStrategy;
	}

	public DifsFactory setSymbolicLinkStrategy(SymbolicLinkStrategy symlinkStrategy) {
		this.symbolicLinkStrategy = symlinkStrategy;
		return this;
	}

	
	public StoreDefinition createEffectiveStoreDefinition() throws IOException {

		StoreDefinition effStoreDef;
		if (!Files.exists(configFile)) {
			if (storeDefinition == null) {
				throw new RuntimeException(
						String.format("Config file %s does not exist and no default config was specified", configFile));
			}
			
			try (OutputStream out = Files.newOutputStream(configFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
				RDFDataMgr.write(out, storeDefinition.getModel(), RDFFormat.TURTLE_PRETTY);
			}
			
			effStoreDef = storeDefinition;
		} else {
//			if (Files.isDirectory(configFile)) {
//				configFile = configFile.resolve("store.conf.ttl");
//			}
			
			// Read the config
			effStoreDef = loadStoreDefinition(configFile);
		}
		
		return effStoreDef;
	}

	public TxnMgr createTxnMgr() throws IOException {
		// If the repo does not yet exist then run init which
		// creates default conf files
		
		Path repoRootPath = configFile.getParent();
		
		if (createIfNotExists) {
			Files.createDirectories(repoRootPath);
		}

		// Set up indexers
		
		Path txnStore = repoRootPath.resolve("txns");
		
		LockManager<Path> processLockMgr = new LockManagerPath(repoRootPath);
		LockManager<Path> threadLockMgr = new ThreadLockManager<>();
		
		LockManager<Path> lockMgr = new LockManagerCompound<>(Arrays.asList(processLockMgr, threadLockMgr));
		
		ResourceRepository<String> resStore = ResourceRepoImpl.createWithUriToPath(repoRootPath.resolve("store"));
		ResourceRepository<String> resLocks = ResourceRepoImpl.createWithUrlEncode(repoRootPath.resolve("locks"));

		SymbolicLinkStrategy effSymlinkStrategy = symbolicLinkStrategy != null ? symbolicLinkStrategy : new SymbolicLinkStrategyStandard(); 

		TxnMgr result = new TxnMgrImpl(lockMgr, txnStore, resStore, resLocks, effSymlinkStrategy);

		return result;
	}

	public DatasetGraph connect() throws IOException {
		StoreDefinition effStoreDef = createEffectiveStoreDefinition();
		
		TxnMgr txnMgr = createTxnMgr();
		
		Collection<DatasetGraphIndexPlugin> indexers = effStoreDef.getIndexDefinition().stream()
			.map(this::loadIndexDefinition)
			.collect(Collectors.toList());
		
		return new DatasetGraphFromTxnMgr(useJournal, txnMgr, indexers);
		// TODO Read configuration file if it exists
		// return DatasetGraphFromFileSystem.create(repoRootPath, lockMgr);
	}
}
