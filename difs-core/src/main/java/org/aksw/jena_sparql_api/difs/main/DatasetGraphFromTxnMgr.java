package org.aksw.jena_sparql_api.difs.main;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.aksw.jena_sparql_api.dataset.file.DatasetGraphIndexPlugin;
import org.aksw.jena_sparql_api.txn.DatasetGraphFromFileSystem;
import org.aksw.jena_sparql_api.txn.FileSync;
import org.aksw.jena_sparql_api.txn.ResourceRepository;
import org.aksw.jena_sparql_api.txn.SyncedDataset;
import org.aksw.jena_sparql_api.txn.TxnImpl;
import org.aksw.jena_sparql_api.txn.TxnImpl.ResourceApi;
import org.aksw.jena_sparql_api.txn.TxnMgr;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.JenaTransactionException;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphBase;
import org.apache.jena.sparql.core.DatasetGraphWrapper;
import org.apache.jena.sparql.core.GraphView;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.system.Txn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Streams;


public class DatasetGraphFromTxnMgr
	extends DatasetGraphBase
{
	protected static final Logger logger = LoggerFactory.getLogger(DatasetGraphFromTxnMgr.class);
	
	protected TxnMgr txnMgr;
	protected ThreadLocal<TxnImpl> txns = ThreadLocal.withInitial(() -> null);
	
	
    protected Collection<DatasetGraphIndexPlugin> indexers = Collections.synchronizedSet(new HashSet<>());

	
//    public static DatasetGraphFromTxnMgr createDefault(Path repoRoot) {
//        PathMatcher pathMatcher = repoRoot.getFileSystem().getPathMatcher("glob:**/*.trig");
//        
//        
//        DatasetGraphFromTxnMgr result = new DatasetGraphFromTxnMgr(
//        		repoRoot,
//                pathMatcher,
//                path -> false);
//
//        return result;
//    }

	
	protected LoadingCache<Path, SyncedDataset> syncCache = CacheBuilder
			.newBuilder()
			.maximumSize(10000)
			.removalListener(ev -> {
				// TODO Sync here or elsewhere?
				// getValue();
			})
			.build(new CacheLoader<Path, SyncedDataset>() {
				@Override
				public SyncedDataset load(Path key) throws Exception {
					ResourceRepository<String> resRepo = txnMgr.getResRepo();
					Path rootPath = resRepo.getRootPath();

					// Path relPath = r// resRepo.getRelPath(key);
					Path absPath = rootPath.resolve(key);					
					FileSync fs = FileSync.create(absPath.resolve("data.trig"));
					
					return new SyncedDataset(fs);
				}
			});
	
	
	public TxnImpl local() {
		return txns.get();
	}
	
	public DatasetGraphFromTxnMgr(TxnMgr txnMgr, Collection<DatasetGraphIndexPlugin> indexers) {
		super();
		this.txnMgr = txnMgr;
		this.indexers = indexers;
	}

	@Override
	public boolean supportsTransactions() {
		return true;
	}

	@Override
	public void begin(TxnType type) {
		begin(TxnType.convert(type));
	}

	@Override
	public void begin(ReadWrite readWrite) {
		TxnImpl txn = txns.get();
		if (txn != null) {
			throw new RuntimeException("Already in a transaction");
		}

		boolean isWrite = ReadWrite.WRITE.equals(readWrite);
		
		txn = txnMgr.newTxn(isWrite);
		txns.set(txn);
	}


	@Override
	public boolean promote(Promote mode) {
		// TODO Auto-generated method stub
		return false;
	}

	/**
	 * Commit first syncs any in-memory changes to temporary files.
	 * Only if this step succeeds a 'commit' journal entry is created which indicates that
	 * persisting of changes succeeded and and is ready to replace existing data.
	 * 
	 * 
	 */
	@Override
	public void commit() {
		
		try {
			Iterator<Path> it = local().streamAccessedResourcePaths().iterator();
			while (it.hasNext()) {
				Path relPath = it.next();
				logger.debug("Syncing: " + relPath);
				// Path relPath = txnMgr.getResRepo().getRelPath(res);

				ResourceApi api = local().getResourceApi(relPath);
				if (api.ownsWriteLock()) {
					// If we own a write lock and the state is dirty then sync
					SyncedDataset synced = syncCache.getIfPresent(relPath);
					if (synced != null) {
						synced.save();
					}
					
					FileSync fs = api.getFileSync();
					if (synced.isDirty()) {
						fs.preCommit();
						synced.updateState();
//						synced.getAdditions().clear();
//						synced.getDeletions().clear();
					}
					
					// The indexers are now run immediately on insert
//					for (DatasetGraphIndexPlugin indexer : indexers) {
//						for (Quad quad : SetFromDatasetGraph.wrap(synced.getDeletions())) {
//							indexer.delete(quad.getGraph(), quad.getSubject(), quad.getPredicate(), quad.getObject());
//						}
//						
//						for (Quad quad : SetFromDatasetGraph.wrap(synced.getAdditions())) {
//							indexer.add(quad.getGraph(), quad.getSubject(), quad.getPredicate(), quad.getObject());
//						}
//					}
				}
			}

			// Once all modified graphs are written out
			// add the statement that the commit action can now be run
			local().addCommit();

			applyJournal();
		} catch (Exception e) {
			try {
				local().addRollback();
			} catch (Exception e2) {
				applyJournal();
				throw new RuntimeException(e2);
			}
			
			applyJournal();
			
			throw new RuntimeException(e);
		}
	}


	protected void applyJournal() {
		boolean isCommit;
		try {
			isCommit = local().isCommit();
		} catch (IOException e1) {
			throw new RuntimeException(e1);
		}
		
		try {

			// Run the finalization actions
			// As these actions remove undo information
			// there is no turning back anymore
			if (isCommit) {
				local().addFinalize();
			}
			
			// TODO Stream the relPaths rather than the string resource names?
			Iterator<Path> it = local().streamAccessedResourcePaths().iterator();
			while (it.hasNext()) {
				Path res = it.next();
				logger.debug("Finalizing and unlocking: " + res);
				ResourceApi api = local().getResourceApi(res);
			
				if (isCommit) {
					api.finalizeCommit();
				} else {
					api.rollback();
				}
				SyncedDataset synced = syncCache.getIfPresent(api.getResFilePath());
				if (synced != null) {
					if (isCommit) {
						synced.getDiff().applyChanges();					
					} else {
						synced.getDiff().clearChanges();
					}
					synced.updateState();
				}
				
				api.unlock();
				api.undeclareAccess();
			}

			local().cleanUpTxn();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void abort() {
		try {
			local().addRollback();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}		
	}

	@Override
	public void end() {
		
		// TODO Apply the changes
		// local().applyChanges();
		
		// Iterate all resources and remove any locks 
//		try {
//			local().streamAccessedResources().forEach(r -> {
//				local().getResourceApi(r).unlock();
//			});
//		} catch (IOException e) {
//			throw new RuntimeException(e);
//		}
		
//		ResourceApi api = local().getResourceApi(iri);
//		api.lock(true);

		txns.remove();
	}

	@Override
	public ReadWrite transactionMode() {
		boolean isWrite = local().isWrite();
		ReadWrite result = isWrite ? ReadWrite.WRITE : ReadWrite.READ;
		return result;
	}

	@Override
	public TxnType transactionType() {
		ReadWrite rw = transactionMode();
		TxnType result = TxnType.convert(rw);
		return result;
	}

	@Override
	public boolean isInTransaction() {
		boolean result = local() != null;
		return result;
	}

	
	public DatasetGraph mapToDatasetGraph(ResourceApi api) {
		api.declareAccess();
		api.lock(local().isWrite());
		Path path = api.getResFilePath();
		SyncedDataset entry;
		try {
			entry = syncCache.get(path);
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		}
		return entry.get();
	}

	@Override
	public Iterator<Node> listGraphNodes() {
		Iterator<Node> result = access(this, () -> {
			return local().listVisibleFiles()
				.map(this::mapToDatasetGraph)
				.collect(Collectors.toList()).stream() // FIXME only collect if not in a txn
	        	.flatMap(dataset -> {
	        		return Streams.stream(dataset.listGraphNodes());
	        	}).iterator();
		});
		
		return result;
	}

	@Override
	public Graph getDefaultGraph() {
		return GraphView.createNamedGraph(this, Quad.defaultGraphIRI);
	}

	@Override
	public Graph getGraph(Node graphNode) {
		return GraphView.createNamedGraph(this, graphNode);
	}

	@Override
	public void addGraph(Node graphName, Graph graph) {
		throw new UnsupportedOperationException("not implemented yet");
	}

	@Override
	public void removeGraph(Node graphName) {
		throw new UnsupportedOperationException("not implemented yet");
//		String iri = graphName.getURI();
//		local().getResourceApi(null)
	}
	
	@Override
	public void add(Node g, Node s, Node p, Node o) {
		mutateGraph(g, dg -> {
			boolean r = !dg.contains(g, s, p, o);
			if (r) {
				dg.add(g, s, p, o);
				for (DatasetGraphIndexPlugin indexer : indexers) {
					indexer.add(dg, g, s, p, o);
				}
			}
			
			
			return r;
		});

//		mutate(this, () -> {
//			String iri = g.getURI();
//			Path relPath = txnMgr.getResRepo().getRelPath(iri);
//
//			// Get the resource and lock it for writing
//			// The lock is held until the end of the transaction
//			ResourceApi api = local().getResourceApi(iri);
//			api.declareAccess();
//			api.lock(true);
//			
//			Synced<?, DatasetGraph> synced;
//			try {
//				synced = syncCache.get(relPath);
//			} catch (ExecutionException e) {
//				throw new RuntimeException(e);
//			}
//			DatasetGraph dg = synced.get();
//			
//			if (!dg.contains(g, s, p, o)) {
//				synced.setDirty(true);
//				dg.add(g, s, p, o);
//			}
//		});
	}
	
	@Override
	public void delete(Node g, Node s, Node p, Node o) {
		mutateGraph(g, dg -> {
			boolean r = dg.contains(g, s, p, o);
			if (r) {
				for (DatasetGraphIndexPlugin indexer : indexers) {
					indexer.delete(dg, g, s, p, o);
				}
				dg.delete(g, s, p, o);
			}
			return r;
		});
	}
	
	/**
	 * 
	 * @param graphNode
	 * @param mutator A predicate with side effect; true means a change was performed
	 */
	protected void mutateGraph(Node graphNode, Predicate<DatasetGraph> mutator) {
		mutate(this, () -> {
			String iri = graphNode.getURI();
			Path relPath = txnMgr.getResRepo().getRelPath(iri);

			// Get the resource and lock it for writing
			// The lock is held until the end of the transaction
			ResourceApi api = local().getResourceApi(iri);
			api.declareAccess();
			api.lock(true);
			
			SyncedDataset synced;
			try {
				synced = syncCache.get(relPath);
			} catch (ExecutionException e) {
				throw new RuntimeException(e);
			}
			
			DatasetGraph dg = synced.get();
			boolean isDirty = mutator.test(dg);
//			if (isDirty) {
//				synced.setDirty(true);
//			}
		});
	}
	
    /**
     * Copied from {@link DatasetGraphWrapper}
     *
     * @param <T>
     * @param mutator
     * @param payload
     */
    public static <T> void mutate(Transactional txn, Runnable mutator) {
        if (txn.isInTransaction()) {
            if (!txn.transactionMode().equals(ReadWrite.WRITE)) {
                TxnType mode = txn.transactionType();
                switch (mode) {
                case WRITE:
                    break;
                case READ:
                    throw new JenaTransactionException("Tried to write inside a READ transaction!");
                case READ_COMMITTED_PROMOTE:
                case READ_PROMOTE:
                    throw new RuntimeException("promotion not implemented");
//                    boolean readCommitted = (mode == TxnType.READ_COMMITTED_PROMOTE);
//                    promote(readCommitted);
                    //break;
                }
            }

            mutator.run();
        } else {
        	Txn.executeWrite(txn, () -> {
                mutator.run();
            });
        }
    }
    
    
    

    	
    @Override
    public void add(Quad quad)
    { add(quad.getGraph(), quad.getSubject(), quad.getPredicate(), quad.getObject()); }

    @Override
    public void delete(Quad quad)
    { delete(quad.getGraph(), quad.getSubject(), quad.getPredicate(), quad.getObject()); }

//    @Override
//    public void add(Node g, Node s, Node p, Node o) {
//        mutate(x -> {
//            if (!contains(g, s, p, o)) {
//                indexPlugins.forEach(plugin -> plugin.add(g, s, p, o));
//                getW().add(g, s, p, o);
//            }
//        }, null);
//    }
//
//
//    @Override
//    public void delete(Node g, Node s, Node p, Node o) {
//        mutate(x -> {
//            if (contains(g, s, p, o)) {
//                indexPlugins.forEach(plugin -> plugin.delete(g, s, p, o));
//                getW().delete(g, s, p, o);
//            }
//        }, null);
//    }

//    @Override
//    public void deleteAny(Node g, Node s, Node p, Node o)
//    { mutate(x -> getW().deleteAny(g, s, p, o), null); }

    @Override
    public void deleteAny(Node g, Node s, Node p, Node o) {
    	super.deleteAny(g, s, p, o);
    }
    
    public static <T> T access(Transactional txn, Supplier<T> source) {
        return txn.isInTransaction() ? source.get() : Txn.calculateRead(txn, source::get);
    }

    
//
//
//    // @Override
//    protected Iterator<Quad> findInAnyNamedGraphs(Node s, Node p, Node o) {
//    	return local().listVisibleFiles().flatMap(api -> {
//    		Path path = api.getResFilePath();
//    		Synced<?, DatasetGraph> entry = syncCache.get(path);
//    		DatasetGraph dg = entry.get();
//    		Stream<Quad> r = Streams.stream(dg.find(g, s, p, o));
//    		return r;
//    	});
//    }
    	
//        DatasetGraphIndexPlugin bestPlugin = findBestMatch(
//                indexPlugins.iterator(), plugin -> plugin.evaluateFind(s, p, o), (lhs, rhs) -> lhs != null && lhs < rhs);
//
//        Iterator<Node> gnames = bestPlugin != null
//            ? bestPlugin.listGraphNodes(s, p, o)
//            : listGraphNodes();
//
//        IteratorConcat<Quad> iter = new IteratorConcat<>() ;
//
//        // Named graphs
//        for ( ; gnames.hasNext() ; )
//        {
//            Node gn = gnames.next();
//            Iterator<Quad> qIter = findInSpecificNamedGraph(gn, s, p, o) ;
//            if ( qIter != null )
//                iter.add(qIter) ;
//        }
//        return iter ;

    
    protected Iterator<Quad> findInSpecificNamedGraph(Node g, Node s, Node p , Node o) {
    	String res = g.getURI();
    	Path relPath = txnMgr.getResRepo().getRelPath(res);

    	
    	return access(this, () -> Stream.of(local().getResourceApi(relPath))
        	.filter(ResourceApi::isVisible)
			.map(this::mapToDatasetGraph)
				.collect(Collectors.toList()).stream() // FIXME only collect if not in a txn
			.flatMap(dg -> Streams.stream(dg.find(Node.ANY, s, p, o))).iterator());
    }

    
    public Stream<ResourceApi> findResources(Node s, Node p, Node o) {
        DatasetGraphIndexPlugin bestPlugin = DatasetGraphFromFileSystem.findBestMatch(
        		indexers.iterator(),
        		plugin -> plugin.evaluateFind(s, p, o), (lhs, rhs) -> lhs != null && lhs < rhs);

		Stream<ResourceApi> visibleMatchingResources = bestPlugin != null
				? bestPlugin.listGraphNodes(s, p, o)
		            .map(relPath -> local().getResourceApi(relPath))
		            .filter(ResourceApi::isVisible)
		        : local().listVisibleFiles();

		return visibleMatchingResources;
    }

    

	public Iterator<Quad> findInAnyNamedGraphs(Node s, Node p, Node o) {
		// findResources(s, p, o)
		return access(this, () -> findResources(s, p, o)
			.map(this::mapToDatasetGraph)
				.collect(Collectors.toList()).stream() // FIXME only collect if not in a txn
			.flatMap(dg -> Streams.stream(dg.find(Node.ANY, s, p, o)))).iterator();
	}

	@Override
	public Iterator<Quad> find(Node g, Node s, Node p, Node o) {
		Iterator<Quad> result = g == null || Node.ANY.equals(g)
			? findInAnyNamedGraphs(s, p, o)
			: findInSpecificNamedGraph(g, s, p, o);
		return result;
//    	return Txn.calculateRead(this, () -> local().listVisibleFiles().flatMap(api -> {
//		Path path = api.getResFilePath();
//		SyncedDataset entry;
//		try {
//			entry = syncCache.get(path);
//		} catch (ExecutionException e) {
//			throw new RuntimeException(e);
//		}
//		DatasetGraph dg = entry.get();
//		Stream<Quad> r = Streams.stream(dg.find(g, s, p, o));
//		return r;
//	}).iterator());
	}

	@Override
	public Iterator<Quad> findNG(Node g, Node s, Node p, Node o) {
		return find(g, s, p, o);
	}
	
}
