package org.aksw.jena_sparql_api.difs.main;

import static org.apache.jena.system.Txn.calculateRead;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.aksw.jena_sparql_api.txn.FileSync;
import org.aksw.jena_sparql_api.txn.RdfSync;
import org.aksw.jena_sparql_api.txn.ResourceRepository;
import org.aksw.jena_sparql_api.txn.Synced;
import org.aksw.jena_sparql_api.txn.TxnImpl;
import org.aksw.jena_sparql_api.txn.TxnImpl.ResourceApi;
import org.aksw.jena_sparql_api.txn.TxnMgr;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphBase;
import org.apache.jena.sparql.core.DatasetGraphWrapper;
import org.apache.jena.sparql.core.GraphView;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.system.Txn;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Streams;

public class DatasetGraphFromTxnMgr
	extends DatasetGraphBase
{
	protected TxnMgr txnMgr;
	protected ThreadLocal<TxnImpl> txns = ThreadLocal.withInitial(() -> null);
	
	
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

	
	protected LoadingCache<Path, Synced<?, DatasetGraph>> syncCache = CacheBuilder
			.newBuilder()
			.maximumSize(1000)
			.removalListener(ev -> {
				// TODO Sync here or elsewhere?
				// getValue();
			})
			.build(new CacheLoader<Path, Synced<?, DatasetGraph>>() {
				@Override
				public Synced<?, DatasetGraph> load(Path key) throws Exception {
					ResourceRepository<String> resRepo = txnMgr.getResRepo();
					Path rootPath = resRepo.getRootPath();

					// Path relPath = r// resRepo.getRelPath(key);
					Path absPath = rootPath.resolve(key);					
					
					return RdfSync.create(absPath);
				}
			});
	
	
	public TxnImpl local() {
		return txns.get();
	}
	
	public DatasetGraphFromTxnMgr(TxnMgr txnMgr) {
		super();
		this.txnMgr = txnMgr;
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

	@Override
	public void commit() {
		try {
			Iterator<String> it = local().streamAccessedResources().iterator();
			while (it.hasNext()) {
				String res = it.next();
				System.out.println("Syncing: " + res);
				Path relPath = txnMgr.getResRepo().getRelPath(res);

				ResourceApi api = local().getResourceApi(res);
				if (api.ownsWriteLock()) {
					// If we own a write lock and the state is dirty then sync
					Synced<?, DatasetGraph> synced = syncCache.getIfPresent(relPath);
					if (synced != null) {
						synced.save();
					}
					
					FileSync fs = api.getFileSync();
					fs.preCommit();
				}
			}

			// Once all modified graphs are written out
			// add the statement that the commit action can now be run
			local().addCommit();

			// Run the finalization actions
			// As these actions remove undo information
			// there is no turning back anymore
			local().addFinalize();
			
			it = local().streamAccessedResources().iterator();
			while (it.hasNext()) {
				String res = it.next();
				System.out.println("Finalizing and unlocking: " + res);
				ResourceApi api = local().getResourceApi(res);
				
				api.finalizeCommit();
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
	
	


	@Override
	public Iterator<Node> listGraphNodes() {
		Iterator<Node> result = Txn.calculateRead(this, () -> {
			return local().listVisibleFiles()
	        	.flatMap(api -> {
	        		Path path = api.getResFilePath();
	        		Synced<?, DatasetGraph> entry;
					try {
						entry = syncCache.get(path);
					} catch (ExecutionException e) {
						throw new RuntimeException(e);
					}
	        		return Streams.stream(entry.get().listGraphNodes());
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
		Txn.executeWrite(this, () -> {
			String iri = g.getURI();
			Path relPath = txnMgr.getResRepo().getRelPath(iri);

			// Get the resource and lock it for writing
			// The lock is held until the end of the transaction
			ResourceApi api = local().getResourceApi(iri);
			api.declareAccess();
			api.lock(true);
			
			Synced<?, DatasetGraph> synced;
			try {
				synced = syncCache.get(relPath);
			} catch (ExecutionException e) {
				throw new RuntimeException(e);
			}
			synced.get().add(g, s, p, o);
		});
	}
	
	@Override
	public void delete(Node g, Node s, Node p, Node o) {		
		Txn.executeWrite(this, () -> {
			String iri = g.getURI();
			Path relPath = txnMgr.getResRepo().getRelPath(iri);

			// Get the resource and lock it for writing
			// The lock is held until the end of the transaction
			ResourceApi api = local().getResourceApi(iri);
			api.declareAccess();
			api.lock(true);
			
			Synced<?, DatasetGraph> synced;
			try {
				synced = syncCache.get(relPath);
			} catch (ExecutionException e) {
				throw new RuntimeException(e);
			}
			synced.get().delete(g, s, p, o);
		});
	}
	
	
    /**
     * Copied from {@link DatasetGraphWrapper}
     *
     * @param <T>
     * @param mutator
     * @param payload
     */
//    private <T> void mutate(final Consumer<T> mutator, final T payload) {
//        if (isInTransaction()) {
//            if (!transactionMode().equals(WRITE)) {
//                TxnType mode = transactionType();
//                switch (mode) {
//                case WRITE:
//                    break;
//                case READ:
//                    throw new JenaTransactionException("Tried to write inside a READ transaction!");
//                case READ_COMMITTED_PROMOTE:
//                case READ_PROMOTE:
//                    throw new RuntimeException("promotion not implemented");
////                    boolean readCommitted = (mode == TxnType.READ_COMMITTED_PROMOTE);
////                    promote(readCommitted);
//                    //break;
//                }
//            }
//
//            // Make the version negative to mark it as 'dirty'
//            version.set(-Math.abs(version.get()));
//
//            mutator.accept(payload);
//        } else {
//            executeWrite(this, () -> {
//                version.set(-Math.abs(version.get()));
//    //            System.out.println(version.get());
//                mutator.accept(payload);
//            });
//        }
//    }

    	
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
    
    private <T> T access(final Supplier<T> source) {
        return isInTransaction() ? source.get() : calculateRead(this, source::get);
    }

    
//    protected Iter<Quad> findInSpecificNamedGraph(Node g, Node s, Node p , Node o) {
//    	
//    }
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


    
	@Override
	public Iterator<Quad> find(Node g, Node s, Node p, Node o) {
    	return Txn.calculateRead(this, () -> local().listVisibleFiles().flatMap(api -> {
    		Path path = api.getResFilePath();
    		Synced<?, DatasetGraph> entry;
			try {
				entry = syncCache.get(path);
			} catch (ExecutionException e) {
				throw new RuntimeException(e);
			}
    		DatasetGraph dg = entry.get();
    		Stream<Quad> r = Streams.stream(dg.find(g, s, p, o));
    		return r;
    	}).iterator());

//		Iterator<Quad> result = g == null || Node.ANY.equals(g)
//			? findInAnyNamedGraphs(s, p, o)
//			: findInSpecificNamedGraph(g, s, p, o);
//		return result;
	}

	@Override
	public Iterator<Quad> findNG(Node g, Node s, Node p, Node o) {
		return find(g, s, p, o);
	}
	
}
