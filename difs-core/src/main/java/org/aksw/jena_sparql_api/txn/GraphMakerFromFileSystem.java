package org.aksw.jena_sparql_api.txn;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import org.aksw.commons.io.util.UriToPathUtils;
import org.aksw.commons.util.healthcheck.HealthcheckRunner;
import org.aksw.jena_sparql_api.dataset.file.LockPolicy;
import org.aksw.jena_sparql_api.lock.LockManager;
import org.apache.jena.dboe.base.file.ProcessFileLock;
import org.apache.jena.ext.com.google.common.collect.Maps;
import org.apache.jena.ext.com.google.common.collect.Streams;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.GraphMaker;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.util.iterator.ExtendedIterator;


/**
 * 
 * Strategies for triples-to-named-graph mapping:
 * QuitStore stores the named graph IRI in a separate file with .graph extension
 * An alternative is to use a trig file with a single named graph; i.e. <pre>@graph @lt;foograph&gt; { ... }</pre>
 */
public class GraphMakerFromFileSystem
	implements GraphMaker, Transactional
{
	protected Path repoPath;         // The root
	protected Path dataPath;         // resolved against repoPath
	protected Path defaultGraphPath; // resolved against dataPath

	protected Path txnPath;          // resolved against repoPath
	
	protected String defaultGraphName = "DEFAULT";
		
	
	protected LockManager<Path> processLockManager;
	protected LockManager<Path> threadLockManager;
	
	@Override
	public Graph getGraph() {
		return createGraph(defaultGraphName);
	}

	@Override
	public Graph openGraph() {
		return openGraph(defaultGraphName);
	}

	@Override
	public Graph createGraph() {
		return createGraph(defaultGraphName);
	}

	
	
	@Override
	public Graph createGraph(String name, boolean strict) {
        Path relFile = UriToPathUtils.resolvePath(name);
        Path absFile = basePath.resolve(relFile);
        
        // TODO Who implements Files.createDirectories?
        Lock lock = processLockManager.getLock(absFile, false);

        
        Path absFolder = absFile.getParent();
        if (!Files.exists(absFolder)) {
        	try {
				Files.createDirectories(absFolder);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
        }
        
        ProcessFileLock lock = ProcessFileLock.create(absFile.toString());

		
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Graph createGraph(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Graph openGraph(String name, boolean strict) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Graph openGraph(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeGraph(String name) {
        Path relPath = UriToPathUtils.resolvePath(name);

        String filename = getFilename();
        Path fileRelPath = relPath.resolve(filename);
        Map<Path, Dataset> targetMap = getTargetMap();
        Dataset ds = targetMap.get(fileRelPath);

        // TODO Finish

        if (txnDsg2Graph != null) {
            // Ensure that the requested graphName is added to the txn handlers
//            DatasetGraph dsg = result.getValue().asDatasetGraph();
//            txnDsg2Graph.addGraph(dsg.getGraph(graphName));
        }
		
	}

	@Override
	public boolean hasGraph(String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ExtendedIterator<String> listGraphs() {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	

    public Entry<Path, Dataset> getOrCreateGraph(Node graphName) {
    	
        // Graph named must be a URI
        String iri = graphName.getURI();
        Path relPath = UriToPathUtils.resolvePath(iri);
        Entry<Path, Dataset> result = getOrCreate(relPath);

        if (txnDsg2Graph != null) {
            // Ensure that the requested graphName is added to the txn handlers
            DatasetGraph dsg = result.getValue().asDatasetGraph();
            Graph graph = dsg.getGraph(graphName);
            txnDsg2Graph.addGraph(graph);
        }

        return result;
    }

    
    public Entry<Path, Dataset> getOrCreate(Path relPath) {
        lockManager.getLock(relPath, true);
    	
    	Map<Path, Dataset> targetMap = getTargetMap();

        
        
        String filename = getFilename();
        Path fileRelPath = relPath.resolve(filename);
        Dataset ds = targetMap.get(fileRelPath);
        if (ds == null) {
            Path fullPath = basePath.resolve(fileRelPath);//.resolve("data.trig");

            DatasetGraphWithSync dsg;
            try {
                // FIXME Implement file deletion on rollback
                // If the transaction in which this graph is created is rolled back
                // then the file that backs the graph must also be deleted again
                dsg = new DatasetGraphWithSync(fullPath, LockPolicy.TRANSACTION);
                dsg.setIndexPlugins(indexPlugins);
                dsg.setPreCommitHooks(preCommitHooks);


            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            ds = DatasetFactory.wrap(dsg);
            targetMap.put(fileRelPath, ds);

            if (txnDsg2Graph != null) {
                List<Node> graphNodes = Streams.stream(dsg.listGraphNodes()).collect(Collectors.toList());
                for (Node graphNode : graphNodes) {
                    txnDsg2Graph.addGraph(dsg.getGraph(graphNode));
                }
            }

        }

        Entry<Path, Dataset> result = Maps.immutableEntry(fileRelPath, ds);

        return result;
    }

    public Path getEffectiveTxnFolder() {
    	return repoPath.resolve(txnPath).toAbsolutePath();
    }
    
	@Override
	public void begin(TxnType type) {
		Path txnFolder = getEffectiveTxnFolder();

		// Check whether recovery needs to be performed
		// ISSUE We may need to claim the resources of a stale transanction and thereby
		// - ensure that no concurrent recovery claimns them as well
		// - deal with the case that a process might be interrupted while claiming resources
		// Maybe there is no way around temporarily using TDB without drowning in complexity...
		// Hm, and if the 'claim' is just a symlink into another txnid folder? That would probably work:
		// - Upon claiming create a symlink from the claimed txnid to the claiming one
		// - Then create a backlink
		// - When claiming a transaction a that claimed txn b's resources I guess it's safe to just delete the claimns of a to b;
		// - The resources will be reclaimed anyway when processing b's folder and finding out that it is stale.
		// If another recovery finds a stale txnids with claimed resources it can just claim those again
		// We probably need a separate phase to remove stale locks first though - so that the lock files can be rewritten without requiring a lock
		// on the stale locks
		String txnId = "txn-" + new Random().nextLong();

		Path txnInstPath = txnFolder.resolve(txnId);

		try {
			Files.createDirectories(txnInstPath);
		} catch (IOException e) {
			throw new RuntimeException("Failed to lock txn folder");
		}
		
//		ProcessFileLock lock = ProcessFileLock.create(txnInstPath.toString());
//		lock.tryLock();
		
		
		
		
		
		
	}

	@Override
	public void begin(ReadWrite readWrite) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean promote(Promote mode) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void commit() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void abort() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void end() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ReadWrite transactionMode() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TxnType transactionType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isInTransaction() {
		// TODO Auto-generated method stub
		return false;
	}



}
