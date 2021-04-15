package org.aksw.jena_sparql_api.txn;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.aksw.commons.io.util.PathUtils;
import org.aksw.jena_sparql_api.difs.main.Array;
import org.aksw.jena_sparql_api.txn.api.Txn;
import org.aksw.jena_sparql_api.txn.api.TxnResourceApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class TxnReadUncommitted
	implements Txn
{
//	private static final Logger logger = LoggerFactory.getLogger(TxnReadUncommitted.class);
	
//	protected boolean isWrite;
	
	protected TxnMgrImpl txnMgr;
	protected String txnId;
//	protected Path txnFolder;
	
	//protected String preCommitFilename = ".precommit";
//	protected String commitFilename = "commit";
//	protected String finalizeFilename = "finalize";
//	protected String rollbackFilename = "rollback";
	
	//protected transient Path preCommitFile;
	//protected transient Path finalizeCommitFile;
//	protected transient Path commitFile;
//	protected transient Path finalizeFile;
//	protected transient Path rollbackFile;	
	
//	protected IsolationLevel isolationLevel;
	
	//protected LockStore<String[], String> lockStore;
	
	protected TxnResourceApi createResourceApi(String[] key) {
		return new TxnResourceApiReadUncommitted<>(this, key);
	}
	
	protected LoadingCache<Array<String>, TxnResourceApi> containerCache = CacheBuilder.newBuilder()
			.maximumSize(1000)
			.build(new CacheLoader<Array<String>, TxnResourceApi>() {
				@Override
				public TxnResourceApi load(Array<String> key) throws Exception {
					return createResourceApi(key.getArray());
				}
			});
			
	
	public TxnReadUncommitted(
			TxnMgrImpl txnMgr,
			String txnId) {
		super();
		this.txnMgr = txnMgr;
		this.txnId = txnId;
	}
	
	
	@Override
	public Stream<TxnResourceApi> listVisibleFiles() {
	    
		// TODO This pure listing of file resources should probably go to the repository
		PathMatcher pathMatcher = txnMgr.getResRepo().getRootPath().getFileSystem().getPathMatcher("glob:**/*.trig");
	
	    List<TxnResourceApi> result;
	    try (Stream<Path> tmp = Files.walk(txnMgr.getResRepo().getRootPath())) {
	    	// TODO Filter out graphs that were created after the transaction start
	        result = tmp
		            .filter(pathMatcher::matches)
		            // We are interested in the folder - not the file itself: Get the parent
		            .map(Path::getParent)
		            .map(path -> txnMgr.resRepo.getRootPath().relativize(path))
		            .map(PathUtils::getPathSegments)
		            .map(this::getResourceApi)
		            .filter(TxnResourceApi::isVisible)
	        		.collect(Collectors.toList());
	    } catch (IOException e1) {
	    	throw new RuntimeException(e1);
		}
	    // paths.stream().map(path -> )
	    
	    return result.stream();
	}    
	
	
//	public Instant getCreationInstant() {
//		try {
//		    BasicFileAttributes attr = Files.readAttributes(txnFolder, BasicFileAttributes.class);
//		    FileTime fileTime = attr.creationTime();
//		    return fileTime.toInstant();
//		} catch (IOException e) {
//			throw new RuntimeException(e);
//		}
//	}
	
//	public TxnResourceApi getResourceApi(String resourceName) {
//		String[] relRelPath = txnMgr.getResRepo().getPathSegments(resourceName);
//		TxnResourceApi result = getResourceApi(relRelPath);
//		return result;
//	}
	
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
	
	@Override
	public boolean isWrite() {
		return false;
	}
	
	public void cleanUpTxn() throws IOException {
	}
		
	public void addCommit() throws IOException {
	}
	
	public void addFinalize() throws IOException {
	}
		
	public void addRollback() throws IOException {
	}
	
	public boolean isFinalize() throws IOException {
		return true;
	}
	
	public boolean isCommit() throws IOException {
		return true;
	}
	
	public boolean isRollback() throws IOException {
		return false;
	}
	
	@Override
	public Stream<String[]> streamAccessedResourcePaths() throws IOException {
		return Stream.empty();
	}
}