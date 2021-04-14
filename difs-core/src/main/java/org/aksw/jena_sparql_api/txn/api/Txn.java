package org.aksw.jena_sparql_api.txn.api;

import java.io.IOException;
import java.util.stream.Stream;

public interface Txn
{
	TxnResourceApi getResourceApi(String[] resRelPath);
	
	Stream<TxnResourceApi> listVisibleFiles();
	Stream<String[]> streamAccessedResourcePaths() throws IOException;

	boolean isWrite();

	void addCommit() throws IOException;
	void addFinalize() throws IOException;
	void addRollback() throws IOException;
	
	boolean isCommit() throws IOException;
	boolean isRollback() throws IOException;
	boolean isFinalize() throws IOException;
	
	void cleanUpTxn() throws IOException;
}
