package org.aksw.jena_sparql_api.txn;

import java.nio.file.Path;

public interface ResourceRepository<R> {
	Path getRootPath();
	
	/** Resource to key */
	String[] getPathSegments(R name);
	
	/** Stream the keys in the repository */
	// Stream<String[]> streamResourceKeys();
}
