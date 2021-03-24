package org.aksw.jena_sparql_api.txn;

import java.nio.file.Path;

public interface ResourceRepository<R> {
	Path getRootPath();
	Path getRelPath(R name);
}
