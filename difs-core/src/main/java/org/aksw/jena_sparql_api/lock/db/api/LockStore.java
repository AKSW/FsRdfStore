package org.aksw.jena_sparql_api.lock.db.api;

import java.io.IOException;
import java.util.stream.Stream;

/**
 * Interface to manage a set of read/write locks for a set of resources
 * 
 * @author Claus Stadler
 *
 * @param <R> The type of the resource subject to locking
 * @param <O> The type of the owner of locks
 */
public interface LockStore<R, O> {

	/**
	 * Get the API to manage read/write locks for a given resource
	 * 
	 * @param resource
	 * @return
	 */
	ResourceLock<O> getLockForResource(String resource);
	
	ResourceLock<O> getLockByKey(String[] lockKey);
	
	/**
	 * Get a stream of all existing resource locks
	 * The resulting stream should be weakly consistent (See {@link Files#list(java.nio.file.Path)).
	 * The result stream should always be used in a try-with-resources block in order to ensure release
	 * of resources.
	 * 
	 * @return
	 * @throws IOException 
	 */
	Stream<ResourceLock<O>> streamResourceLocks() throws IOException;
}
