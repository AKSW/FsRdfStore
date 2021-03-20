package org.aksw.jena_sparql_api.lock;

public interface Txn<T> {
	/** Try to lock the resource for this transaction */
	void lock(T resource, boolean write);
	

}
