package org.aksw.jena_sparql_api.txn;

public interface Txn<R> {
	/** Try to lock the resource for this transaction */
	void lock(R resource, boolean write);
	

}
