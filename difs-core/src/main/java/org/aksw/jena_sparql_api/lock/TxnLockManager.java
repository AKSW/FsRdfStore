package org.aksw.jena_sparql_api.lock;

import org.apache.jena.system.Txn;

public interface TxnLockManager {
	// Create a new transaction
	Txn beginTxn();
	
	
}
