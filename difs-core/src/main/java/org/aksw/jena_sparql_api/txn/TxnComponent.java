package org.aksw.jena_sparql_api.txn;

public interface TxnComponent {
	void preCommit() throws Exception;
	void finalizeCommit() throws Exception;
	// void preRollback();
	void rollback() throws Exception;
}
