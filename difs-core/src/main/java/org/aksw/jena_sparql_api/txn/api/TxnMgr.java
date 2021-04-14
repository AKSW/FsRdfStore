package org.aksw.jena_sparql_api.txn.api;

public interface TxnMgr {
	Txn newTxn(boolean isWrite);
}
