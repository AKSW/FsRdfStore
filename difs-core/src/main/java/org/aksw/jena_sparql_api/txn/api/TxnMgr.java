package org.aksw.jena_sparql_api.txn.api;

import org.aksw.jena_sparql_api.txn.ResourceRepository;

public interface TxnMgr
{
	ResourceRepository<String> getResRepo();

	Txn newTxn(boolean useJournal, boolean isWrite);
}
