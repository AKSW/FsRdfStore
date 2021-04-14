package org.aksw.jena_sparql_api.txn.api;

public interface Txn
// 	extends TxnComponent
{
	TxnResourceApi getResourceApi(String[] resRelPath);
}
