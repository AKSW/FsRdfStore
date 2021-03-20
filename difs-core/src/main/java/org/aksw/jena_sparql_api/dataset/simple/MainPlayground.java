package org.aksw.jena_sparql_api.dataset.simple;

import org.apache.jena.dboe.trans.data.TransBinaryDataFile;
import org.apache.jena.dboe.transaction.txn.TransactionCoordinator;
import org.apache.jena.dboe.transaction.txn.TransactionalComponent;
import org.apache.jena.dboe.transaction.txn.journal.Journal;

public class MainPlayground {

	public static void main(String[] args) {
		Journal journal = Journal.create("/tmp/jrnl.dat");
		
		TransactionalComponent txnComponent = new TransBinaryDataFile(null, null, null);
		
		TransactionCoordinator txnCoord = new TransactionCoordinator(journal, Arrays.asList(txnComponent));
		
		txnCoord.begin();
		txnCoord.detach(null)
		
	}
	
}
