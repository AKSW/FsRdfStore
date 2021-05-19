package org.aksw.difs.txn;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Set;

import org.aksw.difs.builder.DifsFactory;
import org.aksw.jena_sparql_api.txn.api.Txn;
import org.aksw.jena_sparql_api.txn.api.TxnMgr;
import org.aksw.jena_sparql_api.txn.api.TxnResourceApi;
import org.aksw.jena_sparql_api.txn.api.TxnUtils;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.jgrapht.GraphPath;
import org.junit.Assert;
import org.junit.Test;

public class TxnMgrTests {
	@Test
	public void detectDeadLockTest() throws IOException, InterruptedException {
		TxnMgr txnMgr = DifsFactory.newInstance()
			.setConfigFile(Paths.get("/tmp/store.conf.ttl"))
			.setStoreDefinition(sd -> {})
			.createTxnMgr();
		
		Txn a = txnMgr.newTxn("txn-a", true, true);
		Txn b = txnMgr.newTxn("txn-b", true, true);
		
		TxnResourceApi r1a = a.getResourceApi(new String[] {"r1"});
		r1a.declareAccess();
		r1a.getTxnResourceLock().writeLock().lock();

		TxnResourceApi r2b = b.getResourceApi(new String[] {"r2"});
		r2b.declareAccess();
		r2b.getTxnResourceLock().writeLock().lock();		
		
		TxnResourceApi r2a = a.getResourceApi(new String[] {"r2"});
		r2a.declareAccess();
		//r2a.getTxnResourceLock().writeLock().tryLock(1, TimeUnit.SECONDS);

		TxnResourceApi r1b = b.getResourceApi(new String[] {"r1"});
		r1b.declareAccess();
//		r1b.getTxnResourceLock().writeLock().lock();

		Set<GraphPath<Node, Triple>> cycles = TxnUtils.detectDeadLocks(txnMgr);
		Assert.assertEquals(1, cycles.size());
		
		txnMgr.deleteResources();
	}
}
