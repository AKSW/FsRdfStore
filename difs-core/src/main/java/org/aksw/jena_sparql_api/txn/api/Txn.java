package org.aksw.jena_sparql_api.txn.api;

import java.io.IOException;
import java.time.Instant;
import java.util.stream.Stream;

public interface Txn
{
	TxnMgr getTxnMgr();
	
	/** Get the id of this transaction */
	String getId();
	
	TxnResourceApi getResourceApi(String[] resRelPath);
	
	Stream<TxnResourceApi> listVisibleFiles();
	Stream<String[]> streamAccessedResourcePaths() throws IOException;

	
	/** Whether the transaction has become stale */
	boolean isStale();
	
	/** Instruct this TxnMgr to claim (i.e. take ownership) of this txn. Should only be used on stale transactions. */
	void claim();
	
	
	boolean isWrite();

	void addCommit() throws IOException;
	void addFinalize() throws IOException;
	void addRollback() throws IOException;
	
	boolean isCommit() throws IOException;
	boolean isRollback() throws IOException;
	boolean isFinalize() throws IOException;
	
	void cleanUpTxn() throws IOException;
	
	
	Instant getCreationDate();
	
	/**
	 * Update the transaction's most recent activity timestamp to given timestamp;
	 * Used to prevent other processes from considering the transaction stale.
	 */
	void setActivityDate(Instant instant) throws IOException;
	Instant getActivityDate() throws IOException;
	
	/**
	 * Update the transaction's most recent activity timestamp to the current time;
	 * Used to prevent other processes from considering the transaction stale.
	 */
	default Instant updateActivityDate() throws IOException {
		Instant now = Instant.now();
		setActivityDate(now);
		return now;
	}
}
