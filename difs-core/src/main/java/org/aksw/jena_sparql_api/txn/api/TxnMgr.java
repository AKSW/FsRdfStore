package org.aksw.jena_sparql_api.txn.api;

import java.io.IOException;
import java.time.temporal.TemporalAmount;
import java.util.stream.Stream;

import org.aksw.jena_sparql_api.lock.db.api.LockStore;
import org.aksw.jena_sparql_api.txn.ResourceRepository;

public interface TxnMgr
{
    String getTxnMgrId();

    LockStore<String[], String> getLockStore();

    ResourceRepository<String> getResRepo();

    /** Create a new transaction. If the id is null then an id will be allocated.
     *  If a txn with the given id already exists an exception in raised. */
    Txn newTxn(String id, boolean useJournal, boolean isWrite) throws IOException;

    default Txn newTxn (boolean useJournal, boolean isWrite) throws IOException {
        return newTxn(null, useJournal, isWrite);
    }

    /** Get an accessor to an existing transaction */
    Txn getTxn(String txnId);


    /** Stream all existing transactions */
    Stream<Txn> streamTxns() throws IOException;

    /** Method mainly for testing; should delete folders and such */
    void deleteResources() throws IOException;

    TemporalAmount getHeartbeatDuration();
}
