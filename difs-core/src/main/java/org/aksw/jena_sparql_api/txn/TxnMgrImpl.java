package org.aksw.jena_sparql_api.txn;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.temporal.TemporalAmount;
import java.util.Random;
import java.util.stream.Stream;

import org.aksw.commons.io.util.symlink.SymbolicLinkStrategy;
import org.aksw.jena_sparql_api.lock.LockManager;
import org.aksw.jena_sparql_api.lock.db.api.LockStore;
import org.aksw.jena_sparql_api.lock.db.impl.LockStoreImpl;
import org.aksw.jena_sparql_api.txn.api.Txn;
import org.aksw.jena_sparql_api.txn.api.TxnMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.MoreFiles;


/**
 * Path-based transaction manager:
 * Implements a protocol for that uses folders and symlinks to manage locks on paths (files and directories).
 * The protocol is aimed at allowing multiple processes to run transactions on a common pool of paths
 * with serializable isolation level (the highest).
 *
 * Conceptually, three folder structures are involved:
 * The ./store contains the actual data files.
 * The ./txns folder holds a subfolder for each active transaction.
 *   Whenever a resource is accessed within a transaction a symlink from the txn folder to the accessed' resource's shadow is created.
 * The ./shadow is the 'shadow' of the store. For each resource in the store that is involved in a transaction
 *   a folder is created. Upon read or write access lock files are created in the shadow that link to the transaction folder
 *   that owns the lock.
 *
 * The purpose of the shadow is two fold:
 *   (1) It prevents writing all the management files into the store which causes pollution in case a process
 *       running a transaction exits (or loses connection to the store) without clean up.
 *   (2) While resources in ./store can be arbitrarily nested in subfolders, in ./shadow those entries are 'flattened'
 *       by url encoding their relative path. This makes management somewhat easier because locking resources does not have
 *       to deal with corner cases where one process wants to create a sub folder of an empty folder X and another process wants to
 *       clean up and remove X.
 *
 * store/org/example/data.nt
 *
 * locks/.org-example-data/data.nt -&gt store/org/example/data.nt
 * locks/.org-example-data/txn-12345.lock -&gt txns/txn-12345
 *
 *
 * txns/txn-12345/
 *
 *
 * @author Claus Stadler
 *
 */
public class TxnMgrImpl
    implements TxnMgr
{
    private static final Logger logger = LoggerFactory.getLogger(TxnMgrImpl.class);

    protected LockManager<Path> lockMgr;
    protected Path txnBasePath;
    protected ResourceRepository<String> resRepo;
    protected ResourceRepository<String> lockRepo;

    protected LockStore<String[], String> lockStore;


    protected SymbolicLinkStrategy symlinkStrategy;

    /**
     * The (globally unique) id of the transaction manager - transactions use this id to declare their owner.
     * The id is assigned at runtime
     */
    protected String txnMgrId;


    protected TemporalAmount heartbeatDuration;

    public TxnMgrImpl(
            String txnMgrId,
            TemporalAmount heartbeatDuration,
            LockManager<Path> lockMgr,
            Path txnBasePath,
            ResourceRepository<String> resRepo,
            ResourceRepository<String> lockRepo,
            SymbolicLinkStrategy symlinkStrategy) {
        super();
        this.txnMgrId = txnMgrId;
        this.heartbeatDuration = heartbeatDuration;
        this.lockMgr = lockMgr;
        this.txnBasePath = txnBasePath;
        this.resRepo = resRepo;
        this.lockRepo = lockRepo;
        this.symlinkStrategy = symlinkStrategy;

        lockStore = new LockStoreImpl(symlinkStrategy, lockRepo, resRepo, txnId -> txnBasePath.resolve(txnId));
    }

    @Override
    public TemporalAmount getHeartbeatDuration() {
        return heartbeatDuration;
    }

    public String getTxnMgrId() {
        return txnMgrId;
    }

    @Override
    public LockStore<String[], String> getLockStore() {
        return lockStore;
    }

    /**
     * Build a bipartite graph between dependencies and locks; i.e.
     *
     * Read the locks of the given transactions
     */
    public void buildLockGraph() {

    }


    public SymbolicLinkStrategy getSymlinkStrategy() {
        return symlinkStrategy;
    }



    @Override
    public ResourceRepository<String> getResRepo() {
        return resRepo;
    }

    @Override
    public Txn getTxn(String txnId) {
        Path txnFolder = txnBasePath.resolve(txnId);
        Txn result = new TxnSerializable(this, txnId, txnFolder);

        return result;
    }


    public Txn newTxn(String id, boolean useJournal, boolean isWrite) throws IOException {
        String txnId = id == null ? "txn-" + new Random().nextLong() : id;

        Txn result;
        if (!useJournal) {
            result = new TxnReadUncommitted(this, txnId);
        } else {

            Path txnFolder = txnBasePath.resolve(txnId);

            if (Files.exists(txnFolder)) {
                throw new IllegalArgumentException(String.format("A transaction with id %s already exists", txnId));
            }

            try {
                Files.createDirectories(txnFolder);

                if (isWrite) {
                    Files.createFile(txnFolder.resolve("write"));
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to lock txn folder; set useJournal=false if read only access with 'read uncommitted' isolation level is intended");
            }

            logger.debug("Allocated txn folder" + txnFolder);
            result = new TxnSerializable(this, txnId, txnFolder);
            if (!result.claim()) {
                throw new RuntimeException("Failed to claim ownership of the recently created txn " + txnId);
            }
        }

        return result;
    }


    @Override
    public Stream<Txn> streamTxns() throws IOException {
        // The txn folder may not exist yet
        Stream<Path> baseStream = Files.exists(txnBasePath)
                ? Files.list(txnBasePath)
                : Stream.empty();

        return baseStream
                .map(path -> {
                    String txnId = path.getFileName().toString();
                    Txn r = getTxn(txnId);
                    return r;
                });
    }


    /**
     * Deletes the lock and transaction folders.
     * Does not delete the store and index folders.
     */
    @Override
    public void deleteResources() throws IOException {
        try {
            Path lockRepoRootPath = lockRepo.getRootPath();
            if (Files.exists(lockRepoRootPath)) {
                MoreFiles.deleteRecursively(lockRepoRootPath);
            }
        } finally {
            MoreFiles.deleteRecursively(txnBasePath);
        }
    }
}
