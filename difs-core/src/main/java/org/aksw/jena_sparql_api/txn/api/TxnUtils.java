package org.aksw.jena_sparql_api.txn.api;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.aksw.commons.collections.utils.StreamUtils;
import org.aksw.commons.jena.jgrapht.PseudoGraphJenaGraph;
import org.aksw.commons.util.array.Array;
import org.aksw.commons.util.memoize.MemoizedFunction;
import org.aksw.commons.util.memoize.MemoizedFunctionImpl;
import org.aksw.jena_sparql_api.lock.db.api.LockStore;
import org.aksw.jena_sparql_api.lock.db.api.ResourceLock;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.graph.GraphFactory;
import org.jgrapht.GraphPath;
import org.jgrapht.alg.cycle.QueueBFSFundamentalCycleBasis;
import org.jgrapht.alg.cycle.StackBFSFundamentalCycleBasis;
import org.jgrapht.alg.interfaces.CycleBasisAlgorithm;




public class TxnUtils {

    // Custom RDF predicate to relate transactions to resources
    public static final Node accessed = NodeFactory.createURI("urn:accessed");


    /**
     * Utility function that adds a triple to a graph by allocating nodes that represent the given
     * source and target keys.
     *
     * @param <K>
     * @param graph
     * @param nodeFactory
     * @param srcKey
     * @param tgtKey
     * @param predicate
     */
    public static <K> void add(Graph graph, Function<K, Node> nodeFactory, K srcKey, K tgtKey, Node predicate) {
        if (!srcKey.equals(tgtKey)) {
            Node srcNode = nodeFactory.apply(srcKey);
            Node tgtNode = nodeFactory.apply(tgtKey);
            graph.add(new Triple(srcNode, predicate, tgtNode));
        }
    }


    /**
     * Analyze a set of transactions for whether dead lock conditions are met.
     *
     * @param txnMgr
     * @return
     * @throws IOException
     */
    public static Set<GraphPath<Node, Triple>> detectDeadLocksRaw(TxnMgr txnMgr) throws IOException {

        Graph graph = GraphFactory.createDefaultGraph();
        Function<String, Node> nodeFactory = MemoizedFunctionImpl.create(str -> NodeFactory.createURI("urn:" + str));

        LockStore<String[], String> lockStore = txnMgr.getLockStore();
        for (Txn txn : StreamUtils.iterable(txnMgr.streamTxns())) {

            String txnId = txn.getId();

            boolean isWrite = txn.isWrite();

            for (String[] key : StreamUtils.iterable(txn.streamAccessedResourcePaths())) {
                ResourceLock<String> resLock = lockStore.getLockByKey(key);

                String writeLockOwner = resLock.getWriteLockOwnerKey();

                // A read txn only has to wait for an owned write lock
//				if (!isWrite) {
                    // txnId waits for writeLockOwner
                    add(graph, nodeFactory, txnId, writeLockOwner, accessed);
//				}

                // A write txn has to wait for any owned *and* write locks

                if (isWrite) {
                    for (String readLockOwner : StreamUtils.iterable(resLock.streamReadLockOwnerKeys())) {
                        // txnId waits for readLockOwner
                        add(graph, nodeFactory, txnId, readLockOwner, accessed);
                    }
                }
            }
        }

        CycleBasisAlgorithm<Node, Triple> alg = new QueueBFSFundamentalCycleBasis<>(new PseudoGraphJenaGraph(graph));
        Set<GraphPath<Node, Triple>> result = alg.getCycleBasis().getCyclesAsGraphPaths();

        return result;
//				new CycleDetector<>(new PseudoGraphJenaGraph(graph))
//				.findCycles();
    }

    public static Set<String> graphPathsToTxnIds(Set<GraphPath<Node, Triple>> graphPaths) throws IOException {
        Set<String> result = graphPaths.stream()
            .flatMap(x -> x.getVertexList().stream())
            .map(Node::getURI)
            .map(x -> x.substring(4)) // '4' is used to cut of the "urn:" prefix
            .collect(Collectors.toSet());
        return result;

    }


    public static void buildDependencyGraph(
            Graph outGraph,
            Function<String, Node> nodeFactory,
            String thisTxnId,
            boolean isWrite,
            ResourceLock<String> lockEntry) throws IOException {
        Node thisNode = nodeFactory.apply(thisTxnId);

        // Wait for the write lock if it is owned elsewhere
        String writeLockOwner = lockEntry.getWriteLockOwnerKey();
        if (writeLockOwner != null) {
            // Txn other = txnMgr.getTxn(writeLockOwner);
            Node otherNode = nodeFactory.apply(writeLockOwner);
            outGraph.add(new Triple(thisNode, accessed, otherNode));
        }


        // Write txns also have to wait for release of all read locks
        if (isWrite) {
            try (Stream<String> stream = lockEntry.streamReadLockOwnerKeys()) {
                Iterator<String> it = stream.iterator();
                while (it.hasNext()) {
                    String otherTxnId = it.next();
                    Node resNode = nodeFactory.apply(otherTxnId);
                    outGraph.add(new Triple(thisNode, accessed, resNode));
                }
            }
        }

    }

    /**
     * A txn waits for other txns if it declares access to a resource whose locks are owned by other txns.
     * Conversely, a txn does not wait for another if it owns to lock to a resource it declared access to.
     *
     *
     * @param graph
     * @param txn
     */
//    void buildDependencyGraph(Graph outGraph, MemoizedFunction<Txn, Node> memoizer, Txn txn) throws IOException {
//
//        // Get or create a graph node that represents the txn object
//        Node txnNode = memoizer.apply(txn);
//
//        TxnMgr txnMgr = txn.getTxnMgr();
//
//
//
//        LockStore<String[], String> lockStore = txn.getTxnMgr().getLockStore();
//
//        boolean isWrite = txn.isWrite();
//
//        try (Stream<String[]> res = txn.streamAccessedResourcePaths()) {
//            Iterator<String[]> it = res.iterator();
//            while (it.hasNext()) {
//                String[] resourceKey = it.next();
//
//                TxnResourceApi api = txn.getResourceApi(resourceKey);
//                ResourceLock<String> lockEntry = lockStore.getLockByKey(resourceKey);
//
//                // The txn declared access but does not own the lock
//                // - Write transactions may need for release prior locks (read/write)
//                // - Read transactions may need for release of a prior write lock
//                if (!api.getTxnResourceLock().isLockedHere()) {
//
//
//
//                }
//
//
//            }
//
//        }
//    }

    /**
     *
     *
     * Starting for a specific resource, build a graph which provides information about which txn waits for
     * which other based on which resources.
     * On this basis it is possible to detect deadlocks by mean of cycles in the graph, as well as waits
     * for stale transactions.
     *
     * @param outGraph
     * @param txn
     * @param resourceKey
     * @param isStale
     * @throws IOException
     */
//    public static void buildDependencyGraph(Graph outGraph, MemoizedFunction<Object, Node> objectToNode, Txn txn, String[] resourceKey, Predicate<? super Txn> isStale) throws IOException {
//
//        TxnMgr txnMgr = txn.getTxnMgr();
//
//        LockStore<String[], String> lockStore = txn.getTxnMgr().getLockStore();
//
//        TxnResourceApi api = txn.getResourceApi(resourceKey);
//        ResourceLock<String> lockEntry = lockStore.getLockByKey(resourceKey);
//
//        String owner = lockEntry.getWriteLockOwnerKey();
//
//        if (owner != null) {
//            Node txnNode = objectToNode.apply(txn.getId());
//            Node resNode = objectToNode.apply(Array.wrap(resourceKey));
//
//            outGraph.add(new Triple(txnNode, accessed, resNode));
//        }
//
//        try (Stream<String> stream = lockEntry.streamReadLockOwnerKeys()) {
//            Iterator<String> it = stream.iterator();
//            while (it.hasNext()) {
//                String txnId = it.next();
//
//                Txn otherTxn = txnMgr.getTxn(txnId);
//
//                try (Stream<String[]> keys = otherTxn.streamAccessedResourcePaths()) {
//                    Iterator<String[]> itKey = keys.iterator();
//                    while (itKey.hasNext()) {
//                        String[] key = itKey.next();
//
//
//                    }
//                }
//
//
//                Node txnNode = objectToNode.apply(txnId);
//                Node resNode = objectToNode.apply(Array.wrap(resourceKey));
//
//                outGraph.add(new Triple(txnNode, accessed, resNode));
//            }
//        }
//
//        org.jgrapht.Graph<Node, Triple> jgrapht = new PseudoGraphJenaGraph(outGraph);
//
//        CycleBasisAlgorithm<Node, Triple> cycleAlg = new StackBFSFundamentalCycleBasis<Node, Triple>(jgrapht);
//        cycleAlg.getCycleBasis().getCycles();
//
//
//        // CycleDetector<V, E>
//
//
//        // api.getTxnResourceLock()
//
//    }
}
