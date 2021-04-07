package org.aksw.jena_sparql_api.dataset.file;

import java.nio.file.Path;
import java.util.stream.Stream;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.DatasetGraph;

public interface DatasetGraphIndexPlugin {
    public Float evaluateFind(Node s, Node p, Node o);

    /**
     * If the result of {{@link #evaluateFind(Node, Node, Node)} is non-null then
     * this method is expected to yield an iterator of the graph nodes which may contain
     * triples matching the arguments.
     *
     * @param s
     * @param p
     * @param o
     * @return
     */
    // public Iterator<Node> listGraphNodes(Node s, Node p, Node o);
    public Stream<Path> listGraphNodes(DatasetGraph dg, Node s, Node p, Node o);

    public void add(DatasetGraph dg, Node g, Node s, Node p, Node o);
    public void delete(DatasetGraph dg, Node g, Node s, Node p, Node o);
}
