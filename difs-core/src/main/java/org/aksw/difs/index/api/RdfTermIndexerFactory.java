package org.aksw.difs.index.api;

import java.nio.file.Path;
import java.util.function.Function;

import org.apache.jena.graph.Node;

public interface RdfTermIndexerFactory {
	Function<Node, Path> getMapper();
}
